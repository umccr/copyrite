//! File-based sums file logic.
//!

use crate::checksum::file::SumsFile;
use crate::error::Error::CopyError;
use crate::error::Result;
use crate::io::copy::{CopyContent, CopyResult, CopyState, MultiPartOptions, ObjectCopy};
use std::future::Future;
use std::io::SeekFrom;
use std::pin::Pin;
use tokio::fs::copy;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use tokio::{fs, io};

/// Build a file based sums object.
#[derive(Debug, Default)]
pub struct FileBuilder {
    source: Option<String>,
    destination: Option<String>,
}

impl FileBuilder {
    /// Build using the file name.
    pub fn build(self) -> File {
        self.get_components().into()
    }

    /// Set the source file.
    pub fn with_source(mut self, source: &str) -> Self {
        self.source = Some(SumsFile::format_target_file(source));
        self
    }

    /// Set the destination file.
    pub fn with_destination(mut self, destination: &str) -> Self {
        self.destination = Some(SumsFile::format_target_file(destination));
        self
    }
    fn get_components(self) -> (Option<String>, Option<String>) {
        (self.source, self.destination)
    }
}

impl From<(Option<String>, Option<String>)> for File {
    fn from((source, destination): (Option<String>, Option<String>)) -> Self {
        Self::new(source, destination)
    }
}

/// A file object.
#[derive(Debug, Default, Clone)]
pub struct File {
    source: Option<String>,
    destination: Option<String>,
}

impl File {
    fn get_source(&self) -> Result<&str> {
        self.source
            .as_deref()
            .ok_or_else(|| CopyError("missing source".to_string()))
    }

    fn get_destination(&self) -> Result<&str> {
        self.destination
            .as_deref()
            .ok_or_else(|| CopyError("missing destination".to_string()))
    }

    /// Copy the file to the destination.
    pub async fn copy_source(&self) -> Result<u64> {
        Ok(copy(&self.get_source()?, self.get_destination()?).await?)
    }

    /// Read the source into memory. The returned content carries a reopen factory that re-reads the
    /// same range, so the upload body can be re-derived from the source on a retry.
    pub async fn read(&self, multi_part_options: Option<MultiPartOptions>) -> Result<CopyContent> {
        let mut file = fs::File::open(self.get_source()?).await?;

        // Read only the specified range if multipart is being used.
        let file: Box<dyn AsyncRead + Send + Sync + Unpin> =
            if let Some(multipart) = &multi_part_options {
                file.seek(SeekFrom::Start(multipart.start)).await?;

                let size = multipart
                    .end
                    .checked_sub(multipart.start)
                    .ok_or_else(|| CopyError("Invalid multipart range".to_string()))?;
                Box::new(file.take(size))
            } else {
                Box::new(file)
            };

        let self_clone = self.clone();
        CopyContent::builder(file)
            .with_reopen(move || self_clone.reopen_read(multi_part_options.clone()))
            .build()
    }

    /// Re-read the source range.
    fn reopen_read(
        &self,
        multi_part_options: Option<MultiPartOptions>,
    ) -> Pin<Box<dyn Future<Output = Result<CopyContent>> + Send>> {
        let self_clone = self.clone();
        Box::pin(async move { self_clone.read(multi_part_options).await })
    }

    /// Write the data to the destination.
    pub async fn write(
        &self,
        mut data: CopyContent,
        multipart: Option<MultiPartOptions>,
    ) -> Result<u64> {
        // Determine the part number for this write. A multipart write with no part number is the
        // completion step, which writes nothing.
        let part_number = if let Some(multipart) = &multipart {
            if multipart.part_number.is_none() {
                return Ok(0);
            }
            multipart.part_number
        } else {
            None
        };

        let destination = self.get_destination()?;

        // The first part should truncate an existing file.
        let append = if let Some(part_number) = part_number {
            part_number > 1
        } else {
            false
        };
        let mut file = if append {
            fs::OpenOptions::new()
                .append(true)
                .write(true)
                .open(destination)
                .await?
        } else {
            fs::File::create(destination).await?
        };

        let total = if let Some(multipart) = multipart {
            io::copy(
                &mut data.data.take(multipart.bytes_transferred()),
                &mut file,
            )
            .await?
        } else {
            io::copy(&mut data.data, &mut file).await?
        };

        Ok(total)
    }

    pub async fn file_size(&self, source: String) -> Result<u64> {
        let file = fs::File::open(source).await?;
        let size = file.metadata().await?.len();

        Ok(size)
    }

    /// Initialize the state for a bucket and key.
    pub async fn initialize_state(source: &str) -> Result<CopyState> {
        let file = fs::File::open(source).await?;
        let size = file.metadata().await?.len();

        Ok(CopyState::new(size, None, None))
    }

    /// Create a new file object.
    pub fn new(source: Option<String>, destination: Option<String>) -> Self {
        Self {
            source,
            destination,
        }
    }
}

#[async_trait::async_trait]
impl ObjectCopy for File {
    async fn copy(
        &self,
        multipart: Option<MultiPartOptions>,
        _state: &CopyState,
    ) -> Result<CopyResult> {
        // There's no point copying using multiple parts on the filesystem so wait until all parts
        // are "sent" and then copy the file using the filesystem.
        match multipart {
            Some(multipart) if multipart.part_number.is_some() => return Ok(Default::default()),
            _ => {}
        };

        let bytes = self.copy_source().await?;

        CopyResult::new(None, None, bytes, vec![])
    }

    async fn download(&self, multipart: Option<MultiPartOptions>) -> Result<CopyContent> {
        self.read(multipart).await
    }

    async fn upload(
        &self,
        data: CopyContent,
        multipart: Option<MultiPartOptions>,
        _state: &CopyState,
    ) -> Result<CopyResult> {
        // It doesn't matter what the part number is for filesystem operations, just append to the
        // end of the file as we assume correct ordering of parts.
        let bytes = self.write(data, multipart).await?;

        CopyResult::new(None, None, bytes, vec![])
    }

    fn max_part_size(&self) -> u64 {
        u64::MAX
    }

    fn max_parts(&self) -> u64 {
        u64::MAX
    }

    fn min_part_size(&self) -> u64 {
        u64::MIN
    }

    async fn initialize_state(&self) -> Result<CopyState> {
        let source = self.get_source()?;

        Self::initialize_state(source).await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::tempdir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const BODY: &[u8] = b"the quick brown fox jumps over the lazy dog";

    async fn read_all(content: CopyContent) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut data = content.data;
        data.read_to_end(&mut buf).await.unwrap();
        buf
    }

    async fn write_source(dir: &std::path::Path) -> String {
        let path = dir.join("source");
        let mut file = fs::File::create(&path).await.unwrap();
        file.write_all(BODY).await.unwrap();
        file.flush().await.unwrap();
        path.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn reopen_reproduces_source() {
        let tmp = tempdir().unwrap();
        let source = write_source(tmp.path()).await;
        let file = File::new(Some(source), None);

        let content = file.download(None).await.unwrap();
        assert_eq!(read_all(content).await, BODY);

        let content = file.download(None).await.unwrap();
        let reopened = (content.reopen)().await.unwrap();
        assert_eq!(read_all(reopened).await, BODY);
    }

    #[tokio::test]
    async fn reopen_reproduces_parts() {
        let tmp = tempdir().unwrap();
        let source = write_source(tmp.path()).await;
        let file = File::new(Some(source), None);

        let options = MultiPartOptions {
            part_number: Some(1),
            start: 4,
            end: 19,
            ..Default::default()
        };
        let expected = &BODY[4..19];

        let content = file.download(Some(options.clone())).await.unwrap();
        assert_eq!(read_all(content).await, expected);

        // Reopen must re-read the identical range.
        let content = file.download(Some(options)).await.unwrap();
        let reopened = (content.reopen)().await.unwrap();
        assert_eq!(read_all(reopened).await, expected);
    }

    #[tokio::test]
    async fn multipart_write_truncates_stale_destination() {
        let tmp = tempdir().unwrap();

        let source_path = tmp.path().join("source");
        {
            let mut file = fs::File::create(&source_path).await.unwrap();
            file.write_all(b"test").await.unwrap();
            file.flush().await.unwrap();
        }
        let source = File::new(Some(source_path.to_string_lossy().to_string()), None);

        // Stale content from a previous run that must be overwritten, not appended to.
        let destination_path = tmp.path().join("destination");
        {
            let mut file = fs::File::create(&destination_path).await.unwrap();
            file.write_all(b"previous").await.unwrap();
            file.flush().await.unwrap();
        }
        let destination = File::new(None, Some(destination_path.to_string_lossy().to_string()));

        let state = CopyState::new(10, None, None);
        let part1 = MultiPartOptions {
            part_number: Some(1),
            start: 0,
            end: 5,
            ..Default::default()
        };
        let part2 = MultiPartOptions {
            part_number: Some(2),
            start: 5,
            end: 10,
            ..Default::default()
        };

        let content = source.download(Some(part1.clone())).await.unwrap();
        destination
            .upload(content, Some(part1), &state)
            .await
            .unwrap();

        let content = source.download(Some(part2.clone())).await.unwrap();
        destination
            .upload(content, Some(part2), &state)
            .await
            .unwrap();

        let mut written = Vec::new();
        fs::File::open(&destination_path)
            .await
            .unwrap()
            .read_to_end(&mut written)
            .await
            .unwrap();
        assert_eq!(written, b"test");
    }

    #[tokio::test]
    async fn download_upload() {
        let tmp = tempdir().unwrap();
        let source = write_source(tmp.path()).await;
        let destination = tmp.path().join("destination");

        let source_file = File::new(Some(source), None);
        let destination_file = File::new(None, Some(destination.to_string_lossy().to_string()));
        let state = CopyState::new(BODY.len() as u64, None, None);

        let content = source_file.download(None).await.unwrap();
        destination_file
            .upload(content, None, &state)
            .await
            .unwrap();

        let mut written = Vec::new();
        fs::File::open(&destination)
            .await
            .unwrap()
            .read_to_end(&mut written)
            .await
            .unwrap();
        assert_eq!(written, BODY);
    }
}
