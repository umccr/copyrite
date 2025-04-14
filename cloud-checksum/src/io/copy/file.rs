//! File-based sums file logic.
//!

use crate::checksum::file::SumsFile;
use crate::error::Error::CopyError;
use crate::error::Result;
use crate::io::copy::{CopyContent, CopyResult, CopyState, MultiPartOptions, ObjectCopy};
use std::io::SeekFrom;
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

    /// Read the source into memory.
    pub async fn read(&self, multi_part_options: Option<MultiPartOptions>) -> Result<CopyContent> {
        let mut file = fs::File::open(self.get_source()?).await?;

        // Read only the specified range if multipart is being used.
        let file: Box<dyn AsyncRead + Send + Sync + Unpin> =
            if let Some(multipart) = multi_part_options {
                file.seek(SeekFrom::Start(multipart.start)).await?;

                let size = multipart
                    .end
                    .checked_sub(multipart.start)
                    .ok_or_else(|| CopyError("Invalid multipart range".to_string()))?;
                Box::new(file.take(size))
            } else {
                Box::new(file)
            };

        Ok(CopyContent::new(file))
    }

    /// Write the data to the destination.
    pub async fn write(&self, mut data: CopyContent) -> Result<Option<u64>> {
        let destination = self.get_destination()?;
        // Append to an existing file or create a new one.
        let mut file = if fs::try_exists(&destination)
            .await
            .is_ok_and(|exists| exists)
        {
            fs::OpenOptions::new()
                .append(true)
                .write(true)
                .open(destination)
                .await?
        } else {
            fs::File::create(destination).await?
        };

        let total = io::copy(&mut data.data, &mut file).await?;

        Ok(Some(total))
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

        Ok(CopyState::new(Some(size), None, None))
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

        self.copy_source().await?;

        Ok(Default::default())
    }

    async fn download(&self, multipart: Option<MultiPartOptions>) -> Result<CopyContent> {
        self.read(multipart).await
    }

    async fn upload(
        &self,
        data: CopyContent,
        _multipart: Option<MultiPartOptions>,
        _state: &CopyState,
    ) -> Result<CopyResult> {
        // It doesn't matter what the part number is for filesystem operations, just append to the
        // end of the file as we assume correct ordering of parts.
        self.write(data).await?;

        Ok(Default::default())
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
