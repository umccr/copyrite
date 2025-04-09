//! File-based sums file logic.
//!

use crate::checksum::file::SumsFile;
use crate::checksum::Ctx;
use crate::error::Error::CopyError;
use crate::error::Result;
use crate::io::copy::{CopyContent, MultiPartOptions, ObjectCopy};
use crate::io::Provider;
use std::io::SeekFrom;
use tokio::fs::copy;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use tokio::{fs, io};

/// Build a file based sums object.
#[derive(Debug, Default)]
pub struct FileBuilder;

impl FileBuilder {
    /// Build using the file name.
    pub fn build(self) -> File {
        File
    }
}

/// A file object.
#[derive(Debug, Default, Clone)]
pub struct File;

impl File {
    /// Copy the file to the destination.
    pub async fn copy_source(&self, source: String, destination: String) -> Result<u64> {
        Ok(copy(&source, destination).await?)
    }

    /// Read the source into memory.
    pub async fn read(
        &self,
        source: String,
        multi_part_options: Option<MultiPartOptions>,
    ) -> Result<CopyContent> {
        let mut file = fs::File::open(source).await?;
        let size = file.metadata().await?.len();

        // Read only the specified range if multipart is being used.
        let (file, size): (Box<dyn AsyncRead + Send + Sync + Unpin>, _) =
            if let Some(multipart) = multi_part_options {
                file.seek(SeekFrom::Start(multipart.start)).await?;

                let size = multipart
                    .end
                    .checked_sub(multipart.start)
                    .ok_or_else(|| CopyError("Invalid multipart range".to_string()))?;
                (Box::new(file.take(size)), size)
            } else {
                (Box::new(file), size)
            };

        Ok(CopyContent::new(file, Some(size), None, None))
    }

    /// Write the data to the destination.
    pub async fn write(&self, destination: String, mut data: CopyContent) -> Result<Option<u64>> {
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
}

#[async_trait::async_trait]
impl ObjectCopy for File {
    async fn copy(
        &mut self,
        provider_source: Provider,
        provider_destination: Provider,
        multipart: Option<MultiPartOptions>,
        _additional_ctx: Option<Ctx>,
    ) -> Result<Option<u64>> {
        let source = SumsFile::format_target_file(&provider_source.into_file()?);
        let destination = SumsFile::format_target_file(&provider_destination.into_file()?);

        // There's no point copying using multiple parts on the filesystem so wait until all parts
        // are "sent" and then copy the file using the filesystem.
        match multipart {
            Some(multipart) if multipart.part_number.is_some() => return Ok(None),
            _ => {}
        };

        Ok(Some(self.copy_source(source, destination).await?))
    }

    async fn download(
        &mut self,
        source: Provider,
        multipart: Option<MultiPartOptions>,
    ) -> Result<CopyContent> {
        let source = source.into_file()?;
        let source = SumsFile::format_target_file(&source);

        self.read(source, multipart).await
    }

    async fn upload(
        &mut self,
        destination: Provider,
        data: CopyContent,
        _multipart: Option<MultiPartOptions>,
        _additional_ctx: Option<Ctx>,
    ) -> Result<Option<u64>> {
        let destination = destination.into_file()?;
        let destination = SumsFile::format_target_file(&destination);

        // It doesn't matter what the part number is for filesystem operations, just append to the
        // end of the file as we assume correct ordering of parts.
        self.write(destination, data).await
    }

    fn max_part_size(&self) -> u64 {
        u64::MAX
    }

    fn single_part_limit(&self) -> u64 {
        u64::MAX
    }

    fn max_parts(&self) -> u64 {
        u64::MAX
    }

    fn min_part_size(&self) -> u64 {
        u64::MIN
    }

    async fn size(&self, source: Provider) -> Result<Option<u64>> {
        let file = source.into_file()?;

        Ok(Some(self.file_size(file).await?))
    }
}
