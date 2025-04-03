//! File-based sums file logic.
//!

use crate::checksum::file::SumsFile;
use crate::error::Result;
use crate::io::copy::{CopyContent, ObjectCopy, Range};
use crate::io::Provider;
use tokio::fs::copy;
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
#[derive(Debug, Default)]
pub struct File;

impl File {
    /// Copy the file to the destination.
    pub async fn copy(&self, source: String, destination: String) -> Result<u64> {
        Ok(copy(&source, destination).await?)
    }

    /// Read the source into memory.
    pub async fn read(&self, source: String) -> Result<CopyContent> {
        let file = fs::File::open(source).await?;
        let size = file.metadata().await?.len();
        Ok(CopyContent::new(Box::new(file), Some(size), None, None))
    }

    /// Write the data to the destination.
    pub async fn write(&self, destination: String, mut data: CopyContent) -> Result<Option<u64>> {
        let mut file = fs::File::create(destination).await?;

        let total = io::copy(&mut data.data, &mut file).await?;

        Ok(Some(total))
    }
}

#[async_trait::async_trait]
impl ObjectCopy for File {
    async fn copy_object(
        &self,
        provider_source: Provider,
        provider_destination: Provider,
    ) -> Result<Option<u64>> {
        let source = SumsFile::format_target_file(&provider_source.into_file()?);
        let destination = SumsFile::format_target_file(&provider_destination.into_file()?);

        Ok(Some(self.copy(source, destination).await?))
    }

    async fn copy_object_part(
        &mut self,
        provider_source: Provider,
        provider_destination: Provider,
        _part_number: Option<u64>,
        _range: Range,
    ) -> Result<Option<u64>> {
        // There's not much point in copying parts of files already on disk, just let the filesystem do it.
        self.copy_object(provider_source, provider_destination)
            .await
    }

    async fn download(&self, source: Provider) -> Result<CopyContent> {
        let source = source.into_file()?;
        let source = SumsFile::format_target_file(&source);

        self.read(source).await
    }

    async fn upload(&self, destination: Provider, data: CopyContent) -> Result<Option<u64>> {
        let destination = destination.into_file()?;
        let destination = SumsFile::format_target_file(&destination);

        self.write(destination, data).await
    }
}
