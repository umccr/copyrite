//! File-based sums file logic.
//!

use crate::checksum::file::SumsFile;
use crate::error::Error::ParseError;
use crate::error::Result;
use crate::io::copy::ObjectCopy;
use crate::io::Provider;
use tokio::fs;
use tokio::fs::copy;

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
#[derive(Debug, Clone)]
pub struct File;

impl File {
    /// Create a new file.
    pub fn new() -> Self {
        Self {}
    }

    /// Copy the file to the destination.
    pub async fn copy(&self, source: String, destination: String) -> Result<u64> {
        Ok(copy(&source, destination).await?)
    }
}

#[async_trait::async_trait]
impl ObjectCopy for File {
    async fn copy_object(
        &self,
        provider_source: Provider,
        provider_destination: Provider,
    ) -> Result<Option<u64>> {
        let source = provider_source.into_file()?;
        let destination = provider_destination.into_file()?;

        Ok(Some(self.copy(source, destination).await?))
    }
}
