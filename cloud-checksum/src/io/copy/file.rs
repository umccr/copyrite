//! File-based sums file logic.
//!

use crate::error::Result;
use crate::io::copy::ObjectCopy;
use crate::io::Provider;
use tokio::fs::copy;
use tokio::io::AsyncRead;
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
#[derive(Debug, Clone, Default)]
pub struct File;

impl File {
    /// Copy the file to the destination.
    pub async fn copy(&self, source: String, destination: String) -> Result<u64> {
        Ok(copy(&source, destination).await?)
    }

    /// Read the source into memory.
    pub async fn read(&self, source: String) -> Result<impl AsyncRead> {
        let file = fs::File::open(source).await?;
        Ok(file)
    }

    /// Write the data to the destination.
    pub async fn write(
        &self,
        destination: String,
        mut data: impl AsyncRead + Unpin,
    ) -> Result<Option<u64>> {
        let mut file = fs::File::create(destination).await?;

        let total = io::copy(&mut data, &mut file).await?;

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
        let source = provider_source.into_file()?;
        let destination = provider_destination.into_file()?;

        Ok(Some(self.copy(source, destination).await?))
    }

    async fn download(&self, source: Provider) -> Result<Box<dyn AsyncRead + Sync + Send + Unpin>> {
        let source = source.into_file()?;
        Ok(Box::new(self.read(source).await?))
    }

    async fn upload(
        &self,
        destination: Provider,
        data: Box<dyn AsyncRead + Sync + Send + Unpin>,
    ) -> Result<Option<u64>> {
        let destination = destination.into_file()?;
        self.write(destination, data).await
    }
}
