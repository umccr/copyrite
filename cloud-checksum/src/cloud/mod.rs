//! Cloud related functionality.
//!

use crate::checksum::file::SumsFile;
use crate::cloud::aws::S3Builder;
use crate::cloud::file::FileBuilder;
use crate::error::Result;
use tokio::io::AsyncRead;

pub mod aws;
pub mod file;

/// Operations on file based or cloud sums files.
#[async_trait::async_trait]
pub trait ObjectSums {
    /// Get an existing sums file for this object.
    async fn sums_file(&mut self) -> Result<Option<SumsFile>>;

    /// Get a sums file from object metadata. This should not attempt to read the underlying
    /// object.
    async fn metadata_sums_file(&mut self) -> Result<SumsFile>;

    /// Get a reader to the sums files.
    async fn reader(&mut self) -> Result<Box<dyn AsyncRead + Unpin + Send>>;

    /// Get the file size of the target file.
    async fn file_size(&mut self) -> Result<u64>;

    /// Write data to the configured location
    async fn write_data(&self, data: String) -> Result<()>;

    /// Clone this object.
    fn cloned(&self) -> Box<dyn ObjectSums + Send>;
}

/// Build object sums from object URLs.
#[derive(Debug, Default)]
pub struct ObjectSumsBuilder;

impl ObjectSumsBuilder {
    pub async fn build(self, url: String) -> Result<Box<dyn ObjectSums + Send>> {
        if S3Builder::is_s3(&url) {
            Ok(Box::new(
                S3Builder::default()
                    .with_default_client()
                    .await
                    .parse_from_url(url)
                    .build()?,
            ))
        } else {
            Ok(Box::new(FileBuilder::default().with_file(url).build()?))
        }
    }
}
