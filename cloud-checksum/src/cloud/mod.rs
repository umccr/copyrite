//! Cloud related functionality.
//!

use crate::checksum::file::SumsFile;
use crate::cloud::aws::S3Builder;
use crate::cloud::file::FileBuilder;
use crate::error::Result;
use dyn_clone::DynClone;
use tokio::io::AsyncRead;

pub mod aws;
pub mod file;

/// Operations on file based or cloud sums files.
#[async_trait::async_trait]
pub trait ObjectSums: DynClone {
    /// Get an existing sums file for this object.
    async fn sums_file(&mut self) -> Result<Option<SumsFile>>;

    /// Get a reader to the sums files.
    async fn reader(&mut self) -> Result<Box<dyn AsyncRead + Unpin + Send>>;

    /// Get the file size of the target file.
    async fn file_size(&mut self) -> Result<Option<u64>>;

    /// Write data to the configured location.
    async fn write_sums_file(&self, sums_file: &SumsFile) -> Result<()>;

    /// Get the location of the object.
    fn location(&self) -> String;
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
