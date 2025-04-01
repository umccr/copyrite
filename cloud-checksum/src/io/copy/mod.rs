//! Functionality related to copying.
//!

use crate::checksum::file::SumsFile;
use crate::error::Result;
use crate::io::copy::aws::S3Builder;
use crate::io::copy::file::FileBuilder;
use crate::io::sums::ObjectSums;
use crate::io::{default_s3_client, Provider};

pub mod aws;
pub mod file;

/// Write operations on file based or cloud files.
#[async_trait::async_trait]
pub trait ObjectCopy {
    /// Copy the object to a new location using a single part.
    async fn copy_object(
        &self,
        provider_source: Provider,
        provider_destination: Provider,
    ) -> Result<Option<u64>>;
}

/// Build object copy from object URLs.
#[derive(Debug, Default)]
pub struct ObjectCopyBuilder;

impl ObjectCopyBuilder {
    pub async fn build(self, url: String) -> Result<Box<dyn ObjectCopy + Send>> {
        let provider = Provider::try_from(url.as_str())?;
        if provider.is_file() {
            Ok(Box::new(FileBuilder::default().build()))
        } else {
            Ok(Box::new(
                S3Builder::default()
                    .with_client(default_s3_client().await?)
                    .build()?,
            ))
        }
    }
}
