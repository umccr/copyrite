//! Functionality related to copying.
//!

use crate::error::Result;
use crate::io::copy::aws::S3Builder;
use crate::io::copy::file::FileBuilder;
use crate::io::{default_s3_client, Provider};
use crate::MetadataCopy;
use std::collections::HashMap;
use tokio::io::AsyncRead;

pub mod aws;
pub mod file;

/// Content to download/upload with optional tags.
pub struct CopyContent {
    data: Box<dyn AsyncRead + Sync + Send + Unpin>,
    tags: Option<String>,
    metadata: Option<HashMap<String, String>>,
}

impl CopyContent {
    /// Create a new copy content struct.
    pub fn new(
        data: Box<dyn AsyncRead + Sync + Send + Unpin>,
        tags: Option<String>,
        metadata: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            data,
            tags,
            metadata,
        }
    }
}

/// Write operations on file based or cloud files.
#[async_trait::async_trait]
pub trait ObjectCopy {
    /// Copy the object to a new location using a single part.
    async fn copy_object(
        &self,
        provider_source: Provider,
        provider_destination: Provider,
    ) -> Result<Option<u64>>;

    /// Download the object to memory.
    async fn download(&self, source: Provider) -> Result<CopyContent>;

    /// Upload the object to the destination.
    async fn upload(&self, destination: Provider, data: CopyContent) -> Result<Option<u64>>;
}

/// Build object copy from object URLs.
#[derive(Debug, Default)]
pub struct ObjectCopyBuilder {
    metadata_mode: MetadataCopy,
}

impl ObjectCopyBuilder {
    pub async fn build(self, url: String) -> Result<Box<dyn ObjectCopy + Send>> {
        let provider = Provider::try_from(url.as_str())?;
        if provider.is_file() {
            Ok(Box::new(FileBuilder.build()))
        } else {
            Ok(Box::new(
                S3Builder::default()
                    .with_copy_metadata(self.metadata_mode)
                    .with_client(default_s3_client().await?)
                    .build()?,
            ))
        }
    }

    /// Set the copy metadata option.
    pub fn with_copy_metadata(mut self, metadata_mode: MetadataCopy) -> Self {
        self.metadata_mode = metadata_mode;
        self
    }
}
