//! Functionality related to copying.
//!

use crate::checksum::Ctx;
use crate::error::Error::CopyError;
use crate::error::Result;
use crate::io::copy::aws::S3Builder;
use crate::io::copy::file::FileBuilder;
use crate::io::{default_s3_client, Provider};
use crate::MetadataCopy;
use aws_sdk_s3::Client;
use dyn_clone::DynClone;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{empty, AsyncRead};

pub mod aws;
pub mod file;

/// Content to download/upload with optional tags.
pub struct CopyContent {
    data: Box<dyn AsyncRead + Sync + Send + Unpin>,
}

impl Default for CopyContent {
    fn default() -> Self {
        Self {
            data: Box::new(empty()),
        }
    }
}

impl CopyContent {
    /// Create a new copy content struct.
    pub fn new(data: Box<dyn AsyncRead + Sync + Send + Unpin>) -> Self {
        Self { data }
    }
}

#[derive(Debug, Clone, Default)]
pub struct MultiPartOptions {
    pub(crate) part_number: Option<u64>,
    pub(crate) start: u64,
    pub(crate) end: u64,
    pub(crate) parts: Vec<Part>,
    pub(crate) upload_id: Option<String>,
}

impl MultiPartOptions {
    pub fn format_range(&self) -> Option<String> {
        Some(format!("bytes={}-{}", self.start, self.end.checked_sub(1)?))
    }
}

/// Represents a part for a multipart copy.
#[derive(Debug, Clone, Default)]
pub struct Part {
    pub(crate) crc32: Option<String>,
    pub(crate) crc32_c: Option<String>,
    pub(crate) sha1: Option<String>,
    pub(crate) sha256: Option<String>,
    pub(crate) crc64_nvme: Option<String>,
    pub(crate) e_tag: Option<String>,
    pub(crate) part_number: u64,
}

/// The result of a copy operation.
#[derive(Debug, Clone, Default)]
pub struct CopyResult {
    pub(crate) part: Option<Part>,
    pub(crate) upload_id: Option<String>,
}

impl CopyResult {
    /// Create a new copy result.
    pub fn new(part: Option<Part>, upload_id: Option<String>) -> Self {
        Self { part, upload_id }
    }
}

impl From<(Part, String)> for CopyResult {
    fn from((part, upload_id): (Part, String)) -> Self {
        Self {
            part: Some(part),
            upload_id: Some(upload_id),
        }
    }
}

/// The state of the copy operation.
#[derive(Debug, Clone)]
pub struct CopyState {
    size: Option<u64>,
    tags: Option<String>,
    metadata: Option<HashMap<String, String>>,
    additional_ctx: Option<Ctx>,
}

impl CopyState {
    /// Get the object size.
    pub fn size(&self) -> Option<u64> {
        self.size
    }

    /// Get the object tags as a query string.
    pub fn tags(&self) -> Option<String> {
        self.tags.clone()
    }

    /// Get the object metadata.
    pub fn metadata(&self) -> Option<HashMap<String, String>> {
        self.metadata.clone()
    }

    /// Get the additional context.
    pub fn additional_ctx(&self) -> Option<Ctx> {
        self.additional_ctx.clone()
    }

    /// Create a new state.
    pub fn new(
        size: Option<u64>,
        tags: Option<String>,
        metadata: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            size,
            tags,
            metadata,
            additional_ctx: None,
        }
    }

    /// Set the additional context.
    pub fn set_additional_ctx(&mut self, additional_ctx: Ctx) {
        self.additional_ctx = Some(additional_ctx);
    }
}

/// Write operations on file based or cloud files.
#[async_trait::async_trait]
pub trait ObjectCopy: DynClone {
    /// Copy the object to a new location with optional multipart copies.
    async fn copy(
        &self,
        multi_part: Option<MultiPartOptions>,
        state: &CopyState,
    ) -> Result<CopyResult>;

    /// Download the object to memory.
    async fn download(&self, multi_part: Option<MultiPartOptions>) -> Result<CopyContent>;

    /// Upload the object to the destination.
    async fn upload(
        &self,
        data: CopyContent,
        multi_part: Option<MultiPartOptions>,
        state: &CopyState,
    ) -> Result<CopyResult>;

    /// The maximum part size for multipart copy.
    fn max_part_size(&self) -> u64;

    /// The maximum number of parts for multipart copies.
    fn max_parts(&self) -> u64;

    /// The minimum part size for multipart copies.
    fn min_part_size(&self) -> u64;

    /// Get the size of the object.
    async fn initialize_state(&self) -> Result<CopyState>;
}

dyn_clone::clone_trait_object!(ObjectCopy);

/// Build object copy from object URLs.
#[derive(Debug, Default)]
pub struct ObjectCopyBuilder {
    metadata_mode: MetadataCopy,
    client: Option<Arc<Client>>,
    source: Option<Provider>,
    destination: Option<Provider>,
}

impl ObjectCopyBuilder {
    /// Build the object copy. Both the source and destination need to be of the same type.
    pub async fn build(self) -> Result<Box<dyn ObjectCopy + Send + Sync>> {
        let is_s3 = match (&self.source, &self.destination) {
            (Some(source), _) => source.is_s3(),
            (_, Some(destination)) => destination.is_s3(),
            _ => return Err(CopyError("No source or destination provided".to_string())),
        };

        if is_s3 {
            let client = match self.client {
                Some(client) => client,
                None => Arc::new(default_s3_client().await?),
            };
            let source = self.source.map(|source| source.into_s3()).transpose()?;
            let destination = self
                .destination
                .map(|destination| destination.into_s3())
                .transpose()?;

            let mut builder = S3Builder::default()
                .with_copy_metadata(self.metadata_mode)
                .with_client(client);

            if let Some((bucket, key)) = source {
                builder = builder.with_source(&bucket, &key);
            }
            if let Some((bucket, key)) = destination {
                builder = builder.with_destination(&bucket, &key);
            }

            Ok(Box::new(builder.build()?))
        } else {
            let source = self.source.map(|source| source.into_file()).transpose()?;
            let destination = self
                .destination
                .map(|destination| destination.into_file())
                .transpose()?;

            let mut builder = FileBuilder::default();
            if let Some(source) = source {
                builder = builder.with_source(&source);
            }
            if let Some(destination) = destination {
                builder = builder.with_destination(&destination);
            }

            Ok(Box::new(builder.build()))
        }
    }

    /// Set the destination.
    pub fn set_destination(mut self, destination: Option<Provider>) -> Self {
        self.destination = destination;
        self
    }

    /// Set the source.
    pub fn set_source(mut self, source: Option<Provider>) -> Self {
        self.source = source;
        self
    }

    /// Set the copy metadata option.
    pub fn with_copy_metadata(mut self, metadata_mode: MetadataCopy) -> Self {
        self.metadata_mode = metadata_mode;
        self
    }

    /// Set the S3 client if this is an s3 provider.
    pub fn set_client(mut self, client: Option<Arc<Client>>) -> Self {
        self.client = client;
        self
    }
}
