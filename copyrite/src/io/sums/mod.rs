//! Implementations for reading data using IO and from cloud storage.
//!

use crate::checksum::file::SumsFile;
use crate::error::{ApiError, Result};
use crate::io::sums::aws::S3Builder;
use crate::io::sums::file::FileBuilder;
use crate::io::{default_s3_client, Provider};
use aws_sdk_s3::Client;
use dyn_clone::DynClone;
use futures_util::Stream;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncRead;

pub mod aws;
pub mod channel;
pub mod file;

/// The type returned when converting a shared reader into a stream.
pub type ReaderStream = Pin<Box<dyn Stream<Item = Result<Arc<[u8]>>> + Send>>;

/// The shared reader trait defines functions for accessing chunks of data from a
/// reader in a parallel context.
#[async_trait::async_trait]
pub trait SharedReader {
    /// Start the IO-based read task, which reads chunks of data from a reader
    /// until the end.
    async fn read_chunks(&mut self) -> Result<u64>;

    /// Convert the shared reader into a stream of the resulting bytes of reading
    /// the chunks.
    fn as_stream(&mut self) -> ReaderStream;
}

/// Read operations on file based or cloud sums files.
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

    /// Any accumulated recoverable api errors.
    fn api_errors(&self) -> HashSet<ApiError>;
}

dyn_clone::clone_trait_object!(ObjectSums);

/// Build object sums from object URLs.
#[derive(Debug, Default)]
pub struct ObjectSumsBuilder {
    client: Option<Arc<Client>>,
    avoid_get_object_attributes: bool,
}

impl ObjectSumsBuilder {
    pub async fn build(self, url: String) -> Result<Box<dyn ObjectSums + Send>> {
        match Provider::try_from(url.as_str())? {
            Provider::File { file } => {
                Ok(Box::new(FileBuilder::default().with_file(file).build()?))
            }
            Provider::S3 { bucket, key } => {
                let client = match self.client {
                    Some(client) => client,
                    None => Arc::new(default_s3_client().await?),
                };
                Ok(Box::new(
                    S3Builder::default()
                        .with_key(key)
                        .with_bucket(bucket)
                        .with_client(client)
                        .with_avoid_get_object_attributes(self.avoid_get_object_attributes)
                        .build()?,
                ))
            }
        }
    }

    /// Set the S3 client if this is an s3 provider.
    pub fn set_client(mut self, client: Option<Arc<Client>>) -> Self {
        self.client = client;
        self
    }

    /// Avoid `GetObjectAttributes` calls.
    pub fn with_avoid_get_object_attributes(mut self, avoid_get_object_attributes: bool) -> Self {
        self.avoid_get_object_attributes = avoid_get_object_attributes;
        self
    }
}
