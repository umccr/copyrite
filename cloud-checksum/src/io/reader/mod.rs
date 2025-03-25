//! Implementations for reading data using IO and from cloud storage.
//!

use crate::checksum::file::SumsFile;
use crate::error::Result;
use crate::io::ObjectMeta;
use futures_util::Stream;
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
pub trait ObjectRead: ObjectMeta {
    /// Get an existing sums file for this object.
    async fn sums_file(&mut self) -> Result<Option<SumsFile>>;

    /// Get a reader to the sums files.
    async fn reader(&mut self) -> Result<Box<dyn AsyncRead + Unpin + Send>>;

    /// Get the file size of the target file.
    async fn file_size(&mut self) -> Result<Option<u64>>;
}
