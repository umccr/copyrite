//! Shared reader implementations for reading buffered data.
//!

use crate::error::Result;
use futures_util::Stream;
use std::pin::Pin;
use std::sync::Arc;

pub mod channel;

/// The shared reader trait defines functions for accessing chunks of data from a
/// reader in a parallel context.
#[async_trait::async_trait]
pub trait SharedReader {
    /// Start the IO-based read task, which reads chunks of data from a reader
    /// until the end.
    async fn read_task(&mut self) -> Result<u64>;

    /// Convert the shared reader into a stream of the resulting bytes of reading
    /// the chunks.
    fn as_stream(&mut self) -> Pin<Box<dyn Stream<Item = Result<Arc<[u8]>>> + Send>>;
}
