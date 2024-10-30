//! Error handling logic.
//!

use std::{io, result};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::task::JoinError;

/// The result type.
pub type Result<T> = result::Result<T, Error>;

/// Error types for checksum_cloud.
#[derive(Error, Debug)]
pub enum Error {
    #[error("in concurrency logic: {0}")]
    ConcurrencyError(String),
    #[error("in memory logic: {0}")]
    MemoryError(String),
    #[error("performing IO: {0}")]
    IOError(String),
}

impl From<JoinError> for Error {
    fn from(err: JoinError) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl From<broadcast::error::RecvError> for Error {
    fn from(err: broadcast::error::RecvError) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(err: async_channel::RecvError) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl<T> From<broadcast::error::SendError<T>> for Error {
    fn from(err: broadcast::error::SendError<T>) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(err: async_channel::SendError<T>) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IOError(err.to_string())
    }
}
