//! Error handling logic.
//!

use std::sync::mpsc;
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
    IOError(#[from] io::Error),
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

impl From<mpsc::RecvError> for Error {
    fn from(err: mpsc::RecvError) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl<T> From<mpsc::SendError<T>> for Error {
    fn from(err: mpsc::SendError<T>) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl<T> From<async_broadcast::SendError<T>> for Error {
    fn from(err: async_broadcast::SendError<T>) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}
