//! Error handling logic.
//!

use std::{io, result};
use thiserror::Error;
use tokio::sync::broadcast::error::{RecvError, SendError};
use tokio::task::JoinError;

/// The result type.
pub type Result<T> = result::Result<T, Error>;

/// Error types for checksum_cloud.
#[derive(Error, Debug)]
pub enum Error {
    #[error("in concurrency logic: {0}")]
    ConcurrencyError(String),
    #[error("performing IO: {0}")]
    IOError(String),
}

impl From<JoinError> for Error {
    fn from(err: JoinError) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl From<RecvError> for Error {
    fn from(err: RecvError) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl<T> From<SendError<T>> for Error {
    fn from(err: SendError<T>) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IOError(err.to_string())
    }
}
