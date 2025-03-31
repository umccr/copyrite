//! Error handling logic.
//!

use aws_sdk_s3::error::SdkError;
use aws_smithy_runtime_api::client::result::CreateUnhandledError;
use aws_smithy_types::byte_stream;
use std::num::TryFromIntError;
use std::{error, io, result};
use thiserror::Error;
use tokio::sync::mpsc;
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
    #[error("parsing: {0}")]
    ParseError(String),
    #[error("overflow converting numbers: {0}")]
    OverflowError(#[from] TryFromIntError),
    #[error("serde: {0}")]
    SerdeError(String),
    #[error("output file: {0}")]
    SumsFileError(String),
    #[error("generate command error: {0}")]
    GenerateError(String),
    #[error("check command error: {0}")]
    CheckError(String),
    #[error("copy command error: {0}")]
    CopyError(String),
    #[error("aws error: {0}")]
    AwsError(String),
}

impl From<JoinError> for Error {
    fn from(err: JoinError) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(err: mpsc::error::SendError<T>) -> Self {
        Self::ConcurrencyError(err.to_string())
    }
}

impl From<clap::Error> for Error {
    fn from(err: clap::Error) -> Self {
        Self::ParseError(err.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::SerdeError(err.to_string())
    }
}

impl<T> From<SdkError<T>> for Error
where
    T: Send + Sync + error::Error + CreateUnhandledError + 'static,
{
    fn from(err: SdkError<T>) -> Self {
        Self::AwsError(err.into_service_error().to_string())
    }
}

impl From<byte_stream::error::Error> for Error {
    fn from(err: byte_stream::error::Error) -> Self {
        Self::AwsError(err.to_string())
    }
}
