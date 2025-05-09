//! Error handling logic.
//!

use crate::error::Error::AwsError;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::copy_object::CopyObjectError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::get_object_attributes::GetObjectAttributesError;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_sdk_s3::operation::upload_part_copy::UploadPartCopyError;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::CreateUnhandledError;
use aws_smithy_types::byte_stream;
use aws_smithy_types::error::display::DisplayErrorContext;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::{Debug, Display, Formatter};
use std::num::TryFromIntError;
use std::{error, fmt, io, result};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinError;

/// The result type.
pub type Result<T> = result::Result<T, Error>;

/// Error types for checksum_cloud.
#[derive(Error, Serialize, Deserialize)]
pub enum Error {
    #[error("in concurrency logic: {0}")]
    ConcurrencyError(String),
    #[error("in memory logic: {0}")]
    MemoryError(String),
    #[serde(serialize_with = "serialize_io", skip_deserializing)]
    #[error("performing IO: {0}")]
    IOError(#[from] io::Error),
    #[error("parsing: {0}")]
    ParseError(String),
    #[serde(serialize_with = "serialize_try_from_int", skip_deserializing)]
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
    #[serde(serialize_with = "serialize_aws_error")]
    #[error("aws error: {message}")]
    AwsError {
        message: String,
        api_error: Option<ApiError>,
    },
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl Error {
    /// Create an AWS error from a string.
    pub fn aws_error(err: String) -> Self {
        AwsError {
            message: err.to_string(),
            api_error: None,
        }
    }
}

fn serialize_aws_error<S>(
    err: &str,
    api_error: &Option<ApiError>,
    serializer: S,
) -> result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(api_error) = api_error {
        api_error.serialize(serializer)
    } else {
        err.serialize(serializer)
    }
}

fn serialize_try_from_int<S>(
    err: &TryFromIntError,
    serializer: S,
) -> result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    err.to_string().serialize(serializer)
}

fn serialize_io<S>(err: &io::Error, serializer: S) -> result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    err.to_string().serialize(serializer)
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

impl From<byte_stream::error::Error> for Error {
    fn from(err: byte_stream::error::Error) -> Self {
        Self::IOError(io::Error::other(err))
    }
}

/// An API error that could be returned from storage.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialOrd, PartialEq, Ord, Hash)]
pub struct ApiError {
    /// The error kind, e.g. `AccessDenied`.
    pub(crate) code: String,
    /// The API call.
    pub(crate) call: String,
    /// The error message.
    pub(crate) message: String,
}

impl ApiError {
    /// Create a new error.
    pub fn new(code: String, call: String, message: String) -> Self {
        Self {
            code,
            call,
            message,
        }
    }

    /// Check if the error is an access denied error.
    pub fn is_access_denied(&self) -> bool {
        self.code == "AccessDenied"
    }
}

impl<T> From<(&SdkError<T, HttpResponse>, String)> for ApiError
where
    T: ProvideErrorMetadata + CreateUnhandledError + error::Error + Send + Sync + 'static,
{
    fn from((err, call): (&SdkError<T, HttpResponse>, String)) -> Self {
        Self::new(
            err.code().unwrap_or("Unknown").to_string(),
            call,
            err.message()
                .map(|msg| msg.to_string())
                .or_else(|| err.as_service_error().map(|err| err.to_string()))
                .unwrap_or_else(|| DisplayErrorContext(&err).to_string()),
        )
    }
}

impl Display for ApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} for {}: {}", self.code, self.call, self.message)
    }
}

/// Generate an impl for an AWS error type with the context of the API call.
macro_rules! generate_aws_error_impl {
    ($t:ty) => {
        impl From<&SdkError<$t>> for ApiError {
            fn from(err: &SdkError<$t>) -> Self {
                let api_call = stringify!($t);
                (
                    err,
                    api_call
                        .strip_suffix("Error")
                        .unwrap_or(api_call)
                        .to_string(),
                )
                    .into()
            }
        }

        impl From<SdkError<$t>> for Error {
            fn from(err: SdkError<$t>) -> Self {
                let err = ApiError::from(&err);
                Self::AwsError {
                    message: err.to_string(),
                    api_error: Some(err),
                }
            }
        }
    };
}

generate_aws_error_impl!(HeadObjectError);
generate_aws_error_impl!(GetObjectAttributesError);
generate_aws_error_impl!(PutObjectError);
generate_aws_error_impl!(GetObjectTaggingError);
generate_aws_error_impl!(CreateMultipartUploadError);
generate_aws_error_impl!(CompleteMultipartUploadError);
generate_aws_error_impl!(CopyObjectError);
generate_aws_error_impl!(UploadPartCopyError);
generate_aws_error_impl!(GetObjectError);
generate_aws_error_impl!(UploadPartError);
