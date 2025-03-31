//! Module that handles all file IO
//!

use crate::error::{Error, Result};
use crate::io::reader::ObjectRead;
use crate::io::writer::ObjectWrite;
use dyn_clone::DynClone;
use crate::error::Error::ParseError;

pub mod aws;
pub mod file;
pub mod reader;
pub mod writer;

/// The type of provider for the object.
pub enum Provider {
    File{ file: String },
    S3{ bucket: String, key: String },
}

impl Provider {
    /// Format an S3 url.
    pub fn format_s3(bucket: &str, key: &str) -> String {
        format!("s3://{}/{}", bucket, key)
    }

    /// Format a file url.
    pub fn format_file(file: &str) -> String {
        format!("file://{}", file)
    }
    
    /// Format the provider into a string.
    pub fn format(&self) -> String {
        match self {
            Provider::File{ file} => Self::format_file(file),
            Provider::S3{bucket, key} => Self::format_s3(bucket, key),
        }
    }

    /// Parse from an S3 url, e.g.`s3://bucket/key`.
    pub fn parse_s3_url(s: &str) -> Result<Self> {
        let Some(s) = s.strip_prefix("s3://") else {
            return Err(ParseError(format!("{} is not an S3 url", s)));
        };

        let split = s.split_once("/");
        let Some((bucket, key)) = split else {
            return Err(ParseError(format!("failed to parse {}", s)));
        };

        if bucket.is_empty() {
            return Err(ParseError(format!("{} is missing a bucket", s)));
        }
        if key.is_empty() {
            return Err(ParseError(format!("{} is missing a key", s)));
        }

        Ok(Self::S3{ bucket: bucket.to_string(), key: key.to_string() })
    }

    /// Parse from a string a file name which can optionally be prefixed with `file://`
    pub fn parse_file_url(s: &str) -> Self {
        Self::File{file: s.strip_prefix("file://").unwrap_or(s).to_string()}
    }
}

impl TryFrom<&str> for Provider {
    type Error = Error;
    
    fn try_from(url: &str) -> Result<Self> {
        if url.starts_with("s3://") {
            Self::parse_s3_url(url)
        } else {
            Ok(Self::parse_file_url(url))
        }
    }
}

/// Obtain metadata information on objects.
pub trait ObjectMeta: DynClone {
    /// Get the location of the object.
    fn location(&self) -> String;
}

/// Build io from object URLs.
#[derive(Debug, Default)]
pub struct IoBuilder;

impl IoBuilder {
    /// Build an `ObjectRead` instance.
    pub async fn build_read(self, url: String) -> Result<Box<dyn ObjectRead + Send>> {
        match Provider::try_from(url.as_str())? {
            Provider::File{ file } => Ok(Box::new(
                file::FileBuilder::default().with_file(file).build_reader()?,
            )),
            Provider::S3{ bucket, key} => Ok(Box::new(
                aws::S3Builder::default()
                    .with_default_client()
                    .await
                    .with_bucket(bucket)
                    .with_key(key)
                    .build_reader()?,
            )),
        }
    }

    /// Build an `ObjectWrite` instance.
    pub async fn build_write(self, url: String) -> Result<Box<dyn ObjectWrite + Send>> {
        match Provider::try_from(url.as_str())? {
            Provider::File{ file } => Ok(Box::new(
                file::FileBuilder::default().with_file(file).build_writer()?,
            )),
            Provider::S3{ bucket, key}  => Ok(Box::new(
                aws::S3Builder::default()
                    .with_default_client()
                    .await
                    .with_bucket(bucket)
                    .with_key(key)
                    .build_writer()?,
            )),
        }
    }
}
