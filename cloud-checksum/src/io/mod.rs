//! Module that handles all file IO
//!

use crate::error::Result;
use crate::io::reader::ObjectRead;
use crate::io::writer::ObjectWrite;
use dyn_clone::DynClone;

pub mod aws;
pub mod file;
pub mod reader;
pub mod writer;

/// The type of provider for the object.
pub enum Provider {
    File,
    S3,
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
}

impl From<&str> for Provider {
    fn from(url: &str) -> Self {
        if url.starts_with("s3://") {
            Self::S3
        } else {
            Self::File
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
        match Provider::from(url.as_str()) {
            Provider::File => Ok(Box::new(
                file::FileBuilder::default().with_file(url).build_reader()?,
            )),
            Provider::S3 => Ok(Box::new(
                aws::S3Builder::default()
                    .with_default_client()
                    .await
                    .parse_from_url(url)
                    .build_reader()?,
            )),
        }
    }

    /// Build an `ObjectWrite` instance.
    pub async fn build_write(self, url: String) -> Result<Box<dyn ObjectWrite + Send>> {
        match Provider::from(url.as_str()) {
            Provider::File => Ok(Box::new(
                file::FileBuilder::default().with_file(url).build_writer()?,
            )),
            Provider::S3 => Ok(Box::new(
                aws::S3Builder::default()
                    .with_default_client()
                    .await
                    .parse_from_url(url)
                    .build_writer()?,
            )),
        }
    }
}
