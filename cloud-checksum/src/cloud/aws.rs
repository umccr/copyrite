//! AWS checksums and functionality.
//!

use crate::checksum::file::SumsFile;
use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use std::str::FromStr;

/// An S3 object and AWS-related existing sums.
#[derive(Debug, Eq, PartialEq)]
pub struct S3 {
    bucket: String,
    key: String,
    existing_sum: Option<SumsFile>,
}

impl S3 {
    /// Create a new S3 object.
    pub fn new(bucket: String, key: String, existing_sum: Option<SumsFile>) -> S3 {
        Self {
            bucket,
            key,
            existing_sum,
        }
    }

    /// Parse from an S3 url, e.g.`s3://bucket/key`.
    pub fn parse_url(s: &str) -> Result<S3> {
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

        Ok(S3::new(bucket.to_string(), key.to_string(), None))
    }
}

impl FromStr for S3 {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse_url(s)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::error::Result;

    #[test]
    pub fn test_parse_url() -> Result<()> {
        let s3 = S3::from_str("s3://bucket/key")?;
        assert_eq!(s3, S3::new("bucket".to_string(), "key".to_string(), None));

        let s3 = S3::from_str("s3://bucket/key/")?;
        assert_eq!(s3, S3::new("bucket".to_string(), "key/".to_string(), None));

        let s3 = S3::from_str("file://bucket/key");
        assert!(s3.is_err());

        let s3 = S3::from_str("s3://bucket/");
        assert!(s3.is_err());

        let s3 = S3::from_str("s3://");
        assert!(s3.is_err());

        Ok(())
    }
}
