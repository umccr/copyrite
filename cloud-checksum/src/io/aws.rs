//! Builders for S3 based operations.
//!

use crate::error::Error::ParseError;
use crate::error::Result;
use crate::io::{reader, writer};
use aws_config::load_defaults;
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::behavior_version::BehaviorVersion;

/// Build an S3 sums object.
#[derive(Debug, Default)]
pub struct S3Builder {
    client: Option<Client>,
    bucket: Option<String>,
    key: Option<String>,
    url: Option<String>,
}

impl S3Builder {
    /// Set the client by loading AWS environment variables.
    pub async fn with_default_client(mut self) -> Self {
        let config = load_defaults(BehaviorVersion::latest()).await;
        self.client = Some(Client::new(&config));

        self
    }

    /// Set the client.
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the key.
    pub fn with_key(mut self, key: String) -> Self {
        self.key = Some(key);
        self
    }

    /// Set the bucket.
    pub fn with_bucket(mut self, bucket: String) -> Self {
        self.bucket = Some(bucket);
        self
    }

    /// Set the bucket and key from an S3 url, e.g.`s3://bucket/key`.
    pub fn parse_from_url(mut self, url: String) -> Self {
        self.url = Some(url);
        self
    }

    fn get_components(mut self) -> Result<(Client, String, String)> {
        if let Some(url) = self.url {
            let (bucket, key) = Self::parse_url(&url)?;
            self.bucket = Some(bucket);
            self.key = Some(key);
        }

        let error_fn =
            || ParseError("client, bucket and key are required in `S3Builder`".to_string());

        Ok((
            self.client.ok_or_else(error_fn)?,
            self.bucket.ok_or_else(error_fn)?,
            self.key.ok_or_else(error_fn)?,
        ))
    }

    /// Build using the client, bucket and key.
    pub fn build_reader(self) -> Result<reader::aws::S3> {
        Ok(self.get_components()?.into())
    }

    /// Build using the client, bucket and key.
    pub fn build_writer(self) -> Result<writer::aws::S3> {
        Ok(self.get_components()?.into())
    }

    /// Parse from an S3 url, e.g.`s3://bucket/key`.
    pub fn parse_url(s: &str) -> Result<(String, String)> {
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

        Ok((bucket.to_string(), key.to_string()))
    }
}

impl From<(Client, String, String)> for reader::aws::S3 {
    fn from((client, bucket, key): (Client, String, String)) -> Self {
        Self::new(client, bucket, key)
    }
}

impl From<(Client, String, String)> for writer::aws::S3 {
    fn from((client, bucket, key): (Client, String, String)) -> Self {
        Self::new(client, bucket, key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    pub async fn test_parse_url() -> Result<()> {
        let s3 = expected_s3_reader("s3://bucket/key").await?;
        assert_eq!(s3.into_inner(), ("bucket".to_string(), "key".to_string()));
        let s3 = expected_s3_writer("s3://bucket/key").await?;
        assert_eq!(s3.into_inner(), ("bucket".to_string(), "key".to_string()));

        let s3 = expected_s3_reader("s3://bucket/key/").await?;
        assert_eq!(s3.into_inner(), ("bucket".to_string(), "key/".to_string()));
        let s3 = expected_s3_writer("s3://bucket/key/").await?;
        assert_eq!(s3.into_inner(), ("bucket".to_string(), "key/".to_string()));

        let s3 = expected_s3_reader("file://bucket/key").await;
        assert!(s3.is_err());
        let s3 = expected_s3_writer("file://bucket/key").await;
        assert!(s3.is_err());

        let s3 = expected_s3_reader("s3://bucket/").await;
        assert!(s3.is_err());
        let s3 = expected_s3_writer("s3://bucket/").await;
        assert!(s3.is_err());

        let s3 = expected_s3_reader("s3://").await;
        assert!(s3.is_err());
        let s3 = expected_s3_writer("s3://").await;
        assert!(s3.is_err());

        Ok(())
    }

    async fn expected_s3_reader(url: &str) -> Result<reader::aws::S3> {
        S3Builder::default()
            .parse_from_url(url.to_string())
            .with_default_client()
            .await
            .build_reader()
    }

    async fn expected_s3_writer(url: &str) -> Result<writer::aws::S3> {
        S3Builder::default()
            .parse_from_url(url.to_string())
            .with_default_client()
            .await
            .build_writer()
    }
}
