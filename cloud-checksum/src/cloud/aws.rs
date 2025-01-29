//! AWS checksums and functionality.
//!

use std::collections::BTreeSet;
use crate::checksum::file::{SumsFile, SUMS_FILE_ENDING};
use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use aws_config::{load_defaults, BehaviorVersion};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::primitives::AggregatedBytes;
use crate::checksum::aws_etag::AWSETagCtx;
use crate::checksum::file;
use crate::checksum::standard::StandardCtx;

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

    /// Build using the client, bucket and key.
    pub fn build(mut self) -> Result<S3> {
        if let Some(url) = self.url {
            let (bucket, key) = Self::parse_url(&url)?;
            self.bucket = Some(bucket);
            self.key = Some(key);
        }

        let error_fn = || ParseError("client, bucket and key are required in `S3Builder`".to_string());

        Ok(S3::new(
            self.client.ok_or_else(error_fn)?,
            self.bucket.ok_or_else(error_fn)?,
            self.key.ok_or_else(error_fn)?,
        ))
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

/// An S3 object and AWS-related existing sums.
#[derive(Debug)]
pub struct S3 {
    client: Client,
    bucket: String,
    key: String
}

impl S3 {
    /// Create a new S3 object.
    pub fn new(client: Client, bucket: String, key: String) -> S3 {
        Self {
            client,
            bucket,
            key,
        }
    }

    /// Get an existing sums file if it exists.
    pub async fn get_existing_sums(&self) -> Result<Option<SumsFile>> {
        match self.client.get_object().bucket(&self.bucket).key(SumsFile::format_sums_file(&self.key)).send().await {
            Ok(sums) => {
                let data = sums.body.collect().await?.to_vec();
                let sums = SumsFile::read_from_slice(data.as_slice(), SumsFile::format_target_file(&self.key)).await?;
                Ok(Some(sums))
            },
            Err(err) if matches!(err.as_service_error(), Some(GetObjectError::NoSuchKey(_))) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
    
    /// Load a sums file from object metadata.
    pub async fn sums_from_metadata(&self) -> Result<SumsFile> {
        // The target file metadata.
        let key = SumsFile::format_target_file(&self.key);
        let file = self.client.head_object().bucket(&self.bucket).key(key).send().await?;

        let sums_file = SumsFile::default().add_name(key).with_size(file.content_length().map(|size| u64::try_from(size)).transpose()?);
        if let Some(e_tag) = file.e_tag {
            AWSETagCtx::new(
                StandardCtx::MD5()
                AWSETagCtx::PartMode::PartSize(0),
                None,
            )
            
            let checksum = file::Checksum::new(
                Checksum
            )
        }

        checksums.push(file.etag.ok_or_else(|| ParseError("missing etag".to_string()))?);

        SumsFile::new(
            self.bucket.clone(),
            self.key.clone(),
            file.content_length.ok_or_else(|| ParseError("missing content length".to_string()))?,
            file.last_modified.ok_or_else(|| ParseError("missing last modified".to_string()))?,
            file.etag.ok_or_else(|| ParseError("missing etag".to_string()))?,
        )
    }
    
    /// Load the sums file from S3 if it exists. If it does not exist,
    /// fills in a sums file from available object metadata.
    pub async fn load_sums(&self) -> Result<SumsFile> {
        // Return the sums file if it already exists.
        if let Some(sums) = self.get_existing_sums().await? {
            return Ok(sums);
        }

        // The target file metadata.
        let file = self.client.head_object().bucket(&self.bucket).key(SumsFile::format_target_file(&self.key)).send().await?;

        // The sums file if it exists.
        let sums = match self.client.get_object().bucket(&self.bucket).key(&self.key).send().await {
            Ok(sums) => Some(sums),
            Err(err) if err.into_service_error().is_no_such_key() => None,
            Err(err) => return Err(err.into()),
        };



        match sums {
            Ok(_) => {}
            Err(err) => match err {
                SdkError::ConstructionFailure(_) => {}
                SdkError::TimeoutError(_) => {}
                SdkError::DispatchFailure(_) => {}
                SdkError::ResponseError(_) => {}
                SdkError::ServiceError(_) => {}
            }
        }
    }
    
    /// Get the inner values not including the S3 client.
    pub fn into_inner(self) -> (String, String) {
        (self.bucket, self.key)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::error::Result;

    #[tokio::test]
    pub async fn test_parse_url() -> Result<()> {
        let s3 = expected_s3("s3://bucket/key").await?;
        assert_eq!(s3.into_inner(), ("bucket".to_string(), "key".to_string()));

        let s3 = expected_s3("s3://bucket/key/").await?;
        assert_eq!(s3.into_inner(), ("bucket".to_string(), "key/".to_string()));

        let s3 = expected_s3("file://bucket/key").await;
        assert!(s3.is_err());

        let s3 = expected_s3("s3://bucket/").await;
        assert!(s3.is_err());

        let s3 = expected_s3("s3://").await;
        assert!(s3.is_err());

        Ok(())
    }
    
    async fn expected_s3(url: &str) -> Result<S3> {
        S3Builder::default().parse_from_url(url)?.with_default_client().await.build()
    }
}
