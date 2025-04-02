//! AWS checksums and functionality.
//!

use crate::checksum::file::SumsFile;
use crate::error::Error::ParseError;
use crate::error::Result;
use crate::io::copy::{CopyContent, ObjectCopy};
use crate::io::Provider;
use crate::MetadataCopy;
use aws_sdk_s3::types::{MetadataDirective, TaggingDirective};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use bytes::Bytes;
use futures_util::StreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use std::collections::HashMap;
use tokio_util::codec::{BytesCodec, FramedRead};

/// Build an S3 sums object.
#[derive(Debug, Default)]
pub struct S3Builder {
    client: Option<Client>,
    metadata_mode: MetadataCopy,
}

impl S3Builder {
    /// Set the client.
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    fn get_components(self) -> Result<(Client, MetadataCopy)> {
        let error_fn = || {
            ParseError(
                "client, bucket, key and destinations are required in `S3Builder`".to_string(),
            )
        };

        Ok((self.client.ok_or_else(error_fn)?, self.metadata_mode))
    }

    /// Set the copy metadata option.
    pub fn with_copy_metadata(mut self, metadata_mode: MetadataCopy) -> Self {
        self.metadata_mode = metadata_mode;
        self
    }

    /// Build using the client, bucket and key.
    pub fn build(self) -> Result<S3> {
        Ok(self.get_components()?.into())
    }
}

impl From<(Client, MetadataCopy)> for S3 {
    fn from((client, metadata_mode): (Client, MetadataCopy)) -> Self {
        Self::new(client, metadata_mode)
    }
}

/// An S3 object and AWS-related existing sums.
#[derive(Debug, Clone)]
pub struct S3 {
    client: Client,
    metadata_mode: MetadataCopy,
}

impl S3 {
    /// Create a new S3 object.
    pub fn new(client: Client, metadata_mode: MetadataCopy) -> S3 {
        Self {
            client,
            metadata_mode,
        }
    }

    fn is_access_denied<T: ProvideErrorMetadata>(err: &SdkError<T, HttpResponse>) -> bool {
        if let Some(err) = err.as_service_error() {
            if err
                .code()
                .is_some_and(|code| code == "AccessDenied" || code == "InvalidSecurity")
            {
                return true;
            }
        }

        false
    }

    /// Copy the object using the `CopyObject` operation.
    pub async fn copy_object_single(
        &self,
        key: String,
        bucket: String,
        destination_key: String,
        destination_bucket: String,
    ) -> Result<Option<u64>> {
        let key = SumsFile::format_target_file(&key);
        let size = self
            .client
            .head_object()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .send()
            .await?
            .content_length;

        let (tagging, tagging_set) = if self.metadata_mode.is_copy() {
            (TaggingDirective::Copy, None)
        } else {
            (TaggingDirective::Replace, Some("".to_string()))
        };

        let (metadata, metadata_set) = if self.metadata_mode.is_copy() {
            (MetadataDirective::Copy, None)
        } else {
            (MetadataDirective::Replace, Some(HashMap::new()))
        };

        let result = self
            .client
            .copy_object()
            .tagging_directive(tagging)
            .set_tagging(tagging_set)
            .metadata_directive(metadata)
            .set_metadata(metadata_set)
            .copy_source(format!("{}/{}", bucket, key))
            .key(SumsFile::format_target_file(&destination_key))
            .bucket(destination_bucket)
            .send()
            .await;

        if self.metadata_mode.is_best_effort() && result.as_ref().is_err_and(Self::is_access_denied)
        {
            return Ok(None);
        }

        Ok(size.map(u64::try_from).transpose()?)
    }

    /// Get the object from S3.
    pub async fn get_object(&self, key: String, bucket: String) -> Result<CopyContent> {
        let result = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await?;

        let tags = if self.metadata_mode.is_copy() {
            let tags = self
                .client
                .get_object_tagging()
                .bucket(bucket)
                .key(key)
                .send()
                .await?;
            let tags = tags
                .tag_set
                .into_iter()
                .map(|tag| format!("{}={}", tag.key(), tag.value()))
                .collect::<Vec<_>>()
                .join("&");
            Some(tags)
        } else {
            None
        };

        let metadata = if self.metadata_mode.is_copy() {
            result.metadata
        } else {
            None
        };

        Ok(CopyContent::new(
            Box::new(result.body.into_async_read()),
            tags,
            metadata,
        ))
    }

    /// Put the object to S3.
    pub async fn put_object(
        &self,
        key: String,
        bucket: String,
        content: CopyContent,
    ) -> Result<Option<u64>> {
        let data = content.data;
        let stream = StreamBody::new(
            FramedRead::new(data, BytesCodec::new())
                .map(|chunk| chunk.map(|chunk| Frame::data(Bytes::from(chunk)))),
        );
        let body = SdkBody::from_body_1_x(stream);

        let output = self
            .client
            .put_object()
            .set_tagging(content.tags)
            .set_metadata(content.metadata)
            .bucket(bucket)
            .key(key)
            .body(ByteStream::new(body))
            .send()
            .await;

        if self.metadata_mode.is_best_effort() && output.as_ref().is_err_and(Self::is_access_denied)
        {
            return Ok(None);
        }

        Ok(output?.size.map(u64::try_from).transpose()?)
    }
}

#[async_trait::async_trait]
impl ObjectCopy for S3 {
    async fn copy_object(
        &self,
        provider_source: Provider,
        provider_destination: Provider,
    ) -> Result<Option<u64>> {
        let (bucket, key) = provider_source.into_s3()?;
        let (destination_bucket, destination_key) = provider_destination.into_s3()?;

        self.copy_object_single(key, bucket, destination_key, destination_bucket)
            .await
    }

    async fn download(&self, source: Provider) -> Result<CopyContent> {
        let (bucket, key) = source.into_s3()?;
        Ok(self.get_object(key, bucket).await?)
    }

    async fn upload(&self, destination: Provider, data: CopyContent) -> Result<Option<u64>> {
        let (bucket, key) = destination.into_s3()?;
        self.put_object(key, bucket, data).await
    }
}
