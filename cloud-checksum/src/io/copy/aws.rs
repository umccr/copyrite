//! AWS checksums and functionality.
//!

use crate::checksum::file::SumsFile;
use crate::error::Error::ParseError;
use crate::error::Result;
use crate::io::copy::ObjectCopy;
use crate::io::Provider;
use aws_sdk_s3::Client;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Bytes;
use futures_util::StreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use tokio::io::AsyncRead;
use tokio_util::codec::{BytesCodec, FramedRead};

/// Build an S3 sums object.
#[derive(Debug, Default)]
pub struct S3Builder {
    client: Option<Client>,
}

impl S3Builder {
    /// Set the client.
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    fn get_components(self) -> Result<Client> {
        let error_fn = || {
            ParseError(
                "client, bucket, key and destinations are required in `S3Builder`".to_string(),
            )
        };

        self.client.ok_or_else(error_fn)
    }

    /// Build using the client, bucket and key.
    pub fn build(self) -> Result<S3> {
        Ok(self.get_components()?.into())
    }
}

impl From<Client> for S3 {
    fn from(client: Client) -> Self {
        Self::new(client)
    }
}

/// An S3 object and AWS-related existing sums.
#[derive(Debug, Clone)]
pub struct S3 {
    client: Client,
}

impl S3 {
    /// Create a new S3 object.
    pub fn new(client: Client) -> S3 {
        Self { client }
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

        self.client
            .copy_object()
            .copy_source(format!("{}/{}", bucket, key))
            .key(SumsFile::format_target_file(&destination_key))
            .bucket(destination_bucket)
            .send()
            .await?;

        Ok(size.map(u64::try_from).transpose()?)
    }

    /// Get the object from S3.
    pub async fn get_object(&self, key: String, bucket: String) -> Result<impl AsyncRead> {
        Ok(self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?
            .body
            .into_async_read())
    }

    /// Put the object to S3.
    pub async fn put_object(
        &self,
        key: String,
        bucket: String,
        data: impl AsyncRead + Send + Sync + 'static,
    ) -> Result<Option<u64>> {
        let stream = StreamBody::new(
            FramedRead::new(data, BytesCodec::new())
                .map(|chunk| chunk.map(|chunk| Frame::data(Bytes::from(chunk)))),
        );
        let body = SdkBody::from_body_1_x(stream);

        let output = self
            .client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::new(body))
            .send()
            .await?;

        Ok(output.size.map(u64::try_from).transpose()?)
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

    async fn download(&self, source: Provider) -> Result<Box<dyn AsyncRead + Sync + Send + Unpin>> {
        let (bucket, key) = source.into_s3()?;
        Ok(Box::new(self.get_object(key, bucket).await?))
    }

    async fn upload(
        &self,
        destination: Provider,
        data: Box<dyn AsyncRead + Sync + Send + Unpin>,
    ) -> Result<Option<u64>> {
        let (bucket, key) = destination.into_s3()?;
        self.put_object(key, bucket, data).await
    }
}
