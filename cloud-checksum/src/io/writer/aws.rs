//! AWS checksums and functionality.
//!

use crate::checksum::file::SumsFile;
use crate::error::Result;
use crate::io::writer::ObjectWrite;
use crate::io::{ObjectMeta, Provider};
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::ByteStream;

/// An S3 object and AWS-related existing sums.
#[derive(Debug, Clone)]
pub struct S3 {
    client: Client,
    bucket: String,
    key: String,
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

    /// Write the sums file to the configured location using `PutObject`.
    pub async fn put_sums(&self, sums_file: &SumsFile) -> Result<()> {
        let key = SumsFile::format_sums_file(&self.key);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(sums_file.to_json_string()?.into_bytes()))
            .send()
            .await?;
        Ok(())
    }

    /// Get the inner values.
    pub fn into_inner(self) -> (String, String) {
        (self.bucket, self.key)
    }
}

impl ObjectMeta for S3 {
    fn location(&self) -> String {
        Provider::format_s3(&self.bucket, &self.key)
    }
}

#[async_trait::async_trait]
impl ObjectWrite for S3 {
    async fn write_sums_file(&self, sums_file: &SumsFile) -> Result<()> {
        self.put_sums(sums_file).await
    }
}
