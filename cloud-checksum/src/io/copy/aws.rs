//! AWS checksums and functionality.
//!

use crate::checksum::file::SumsFile;
use crate::checksum::Ctx;
use crate::error::Error::{AwsError, ParseError};
use crate::error::Result;
use crate::io::copy::{CopyContent, MultiPartOptions, ObjectCopy};
use crate::io::Provider;
use crate::MetadataCopy;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, MetadataDirective,
    TaggingDirective,
};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::byte_stream::ByteStream;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use std::collections::HashMap;
use tokio::io::AsyncReadExt;

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
    multipart_upload: HashMap<(String, String), String>,
    completed_parts: HashMap<String, Vec<CompletedPart>>,
}

impl S3 {
    /// Create a new S3 object.
    pub fn new(client: Client, metadata_mode: MetadataCopy) -> S3 {
        Self {
            client,
            metadata_mode,
            multipart_upload: HashMap::new(),
            completed_parts: HashMap::new(),
        }
    }

    /// Check if the error is an access denied error.
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

    /// Create a new multipart upload, or return an existing one if it is in progress for the
    /// bucket and key.
    pub async fn get_multipart_upload(
        &mut self,
        key: String,
        bucket: String,
        tagging: Option<String>,
        metadata: Option<HashMap<String, String>>,
        additional_checksum: Option<ChecksumAlgorithm>,
    ) -> Result<String> {
        let entry = (bucket, key);
        if self.multipart_upload.contains_key(&entry) {
            return Ok(self.multipart_upload[&entry].clone());
        }

        let upload = self
            .client
            .create_multipart_upload()
            .set_tagging(tagging)
            .set_metadata(metadata)
            .set_checksum_algorithm(additional_checksum)
            .bucket(&entry.0)
            .key(&entry.1)
            .send()
            .await?;

        Ok(self
            .multipart_upload
            .entry(entry)
            .or_insert(
                upload
                    .upload_id
                    .ok_or_else(|| AwsError("missing upload id".to_string()))?,
            )
            .to_string())
    }

    /// Reset the cached multipart upload.
    pub fn reset_multipart_upload(&mut self, key: String, bucket: String) {
        let entry = (bucket, key);
        self.multipart_upload.remove(&entry);
    }

    /// Copy the object using the `CopyObject` operation.
    pub async fn copy_object(
        &self,
        key: String,
        bucket: String,
        destination_key: String,
        destination_bucket: String,
        additional_checksum: Option<ChecksumAlgorithm>,
    ) -> Result<Option<u64>> {
        let size = self
            .client
            .head_object()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .send()
            .await?
            .content_length;

        let (tagging, tagging_set) = self.tagging_directive();
        let (metadata, metadata_set) = self.metadata_directive();

        let result = self
            .client
            .copy_object()
            .tagging_directive(tagging)
            .set_tagging(tagging_set)
            .metadata_directive(metadata)
            .set_metadata(metadata_set)
            .set_checksum_algorithm(additional_checksum)
            .copy_source(Self::copy_source(&key, &bucket))
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

    /// Get the copy source.
    fn copy_source(key: &str, bucket: &str) -> String {
        format!("{}/{}", bucket, key)
    }

    /// Extract the metadata directive and metadata to be set.
    fn metadata_directive(&self) -> (MetadataDirective, Option<HashMap<String, String>>) {
        let (metadata, metadata_set) = if self.metadata_mode.is_copy() {
            (MetadataDirective::Copy, None)
        } else {
            (MetadataDirective::Replace, Some(HashMap::new()))
        };

        (metadata, metadata_set)
    }

    /// Extract the tagging directive and tags to be set.
    fn tagging_directive(&self) -> (TaggingDirective, Option<String>) {
        let (tagging, tagging_set) = if self.metadata_mode.is_copy() {
            (TaggingDirective::Copy, None)
        } else {
            (TaggingDirective::Replace, Some("".to_string()))
        };
        (tagging, tagging_set)
    }

    /// Get the url-encoded tagging for the object.
    async fn get_tagging(&self, key: &str, bucket: &str) -> Result<Option<String>> {
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

        Ok(Some(tags))
    }

    /// Get the size of the object.
    pub async fn object_size(&self, key: String, bucket: String) -> Result<Option<u64>> {
        Ok(self
            .client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await?
            .content_length
            .map(u64::try_from)
            .transpose()?)
    }

    /// Copy the object using multiple parts.
    pub async fn copy_object_multipart(
        &mut self,
        key: String,
        bucket: String,
        destination_key: String,
        destination_bucket: String,
        multi_part: MultiPartOptions,
        additional_checksum: Option<ChecksumAlgorithm>,
    ) -> Result<Option<u64>> {
        let head = self
            .client
            .head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await?;
        let tagging = self.get_tagging(&key, &bucket).await?;

        let upload_id = self
            .get_multipart_upload(
                destination_key.to_string(),
                destination_bucket.to_string(),
                tagging,
                head.metadata,
                additional_checksum,
            )
            .await?;

        if let Some(part_number) = multi_part.part_number {
            let part = self
                .client
                .upload_part_copy()
                .upload_id(&upload_id)
                .part_number(i32::try_from(part_number)?)
                .key(destination_key)
                .bucket(destination_bucket)
                .copy_source(Self::copy_source(&key, &bucket))
                .copy_source_range(
                    multi_part
                        .format_range()
                        .ok_or_else(|| AwsError("invalid range".to_string()))?,
                )
                .send()
                .await?
                .copy_part_result
                .ok_or_else(|| AwsError("missing copy part result".to_string()))?;

            let parts = self.completed_parts.entry(upload_id).or_default();
            parts.push(
                CompletedPart::builder()
                    .set_checksum_crc32(part.checksum_crc32)
                    .set_checksum_crc32_c(part.checksum_crc32_c)
                    .set_checksum_sha1(part.checksum_sha1)
                    .set_checksum_sha256(part.checksum_sha256)
                    .set_checksum_crc64_nvme(part.checksum_crc64_nvme)
                    .set_e_tag(part.e_tag)
                    .set_part_number(Some(i32::try_from(part_number)?))
                    .build(),
            );
        } else {
            self.complete_multipart_upload(destination_key, destination_bucket, upload_id)
                .await?;
        }

        Ok(head.content_length.map(u64::try_from).transpose()?)
    }

    /// Get the object from S3.
    pub async fn get_object(
        &self,
        key: String,
        bucket: String,
        multi_part: Option<MultiPartOptions>,
    ) -> Result<CopyContent> {
        let result = self
            .client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .set_part_number(
                multi_part
                    .as_ref()
                    .and_then(|multi_part| multi_part.part_number.map(i32::try_from))
                    .transpose()?,
            )
            .set_range(multi_part.and_then(|multi_part| multi_part.format_range()))
            .send()
            .await?;

        let size = result.content_length.map(u64::try_from).transpose()?;

        let tags = if self.metadata_mode.is_copy() {
            self.get_tagging(&key, &bucket).await?
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
            size,
            tags,
            metadata,
        ))
    }

    /// Put the object to S3.
    pub async fn put_object(
        &self,
        key: String,
        bucket: String,
        mut content: CopyContent,
        additional_checksum: Option<ChecksumAlgorithm>,
    ) -> Result<Option<u64>> {
        let buf = Self::read_content(&mut content).await?;

        let output = self
            .client
            .put_object()
            .set_tagging(content.tags)
            .set_metadata(content.metadata)
            .set_checksum_algorithm(additional_checksum)
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(buf))
            .send()
            .await;

        if self.metadata_mode.is_best_effort() && output.as_ref().is_err_and(Self::is_access_denied)
        {
            return Ok(None);
        }

        Ok(content.size)
    }

    /// Read the copy content into a buffer.
    async fn read_content(content: &mut CopyContent) -> Result<Vec<u8>> {
        let mut buf = if let Some(capacity) = content.size {
            Vec::with_capacity(usize::try_from(capacity)?)
        } else {
            Vec::new()
        };

        content.data.read_to_end(&mut buf).await?;

        Ok(buf)
    }

    /// Upload objects using multi part uploads.
    pub async fn put_object_multipart(
        &mut self,
        key: String,
        bucket: String,
        mut content: CopyContent,
        multi_part: MultiPartOptions,
        additional_checksum: Option<ChecksumAlgorithm>,
    ) -> Result<Option<u64>> {
        let buf = Self::read_content(&mut content).await?;

        let upload_id = self
            .get_multipart_upload(
                key.to_string(),
                bucket.to_string(),
                content.tags,
                content.metadata,
                additional_checksum.clone(),
            )
            .await?;

        if let Some(part_number) = multi_part.part_number {
            let part = self
                .client
                .upload_part()
                .upload_id(&upload_id)
                .set_checksum_algorithm(additional_checksum)
                .part_number(i32::try_from(part_number)?)
                .key(&key)
                .bucket(&bucket)
                .body(ByteStream::from(buf))
                .send()
                .await?;

            let parts = self.completed_parts.entry(upload_id).or_default();
            parts.push(
                CompletedPart::builder()
                    .set_checksum_crc32(part.checksum_crc32)
                    .set_checksum_crc32_c(part.checksum_crc32_c)
                    .set_checksum_sha1(part.checksum_sha1)
                    .set_checksum_sha256(part.checksum_sha256)
                    .set_checksum_crc64_nvme(part.checksum_crc64_nvme)
                    .set_e_tag(part.e_tag)
                    .set_part_number(Some(i32::try_from(part_number)?))
                    .build(),
            );
        } else {
            self.complete_multipart_upload(key, bucket, upload_id)
                .await?;
        }

        Ok(content.size)
    }

    /// Complete a multipart upload.
    async fn complete_multipart_upload(
        &mut self,
        key: String,
        bucket: String,
        upload_id: String,
    ) -> Result<()> {
        // Parts must be ordered.
        let mut parts = self
            .completed_parts
            .remove_entry(&upload_id)
            .ok_or_else(|| AwsError("missing completed parts".to_string()))?
            .1;
        parts.sort_by(|a, b| a.part_number.cmp(&b.part_number));

        self.client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .upload_id(upload_id)
            .send()
            .await?;
        self.reset_multipart_upload(key, bucket);

        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectCopy for S3 {
    async fn copy(
        &mut self,
        provider_source: Provider,
        provider_destination: Provider,
        multi_part: Option<MultiPartOptions>,
        additional_ctx: Option<Ctx>,
    ) -> Result<Option<u64>> {
        let (bucket, key) = provider_source.into_s3()?;
        let (destination_bucket, destination_key) = provider_destination.into_s3()?;

        let key = SumsFile::format_target_file(&key);
        let destination_key = SumsFile::format_target_file(&destination_key);

        let ctx = additional_ctx.map(ChecksumAlgorithm::from);
        if let Some(multi_part) = multi_part {
            self.copy_object_multipart(
                key,
                bucket,
                destination_key,
                destination_bucket,
                multi_part,
                ctx,
            )
            .await
        } else {
            self.copy_object(key, bucket, destination_key, destination_bucket, ctx)
                .await
        }
    }

    async fn download(
        &mut self,
        source: Provider,
        multi_part: Option<MultiPartOptions>,
    ) -> Result<CopyContent> {
        let (bucket, key) = source.into_s3()?;
        let key = SumsFile::format_target_file(&key);

        Ok(self.get_object(key, bucket, multi_part).await?)
    }

    async fn upload(
        &mut self,
        destination: Provider,
        data: CopyContent,
        multi_part: Option<MultiPartOptions>,
        additional_ctx: Option<Ctx>,
    ) -> Result<Option<u64>> {
        let (bucket, key) = destination.into_s3()?;
        let key = SumsFile::format_target_file(&key);

        let ctx = additional_ctx.map(ChecksumAlgorithm::from);
        if let Some(multi_part) = multi_part {
            self.put_object_multipart(key, bucket, data, multi_part, ctx)
                .await
        } else {
            self.put_object(key, bucket, data, ctx).await
        }
    }

    fn max_part_size(&self) -> u64 {
        5368709120
    }

    fn max_parts(&self) -> u64 {
        10000
    }

    fn single_part_limit(&self) -> u64 {
        // AWS docs refer to MB as MiB
        5368709120
    }

    fn min_part_size(&self) -> u64 {
        5242880
    }

    async fn size(&self, source: Provider) -> Result<Option<u64>> {
        let (bucket, key) = source.into_s3()?;

        self.object_size(key, bucket).await
    }
}
