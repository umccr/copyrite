//! AWS checksums and functionality.
//!

use crate::checksum::file::SumsFile;
use crate::cli::MetadataCopy;
use crate::error::Error::{CopyError, ParseError};
use crate::error::{ApiError, Error, Result};
use crate::io::copy::{CopyContent, CopyResult, CopyState, MultiPartOptions, ObjectCopy, Part};
use aws_sdk_s3::operation::get_object_tagging::{GetObjectTaggingError, GetObjectTaggingOutput};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, CopyPartResult, MetadataDirective,
    TaggingDirective,
};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::byte_stream::ByteStream;
use std::collections::HashMap;
use std::result;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

/// Build an S3 sums object.
#[derive(Debug, Default)]
pub struct S3Builder {
    client: Option<Arc<Client>>,
    metadata_mode: MetadataCopy,
    tag_mode: MetadataCopy,
    source: Option<BucketKey>,
    destination: Option<BucketKey>,
}

impl S3Builder {
    /// Set the client.
    pub fn with_client(mut self, client: Arc<Client>) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the source.
    pub fn with_source(mut self, bucket: &str, key: &str) -> Self {
        self.source = Some(BucketKey {
            bucket: bucket.to_string(),
            key: SumsFile::format_target_file(key),
        });
        self
    }

    /// Set the destination.
    pub fn with_destination(mut self, bucket: &str, key: &str) -> Self {
        self.destination = Some(BucketKey {
            bucket: bucket.to_string(),
            key: SumsFile::format_target_file(key),
        });
        self
    }

    /// Set the copy metadata option.
    pub fn with_copy_metadata(mut self, metadata_mode: MetadataCopy) -> Self {
        self.metadata_mode = metadata_mode;
        self
    }

    /// Set the copy metadata option.
    pub fn with_copy_tags(mut self, tag_mode: MetadataCopy) -> Self {
        self.tag_mode = tag_mode;
        self
    }

    /// Build using the client, bucket and key.
    pub fn build(self) -> Result<S3> {
        let error_fn = || {
            ParseError(
                "client, bucket, key and destinations are required in `S3Builder`".to_string(),
            )
        };

        Ok((
            self.client.ok_or_else(error_fn)?,
            self.metadata_mode,
            self.tag_mode,
            self.source,
            self.destination,
        )
            .into())
    }
}

impl
    From<(
        Arc<Client>,
        MetadataCopy,
        MetadataCopy,
        Option<BucketKey>,
        Option<BucketKey>,
    )> for S3
{
    fn from(
        (client, metadata_mode, tag_mode, source, destination): (
            Arc<Client>,
            MetadataCopy,
            MetadataCopy,
            Option<BucketKey>,
            Option<BucketKey>,
        ),
    ) -> Self {
        Self::new(client, metadata_mode, tag_mode, source, destination)
    }
}

impl From<(CopyPartResult, u64, String)> for CopyResult {
    fn from((part, part_number, upload_id): (CopyPartResult, u64, String)) -> Self {
        (
            Part {
                crc32: part.checksum_crc32,
                crc32_c: part.checksum_crc32_c,
                sha1: part.checksum_sha1,
                sha256: part.checksum_sha256,
                crc64_nvme: part.checksum_crc64_nvme,
                e_tag: part.e_tag,
                part_number,
            },
            upload_id,
        )
            .into()
    }
}

impl From<(UploadPartOutput, u64, String)> for CopyResult {
    fn from((part, part_number, upload_id): (UploadPartOutput, u64, String)) -> Self {
        (
            Part {
                crc32: part.checksum_crc32,
                crc32_c: part.checksum_crc32_c,
                sha1: part.checksum_sha1,
                sha256: part.checksum_sha256,
                crc64_nvme: part.checksum_crc64_nvme,
                e_tag: part.e_tag,
                part_number,
            },
            upload_id,
        )
            .into()
    }
}

impl TryFrom<Part> for CompletedPart {
    type Error = Error;

    fn try_from(part: Part) -> Result<Self> {
        Ok(CompletedPart::builder()
            .set_checksum_crc32(part.crc32)
            .set_checksum_crc32_c(part.crc32_c)
            .set_checksum_sha1(part.sha1)
            .set_checksum_sha256(part.sha256)
            .set_checksum_crc64_nvme(part.crc64_nvme)
            .set_e_tag(part.e_tag)
            .set_part_number(Some(i32::try_from(part.part_number)?))
            .build())
    }
}

/// Represents an S3 bucket and key.
#[derive(Debug, Clone)]
pub struct BucketKey {
    bucket: String,
    key: String,
}

/// An S3 object and AWS-related existing sums.
#[derive(Debug, Clone)]
pub struct S3 {
    client: Arc<Client>,
    metadata_mode: MetadataCopy,
    tag_mode: MetadataCopy,
    source: Option<BucketKey>,
    destination: Option<BucketKey>,
}

impl S3 {
    /// Initialize the state for a bucket and key.
    pub async fn initialize_state(&self, key: String, bucket: String) -> Result<CopyState> {
        let head = self.head_object(&key, &bucket).await?;
        let tags = self.tagging(&key, &bucket).await;

        // Getting tags could fail, that's okay if using best-effort mode.
        let tags = if self.tag_mode.is_best_effort() {
            None
        } else {
            Some(
                tags?
                    .tag_set
                    .iter()
                    .map(|tag| format!("{}={}", tag.key(), tag.value()))
                    .collect::<Vec<_>>()
                    .join("&"),
            )
        };

        let size = head
            .content_length
            .map(u64::try_from)
            .transpose()?
            .ok_or_else(|| Error::aws_error("missing size".to_string()))?;
        let metadata = head.metadata;

        Ok(CopyState::new(size, tags, metadata))
    }

    /// Get the head object output.
    pub async fn head_object(
        &self,
        key: &str,
        bucket: &str,
    ) -> result::Result<HeadObjectOutput, SdkError<HeadObjectError, HttpResponse>> {
        self.client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
    }

    /// Get the object tagging.
    pub async fn tagging(
        &self,
        key: &str,
        bucket: &str,
    ) -> result::Result<GetObjectTaggingOutput, SdkError<GetObjectTaggingError, HttpResponse>> {
        self.client
            .get_object_tagging()
            .bucket(bucket)
            .key(key)
            .send()
            .await
    }

    /// Create a new S3 object.
    pub fn new(
        client: Arc<Client>,
        metadata_mode: MetadataCopy,
        tag_mode: MetadataCopy,
        source: Option<BucketKey>,
        destination: Option<BucketKey>,
    ) -> S3 {
        Self {
            client,
            metadata_mode,
            tag_mode,
            source,
            destination,
        }
    }

    /// Create a new multipart upload.
    pub async fn get_multipart_upload(
        &self,
        key: &str,
        bucket: &str,
        tagging: Option<String>,
        metadata: Option<HashMap<String, String>>,
        additional_checksum: Option<ChecksumAlgorithm>,
    ) -> Result<(String, Vec<ApiError>)> {
        let do_upload = |tagging, metadata, additional_checksum| async {
            self.client
                .create_multipart_upload()
                .set_tagging(tagging)
                .set_metadata(metadata)
                .set_checksum_algorithm(additional_checksum)
                .bucket(bucket)
                .key(key)
                .send()
                .await
        };

        let result = do_upload(
            tagging.clone(),
            metadata.clone(),
            additional_checksum.clone(),
        )
        .await;

        // Retry if this is a best effort copy and the error was access denied.
        let (upload, err) = if let Err(ref err) = result {
            let err = ApiError::from(err);
            if self.tag_mode.is_best_effort() && err.is_access_denied() {
                (
                    do_upload(None, metadata, additional_checksum).await?,
                    vec![err],
                )
            } else {
                (result?, vec![])
            }
        } else {
            (result?, vec![])
        };

        Ok((
            upload
                .upload_id
                .ok_or_else(|| Error::aws_error("missing upload id".to_string()))?,
            err,
        ))
    }

    fn get_source(&self) -> Result<&BucketKey> {
        self.source
            .as_ref()
            .ok_or_else(|| CopyError("missing source".to_string()))
    }

    fn get_destination(&self) -> Result<&BucketKey> {
        self.destination
            .as_ref()
            .ok_or_else(|| CopyError("missing destination".to_string()))
    }

    /// Copy the object using the `CopyObject` operation.
    pub async fn copy_object(&self, state: &CopyState) -> Result<CopyResult> {
        let size = state.size();

        let (tagging, tagging_set) = self.tagging_directive();
        let (metadata, metadata_set) = self.metadata_directive();

        let source = self.get_source()?;
        let destination = self.get_destination()?;

        let additional_checksum = state.additional_ctx().map(ChecksumAlgorithm::from);
        let do_copy = |tagging, tagging_set, metadata, metadata_set, additional_checksum| async {
            self.client
                .copy_object()
                .tagging_directive(tagging)
                .set_tagging(tagging_set)
                .metadata_directive(metadata)
                .set_metadata(metadata_set)
                .set_checksum_algorithm(additional_checksum)
                .copy_source(Self::copy_source(&source.key, &source.bucket))
                .key(&destination.key)
                .bucket(&destination.bucket)
                .send()
                .await
        };

        let result = do_copy(
            tagging,
            tagging_set,
            metadata.clone(),
            metadata_set.clone(),
            additional_checksum.clone(),
        )
        .await;

        // Retry if this is a best effort copy and the error was access denied.
        let (_, err) = if let Err(ref err) = result {
            let err = ApiError::from(err);
            if self.tag_mode.is_best_effort() && err.is_access_denied() {
                let result = do_copy(
                    TaggingDirective::Replace,
                    Some("".to_string()),
                    metadata,
                    metadata_set.clone(),
                    additional_checksum,
                )
                .await?;
                (result, vec![err])
            } else {
                (result?, vec![])
            }
        } else {
            (result?, vec![])
        };

        CopyResult::new(None, None, size, err)
    }

    /// Get the copy source.
    fn copy_source(key: &str, bucket: &str) -> String {
        format!("{}/{}", bucket, key)
    }

    /// Extract the metadata directive and metadata to be set.
    fn metadata_directive(&self) -> (MetadataDirective, Option<HashMap<String, String>>) {
        let (metadata, metadata_set) =
            if self.metadata_mode.is_copy() || self.metadata_mode.is_best_effort() {
                (MetadataDirective::Copy, None)
            } else {
                (MetadataDirective::Replace, Some(HashMap::new()))
            };

        (metadata, metadata_set)
    }

    /// Extract the tagging directive and tags to be set.
    fn tagging_directive(&self) -> (TaggingDirective, Option<String>) {
        let (tagging, tagging_set) = if self.tag_mode.is_copy() || self.tag_mode.is_best_effort() {
            (TaggingDirective::Copy, None)
        } else {
            (TaggingDirective::Replace, Some("".to_string()))
        };
        (tagging, tagging_set)
    }

    /// Copy the object using multiple parts.
    pub async fn copy_object_multipart(
        &self,
        multi_part: MultiPartOptions,
        state: &CopyState,
    ) -> Result<CopyResult> {
        let tagging = state.tags();

        let source = self.get_source()?;
        let destination = self.get_destination()?;

        let additional_checksum = state.additional_ctx().map(ChecksumAlgorithm::from);

        // Create the upload id if it doesn't exist or use the existing one.
        let (upload_id, api_errors) = if let Some(upload_id) = &multi_part.upload_id {
            (upload_id.to_string(), vec![])
        } else {
            self.get_multipart_upload(
                &destination.key,
                &destination.bucket,
                tagging,
                state.metadata(),
                additional_checksum,
            )
            .await?
        };

        if let Some(part_number) = multi_part.part_number {
            let part = self
                .client
                .upload_part_copy()
                .upload_id(&upload_id)
                .part_number(i32::try_from(part_number)?)
                .key(&destination.key)
                .bucket(&destination.bucket)
                .copy_source(Self::copy_source(&source.key, &source.bucket))
                .copy_source_range(
                    multi_part
                        .format_range()
                        .ok_or_else(|| Error::aws_error("invalid range".to_string()))?,
                )
                .send()
                .await?
                .copy_part_result
                .ok_or_else(|| Error::aws_error("missing copy part result".to_string()))?;

            let mut result: CopyResult = (part, part_number, upload_id).into();
            result.bytes_transferred = multi_part.bytes_transferred();
            result = result.with_api_errors(api_errors)?;

            Ok(result)
        } else {
            self.complete_multipart_upload(
                &destination.key,
                &destination.bucket,
                upload_id.to_string(),
                multi_part.parts,
            )
            .await?;

            CopyResult::new(None, Some(upload_id), 0, vec![])
        }
    }

    /// Get the object from S3.
    pub async fn get_object(&self, multi_part: Option<MultiPartOptions>) -> Result<CopyContent> {
        let source = self.get_source()?;

        if let Some(multipart) = &multi_part {
            if multipart.part_number.is_none() {
                return Ok(Default::default());
            }
        }

        let result = self
            .client
            .get_object()
            .bucket(&source.bucket)
            .key(&source.key)
            .set_range(
                multi_part
                    .as_ref()
                    .and_then(|multi_part| multi_part.format_range()),
            )
            .send()
            .await?;

        Ok(CopyContent::new(Box::new(result.body.into_async_read())))
    }

    /// Put the object to S3.
    pub async fn put_object(
        &self,
        mut content: CopyContent,
        state: &CopyState,
    ) -> Result<CopyResult> {
        let destination = self.get_destination()?;
        let buf = Self::read_content(&mut content, None).await?;

        let additional_checksum = state.additional_ctx().map(ChecksumAlgorithm::from);
        let do_put = |tags, metadata, additional_checksum, buf| async {
            self.client
                .put_object()
                .set_tagging(tags)
                .set_metadata(metadata)
                .set_checksum_algorithm(additional_checksum)
                .bucket(&destination.bucket)
                .key(&destination.key)
                .body(ByteStream::from(buf))
                .send()
                .await
        };

        let result = do_put(
            state.tags(),
            state.metadata(),
            additional_checksum.clone(),
            buf.clone(),
        )
        .await;

        // Retry if this is a best effort copy and the error was access denied.
        let (_, err) = if let Err(ref err) = result {
            let err = ApiError::from(err);
            if self.tag_mode.is_best_effort() && err.is_access_denied() {
                let result = do_put(None, state.metadata(), additional_checksum, buf).await?;

                (result, vec![err])
            } else {
                (result?, vec![])
            }
        } else {
            (result?, vec![])
        };

        CopyResult::new(None, None, state.size(), err)
    }

    /// Read the copy content into a buffer.
    async fn read_content(
        content: &mut CopyContent,
        multi_part: Option<&MultiPartOptions>,
    ) -> Result<Vec<u8>> {
        if let Some(multi_part) = multi_part {
            if multi_part.part_number.is_none() {
                return Ok(Vec::new());
            }

            let mut buf = vec![0; usize::try_from(multi_part.bytes_transferred())?];
            content.data.read_exact(&mut buf).await?;

            Ok(buf)
        } else {
            let mut buf = vec![];
            content.data.read_to_end(&mut buf).await?;

            Ok(buf)
        }
    }

    /// Upload objects using multi part uploads.
    pub async fn put_object_multipart(
        &self,
        mut content: CopyContent,
        multi_part: MultiPartOptions,
        state: &CopyState,
    ) -> Result<CopyResult> {
        let destination = self.get_destination()?;
        let buf = Self::read_content(&mut content, Some(&multi_part)).await?;

        let additional_checksum = state.additional_ctx().map(ChecksumAlgorithm::from);
        // Create the upload id if it doesn't exist or use the existing one.
        let (upload_id, err) = if let Some(upload_id) = multi_part.upload_id.as_ref() {
            (upload_id.to_string(), vec![])
        } else {
            self.get_multipart_upload(
                &destination.key,
                &destination.bucket,
                state.tags(),
                state.metadata(),
                additional_checksum.clone(),
            )
            .await?
        };

        if let Some(part_number) = multi_part.part_number {
            let part = self
                .client
                .upload_part()
                .upload_id(&upload_id)
                .set_checksum_algorithm(additional_checksum)
                .part_number(i32::try_from(part_number)?)
                .key(&destination.key)
                .bucket(&destination.bucket)
                .body(ByteStream::from(buf))
                .send()
                .await?;

            let mut result: CopyResult = (part, part_number, upload_id).into();
            result.bytes_transferred = multi_part.bytes_transferred();
            result = result.with_api_errors(err)?;

            Ok(result)
        } else {
            self.complete_multipart_upload(
                &destination.key,
                &destination.bucket,
                upload_id.to_string(),
                multi_part.parts,
            )
            .await?;

            CopyResult::new(None, Some(upload_id), 0, err)
        }
    }

    /// Complete a multipart upload.
    async fn complete_multipart_upload(
        &self,
        key: &str,
        bucket: &str,
        upload_id: String,
        mut parts: Vec<Part>,
    ) -> Result<()> {
        // Parts must be ordered.
        parts.sort_by(|a, b| a.part_number.cmp(&b.part_number));

        self.client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(
                        parts
                            .into_iter()
                            .map(|part| part.try_into())
                            .collect::<Result<Vec<_>>>()?,
                    ))
                    .build(),
            )
            .upload_id(upload_id)
            .send()
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectCopy for S3 {
    async fn copy(
        &self,
        multi_part: Option<MultiPartOptions>,
        state: &CopyState,
    ) -> Result<CopyResult> {
        if let Some(multi_part) = multi_part {
            self.copy_object_multipart(multi_part, state).await
        } else {
            self.copy_object(state).await
        }
    }

    async fn download(&self, multi_part: Option<MultiPartOptions>) -> Result<CopyContent> {
        Ok(self.get_object(multi_part).await?)
    }

    async fn upload(
        &self,
        data: CopyContent,
        multi_part: Option<MultiPartOptions>,
        state: &CopyState,
    ) -> Result<CopyResult> {
        if let Some(multi_part) = multi_part {
            self.put_object_multipart(data, multi_part, state).await
        } else {
            self.put_object(data, state).await
        }
    }

    fn max_part_size(&self) -> u64 {
        5368709120
    }

    fn max_parts(&self) -> u64 {
        10000
    }

    fn min_part_size(&self) -> u64 {
        5242880
    }

    async fn initialize_state(&self) -> Result<CopyState> {
        let source = self.get_source()?;

        self.initialize_state(source.key.to_string(), source.bucket.to_string())
            .await
    }
}
