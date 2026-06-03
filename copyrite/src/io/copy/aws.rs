//! AWS checksums and functionality.
//!

use crate::checksum::file::SumsFile;
use crate::cli::MetadataCopy;
use crate::error::Error::{CopyError, ParseError};
use crate::error::{ApiError, Error, Result};
use crate::io::S3Client;
use crate::io::copy::{
    CopyContent, CopyResult, CopyState, MultiPartOptions, ObjectCopy, Part, Reopen,
};
use aws_sdk_s3::operation::get_object_tagging::{GetObjectTaggingError, GetObjectTaggingOutput};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, CopyPartResult, MetadataDirective,
    TaggingDirective,
};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Bytes;
use futures_util::stream::poll_fn;
use futures_util::{StreamExt, TryStreamExt};
use http_body::Frame;
use http_body_util::StreamBody;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::result;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use tokio_util::io::ReaderStream;

/// The number of chunks buffered when re-trying an SDK body.
const REOPEN_CHANNEL_CAPACITY: usize = 16;

/// The read buffer capacity used when streaming a reader into an upload body.
const READER_STREAM_CAPACITY: usize = 64 * 1024;

/// Build an S3 sums object.
#[derive(Debug, Default)]
pub struct S3Builder {
    client: Option<S3Client>,
    metadata_mode: MetadataCopy,
    tag_mode: MetadataCopy,
    source: Option<BucketKey>,
    destination: Option<BucketKey>,
}

impl S3Builder {
    /// Set the client.
    pub fn with_client(mut self, client: S3Client) -> Self {
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
        S3Client,
        MetadataCopy,
        MetadataCopy,
        Option<BucketKey>,
        Option<BucketKey>,
    )> for S3
{
    fn from(
        (client, metadata_mode, tag_mode, source, destination): (
            S3Client,
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
    client: S3Client,
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
        self.client.head_object(|b| b.bucket(bucket).key(key)).await
    }

    /// Get the object tagging.
    pub async fn tagging(
        &self,
        key: &str,
        bucket: &str,
    ) -> result::Result<GetObjectTaggingOutput, SdkError<GetObjectTaggingError, HttpResponse>> {
        self.client
            .get_object_tagging(|b| b.bucket(bucket).key(key))
            .await
    }

    /// Create a new S3 object.
    pub fn new(
        client: S3Client,
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
                .create_multipart_upload(|b| {
                    b.set_tagging(tagging)
                        .set_metadata(metadata)
                        .set_checksum_algorithm(additional_checksum)
                        .bucket(bucket)
                        .key(key)
                })
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
                .copy_object(move |b| {
                    b.tagging_directive(tagging)
                        .set_tagging(tagging_set)
                        .metadata_directive(metadata)
                        .set_metadata(metadata_set)
                        .set_checksum_algorithm(additional_checksum)
                        .copy_source(Self::copy_source(&source.key, &source.bucket))
                        .key(&destination.key)
                        .bucket(&destination.bucket)
                })
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
            let part_number_i32 = i32::try_from(part_number)?;
            let range = multi_part
                .format_range()
                .ok_or_else(|| Error::aws_error("invalid range".to_string()))?;
            let response = self
                .client
                .upload_part_copy(|b| {
                    b.upload_id(&upload_id)
                        .part_number(part_number_i32)
                        .key(&destination.key)
                        .bucket(&destination.bucket)
                        .copy_source(Self::copy_source(&source.key, &source.bucket))
                        .copy_source_range(range)
                })
                .await?;

            let part = response
                .copy_part_result
                .ok_or_else(|| Error::aws_error("missing copy part result".to_string()))?;

            let mut result: CopyResult = (part, part_number, upload_id).into();
            result.bytes_transferred = multi_part.bytes_transferred();
            result = result.with_api_errors(api_errors)?;

            Ok(result)
        } else {
            let parts = multi_part.parts.ok_or_else(|| {
                Error::aws_error("missing parts for multipart completion".to_string())
            })?;
            self.complete_multipart_upload(
                &destination.key,
                &destination.bucket,
                upload_id.to_string(),
                parts,
            )
            .await?;

            CopyResult::new(None, Some(upload_id), 0, vec![])
        }
    }

    /// Get the object from S3. The returned content carries a reopen function that re-issues the
    /// same ranged get.
    pub async fn get_object(&self, multi_part: Option<MultiPartOptions>) -> Result<CopyContent> {
        let source = self.get_source()?;

        if let Some(multipart) = &multi_part
            && multipart.part_number.is_none()
        {
            return Ok(CopyContent::empty());
        }

        let range = multi_part
            .as_ref()
            .and_then(|multi_part| multi_part.format_range());

        let result = self
            .client
            .get_object(|b| b.bucket(&source.bucket).key(&source.key).set_range(range))
            .await?;

        let self_clone = self.clone();
        CopyContent::builder(Box::new(result.body.into_async_read()))
            .with_reopen(move || self_clone.reopen_get(multi_part.clone()))
            .build()
    }

    /// Re-derive the object stream from the source.
    fn reopen_get(
        &self,
        multi_part: Option<MultiPartOptions>,
    ) -> Pin<Box<dyn Future<Output = Result<CopyContent>> + Send>> {
        let self_clone = self.clone();
        Box::pin(async move { self_clone.get_object(multi_part).await })
    }

    /// Wrap an async reader into an `SdkBody`.
    fn reader_body(reader: Box<dyn AsyncRead + Sync + Send + Unpin>) -> SdkBody {
        let stream =
            ReaderStream::with_capacity(reader, READER_STREAM_CAPACITY).map_ok(Frame::data);
        SdkBody::from_body_1_x(StreamBody::new(stream))
    }

    /// Build a streaming `SdkBody` that re-tries its data from the source. This allows the SDK body
    /// to be re-tried automatically when needed.
    fn reopen_body(reopen: Arc<Reopen>) -> SdkBody {
        let (tx, mut rx) =
            mpsc::channel::<result::Result<Bytes, io::Error>>(REOPEN_CHANNEL_CAPACITY);
        tokio::spawn(async move {
            match (*reopen)().await {
                Ok(content) => {
                    let mut stream =
                        ReaderStream::with_capacity(content.data, READER_STREAM_CAPACITY);
                    while let Some(chunk) = stream.next().await {
                        if tx.send(chunk).await.is_err() {
                            break;
                        }
                    }
                }
                Err(err) => {
                    let _ = tx.send(Err(io::Error::other(err.to_string()))).await;
                }
            }
        });

        let stream = poll_fn(move |cx| rx.poll_recv(cx)).map_ok(Frame::data);
        SdkBody::from_body_1_x(StreamBody::new(stream))
    }

    /// Build a retryable streaming `ByteStream`.
    fn retryable_body(
        initial: Option<Box<dyn AsyncRead + Sync + Send + Unpin>>,
        reopen: Arc<Reopen>,
    ) -> ByteStream {
        let initial = Mutex::new(initial);
        ByteStream::new(SdkBody::retryable(move || {
            match initial.lock().unwrap_or_else(|err| err.into_inner()).take() {
                Some(reader) => Self::reader_body(reader),
                None => Self::reopen_body(Arc::clone(&reopen)),
            }
        }))
    }

    /// Build the retryable upload body for a copy content.
    fn upload_body(content: CopyContent) -> ByteStream {
        Self::retryable_body(Some(content.data), Arc::new(content.reopen))
    }

    /// Put the object to S3 by streaming the content directly to the destination.
    pub async fn put_object(&self, content: CopyContent, state: &CopyState) -> Result<CopyResult> {
        // Best effort tagging needs to reissue the upload without tags.
        if self.tag_mode.is_best_effort() {
            return self.put_object_best_effort(content, state).await;
        }

        let destination = self.get_destination()?;
        self.send_put_object(
            destination,
            Self::upload_body(content),
            state.tags(),
            state.metadata(),
            state.additional_ctx().map(ChecksumAlgorithm::from),
            i64::try_from(state.size())?,
        )
        .await?;

        CopyResult::new(None, None, state.size(), vec![])
    }

    /// Send a streaming `PutObject` request to the destination.
    async fn send_put_object(
        &self,
        destination: &BucketKey,
        body: ByteStream,
        tags: Option<String>,
        metadata: Option<HashMap<String, String>>,
        additional_checksum: Option<ChecksumAlgorithm>,
        content_length: i64,
    ) -> result::Result<PutObjectOutput, SdkError<PutObjectError, HttpResponse>> {
        let bucket = destination.bucket.clone();
        let key = destination.key.clone();
        self.client
            .put_object(move |b| {
                b.set_tagging(tags)
                    .set_metadata(metadata)
                    .set_checksum_algorithm(additional_checksum)
                    .content_length(content_length)
                    .bucket(bucket)
                    .key(key)
                    .body(body)
            })
            .await
    }

    /// Put the object to S3 for best effort tagging. This will take into account access denied
    /// errors and re-try if needed.
    async fn put_object_best_effort(
        &self,
        content: CopyContent,
        state: &CopyState,
    ) -> Result<CopyResult> {
        let destination = self.get_destination()?;
        let additional_checksum = state.additional_ctx().map(ChecksumAlgorithm::from);
        let content_length = i64::try_from(state.size())?;

        let CopyContent { data, reopen } = content;
        let reopen = Arc::new(reopen);

        let result = self
            .send_put_object(
                destination,
                Self::retryable_body(Some(data), Arc::clone(&reopen)),
                state.tags(),
                state.metadata(),
                additional_checksum.clone(),
                content_length,
            )
            .await;

        let err = match result {
            Ok(_) => return CopyResult::new(None, None, state.size(), vec![]),
            Err(err) => err,
        };

        // Only retry without tags on access denied.
        let api_error = ApiError::from(&err);
        if !api_error.is_access_denied() {
            return Err(err.into());
        }

        self.send_put_object(
            destination,
            Self::retryable_body(None, reopen),
            None,
            state.metadata(),
            additional_checksum,
            content_length,
        )
        .await?;

        CopyResult::new(None, None, state.size(), vec![api_error])
    }

    /// Upload objects using multi part uploads.
    pub async fn put_object_multipart(
        &self,
        content: CopyContent,
        multi_part: MultiPartOptions,
        state: &CopyState,
    ) -> Result<CopyResult> {
        let destination = self.get_destination()?;

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
            let part_number_i32 = i32::try_from(part_number)?;
            let content_length = i64::try_from(multi_part.bytes_transferred())?;
            let part = self
                .client
                .upload_part(|b| {
                    b.upload_id(&upload_id)
                        .set_checksum_algorithm(additional_checksum)
                        .content_length(content_length)
                        .part_number(part_number_i32)
                        .key(&destination.key)
                        .bucket(&destination.bucket)
                        .body(Self::upload_body(content))
                })
                .await?;

            let mut result: CopyResult = (part, part_number, upload_id).into();
            result.bytes_transferred = multi_part.bytes_transferred();
            result = result.with_api_errors(err)?;

            Ok(result)
        } else {
            let parts = multi_part.parts.ok_or_else(|| {
                Error::aws_error("missing parts for multipart completion".to_string())
            })?;
            self.complete_multipart_upload(
                &destination.key,
                &destination.bucket,
                upload_id.to_string(),
                parts,
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
        parts.sort_by_key(|a| a.part_number);

        let parts = parts
            .into_iter()
            .map(|part| part.try_into())
            .collect::<Result<Vec<_>>>()?;
        self.client
            .complete_multipart_upload(|b| {
                b.bucket(bucket)
                    .key(key)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(parts))
                            .build(),
                    )
                    .upload_id(upload_id)
            })
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
        self.get_object(multi_part).await
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

    fn max_object_size(&self) -> u64 {
        // S3 objects can be at most 50 TiB.
        54975581388800
    }

    async fn initialize_state(&self) -> Result<CopyState> {
        let source = self.get_source()?;

        self.initialize_state(source.key.to_string(), source.bucket.to_string())
            .await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::io::copy::CopyContent;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::config::SharedAsyncSleep;
    use aws_sdk_s3::config::retry::RetryConfig;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_smithy_async::rt::sleep::TokioSleep;
    use aws_smithy_mocks::{MockResponseInterceptor, Rule, RuleMode, mock};
    use aws_smithy_types::byte_stream::ByteStream;
    use aws_smithy_types::error::ErrorMetadata;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    const BUCKET: &str = "bucket";
    const KEY: &str = "key";
    const BODY: &[u8] = b"test";

    /// Build a mock client that enables real retries with backoff so the SDK retry layer actually
    /// re-drives the request body through the reopen factory.
    fn retrying_mock_client(rules: &[&Rule]) -> Client {
        let mut interceptor = MockResponseInterceptor::new().rule_mode(RuleMode::MatchAny);
        for rule in rules {
            interceptor = interceptor.with_rule(rule);
        }

        Client::from_conf(
            aws_sdk_s3::config::Config::builder()
                .with_test_defaults_v2()
                .http_client(aws_smithy_mocks::create_mock_http_client())
                .sleep_impl(SharedAsyncSleep::new(TokioSleep::new()))
                .retry_config(RetryConfig::standard().with_max_attempts(3))
                .interceptor(interceptor)
                .build(),
        )
    }

    /// A `get_object` rule that streams the test body.
    fn get_object_rule() -> Rule {
        mock!(Client::get_object)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .sequence()
            .output(|| {
                GetObjectOutput::builder()
                    .body(ByteStream::from_static(BODY))
                    .build()
            })
            .repeatedly()
            .build()
    }

    /// Build an S3 source from a mock client.
    fn s3_source(client: Client) -> S3 {
        S3Builder::default()
            .with_client(S3Client::new(Arc::new(client), false, false))
            .with_source(BUCKET, KEY)
            .build()
            .unwrap()
    }

    /// Build an S3 destination from a mock client.
    fn s3_destination(client: Client, tag_mode: MetadataCopy) -> S3 {
        S3Builder::default()
            .with_client(S3Client::new(Arc::new(client), false, false))
            .with_copy_tags(tag_mode)
            .with_destination(BUCKET, KEY)
            .build()
            .unwrap()
    }

    /// Test copy state.
    fn copy_state() -> CopyState {
        CopyState::new(BODY.len() as u64, Some("tag=value".to_string()), None)
    }

    /// Download the mock source.
    async fn download<F, Fut>(get_object: &Rule, upload: F) -> Result<CopyResult>
    where
        F: FnOnce(CopyContent) -> Fut,
        Fut: Future<Output = Result<CopyResult>>,
    {
        let source = s3_source(retrying_mock_client(&[get_object]));
        let content = source.download(None).await?;
        upload(content).await
    }

    /// Download the mock source then upload to a copy mode destination backed by `put_object`.
    async fn test_download(get_object: &Rule, put_object: &Rule) -> Result<CopyResult> {
        download(get_object, |content| {
            let destination =
                s3_destination(retrying_mock_client(&[put_object]), MetadataCopy::Copy);
            async move { destination.put_object(content, &copy_state()).await }
        })
        .await
    }

    /// A `put_object` rule that returns 503 `failures` times and then succeeds.
    fn test_put_object(failures: usize) -> Rule {
        mock!(Client::put_object)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .sequence()
            .http_status(503, None)
            .times(failures)
            .output(|| PutObjectOutput::builder().build())
            .build()
    }

    /// A `put_object` rule that always returns 503.
    fn test_put_object_failing() -> Rule {
        mock!(Client::put_object)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .sequence()
            .http_status(503, None)
            .repeatedly()
            .build()
    }

    #[tokio::test]
    async fn put_object_retries() {
        let get_object = get_object_rule();
        let put_object = test_put_object(2);

        let result = test_download(&get_object, &put_object).await;
        assert!(result.is_ok());
        assert_eq!(put_object.num_calls(), 3);
    }

    #[tokio::test]
    async fn put_object_retries_exceeded() {
        let get_object = get_object_rule();
        let put_object = test_put_object_failing();

        let result = test_download(&get_object, &put_object).await;
        assert!(result.is_err());
        assert_eq!(put_object.num_calls(), 3);
    }

    #[tokio::test]
    async fn put_object_best_effort() {
        let get_object = get_object_rule();
        let put_object = mock!(Client::put_object)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .sequence()
            .error(|| {
                PutObjectError::generic(ErrorMetadata::builder().code("AccessDenied").build())
            })
            .output(|| PutObjectOutput::builder().build())
            .build();

        let result = download(&get_object, |content| {
            let destination = s3_destination(
                retrying_mock_client(&[&put_object]),
                MetadataCopy::BestEffort,
            );
            async move { destination.put_object(content, &copy_state()).await }
        })
        .await
        .unwrap();
        assert_eq!(put_object.num_calls(), 2);
        assert_eq!(result.api_errors.len(), 1);
        assert!(result.api_errors[0].is_access_denied());
    }

    #[tokio::test]
    async fn put_object_best_effort_propagates() {
        let get_object = get_object_rule();
        let put_object = mock!(Client::put_object)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .sequence()
            .error(|| {
                PutObjectError::generic(ErrorMetadata::builder().code("InvalidRequest").build())
            })
            .repeatedly()
            .build();

        let result = download(&get_object, |content| {
            let destination = s3_destination(
                retrying_mock_client(&[&put_object]),
                MetadataCopy::BestEffort,
            );
            async move { destination.put_object(content, &copy_state()).await }
        })
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn upload_part_retries_transient_error() {
        let get_object = get_object_rule();
        let create = mock!(Client::create_multipart_upload)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .sequence()
            .output(|| {
                CreateMultipartUploadOutput::builder()
                    .upload_id("upload-id")
                    .build()
            })
            .repeatedly()
            .build();
        let upload_part = mock!(Client::upload_part)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .sequence()
            .http_status(503, None)
            .times(2)
            .output(|| UploadPartOutput::builder().e_tag("etag").build())
            .build();

        let source = s3_source(retrying_mock_client(&[&get_object]));
        let options = MultiPartOptions {
            part_number: Some(1),
            start: 0,
            end: BODY.len() as u64,
            ..Default::default()
        };
        let content = source.download(Some(options.clone())).await.unwrap();

        let destination = s3_destination(
            retrying_mock_client(&[&create, &upload_part]),
            MetadataCopy::Copy,
        );
        let result = destination
            .put_object_multipart(content, options, &copy_state())
            .await;

        assert!(result.is_ok());
        assert_eq!(upload_part.num_calls(), 3);
    }

    #[tokio::test]
    async fn reopen_reproduces_source() {
        let get_object = get_object_rule();
        let source = s3_source(retrying_mock_client(&[&get_object]));

        let content = source.download(None).await.unwrap();
        let mut reopened = (content.reopen)().await.unwrap();

        let mut buf = Vec::new();
        reopened.data.read_to_end(buf.as_mut()).await.unwrap();
        assert_eq!(buf, BODY);
    }
}
