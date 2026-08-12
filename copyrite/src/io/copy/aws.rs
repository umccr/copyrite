//! AWS checksums and functionality.
//!

use crate::checksum::Ctx;
use crate::checksum::file::SumsFile;
use crate::checksum::standard::StandardCtx;
use crate::cli::MetadataCopy;
use crate::error::Error::{CopyError, ParseError};
use crate::error::{ApiError, Error, Result};
use crate::io::S3Client;
use crate::io::copy::{
    CopyContent, CopyResult, CopyState, MultiPartOptions, ObjectCopy, Part, Reopen, SystemMetadata,
};
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::get_object_tagging::{GetObjectTaggingError, GetObjectTaggingOutput};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumType, CompletedMultipartUpload, CompletedPart, CopyPartResult,
    MetadataDirective, Tag, TaggingDirective,
};
use aws_smithy_http::label::EncodingStrategy;
use aws_smithy_http::{label, query};
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use futures_util::stream::poll_fn;
use futures_util::{Stream, StreamExt, TryStreamExt};
use http_body::Frame;
use http_body_util::StreamBody;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::result;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;
use tokio_util::io::{ReaderStream, StreamReader};

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
                sha512: part.checksum_sha512,
                crc64_nvme: part.checksum_crc64_nvme,
                xxhash64: part.checksum_xxhash64,
                xxhash3: part.checksum_xxhash3,
                xxhash128: part.checksum_xxhash128,
                md5: part.checksum_md5,
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
                sha512: part.checksum_sha512,
                crc64_nvme: part.checksum_crc64_nvme,
                xxhash64: part.checksum_xxhash64,
                xxhash3: part.checksum_xxhash3,
                xxhash128: part.checksum_xxhash128,
                md5: part.checksum_md5,
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
            .set_checksum_sha512(part.sha512)
            .set_checksum_crc64_nvme(part.crc64_nvme)
            .set_checksum_xxhash64(part.xxhash64)
            .set_checksum_xxhash3(part.xxhash3)
            .set_checksum_xxhash128(part.xxhash128)
            .set_checksum_md5(part.md5)
            .set_e_tag(part.e_tag)
            .set_part_number(Some(i32::try_from(part.part_number)?))
            .build())
    }
}

impl From<&HeadObjectOutput> for SystemMetadata {
    fn from(head: &HeadObjectOutput) -> Self {
        Self {
            content_type: head.content_type.clone(),
            cache_control: head.cache_control.clone(),
            content_disposition: head.content_disposition.clone(),
            content_encoding: head.content_encoding.clone(),
            content_language: head.content_language.clone(),
        }
    }
}

/// The additional checksum to attach to an upload.
#[derive(Debug, Clone)]
enum UploadChecksum {
    /// An algorithm the SDK computes while streaming the upload.
    Computed(ChecksumAlgorithm),
    /// A precalculated base64 digest for an algorithm.
    Precalculated(Box<StandardCtx>, String),
    /// No additional checksum applies to this upload.
    None,
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

        let mut api_errors = Vec::new();
        let tags = match self.tag_mode {
            MetadataCopy::Suppress => None,
            // Best effort copies proceed without tags on any tagging error, not just access
            // denied, because tagging support varies across S3-compatible endpoints. The error
            // is still recorded so it appears in the copy stats rather than disappearing.
            MetadataCopy::BestEffort => match self.tagging(&key, &bucket).await {
                Ok(output) => Some(Self::format_tag_set(output.tag_set())),
                Err(ref err) => {
                    api_errors.push(ApiError::from(err));
                    None
                }
            },
            MetadataCopy::Copy => Some(Self::format_tag_set(
                self.tagging(&key, &bucket).await?.tag_set(),
            )),
        };

        let size = head
            .content_length
            .map(u64::try_from)
            .transpose()?
            .ok_or_else(|| Error::aws_error("missing size".to_string()))?;

        // System metadata follows the same mode as user metadata, so `Suppress` resets the
        // destination to defaults.
        let (metadata, system_metadata) = match self.metadata_mode {
            MetadataCopy::Suppress => (None, SystemMetadata::default()),
            _ => (head.metadata.clone(), SystemMetadata::from(&head)),
        };

        Ok(CopyState::new(size, tags, metadata)
            .with_system_metadata(system_metadata)
            .with_etag(head.e_tag)
            .with_api_errors(api_errors))
    }

    /// Format a tag set as URL query parameters. The tag keys and values must be URL-encoded
    /// because S3 decodes the `x-amz-tagging` header server-side and the SDK doesn't do this
    /// encoding.
    ///
    /// See https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-request-header-Tagging
    pub fn format_tag_set(tag_set: &[Tag]) -> String {
        tag_set
            .iter()
            .map(|tag| {
                format!(
                    "{}={}",
                    query::fmt_string(tag.key()),
                    query::fmt_string(tag.value())
                )
            })
            .collect::<Vec<_>>()
            .join("&")
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
    async fn get_multipart_upload(
        &self,
        key: &str,
        bucket: &str,
        tagging: Option<String>,
        metadata: Option<HashMap<String, String>>,
        system_metadata: SystemMetadata,
        checksum: UploadChecksum,
    ) -> Result<(String, Vec<ApiError>)> {
        let do_upload = |tagging, metadata, checksum: UploadChecksum| async {
            let system_metadata = system_metadata.clone();
            self.client
                .create_multipart_upload(|b| {
                    let b = match checksum {
                        UploadChecksum::Computed(algorithm) => b.checksum_algorithm(algorithm),
                        // Algorithms without SDK support must be declared here, otherwise S3
                        // rejects the value at `CompleteMultipartUpload` with `InvalidRequest`.
                        // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html
                        UploadChecksum::Precalculated(ctx, _) => {
                            let checksum_type =
                                ctx.multipart_checksum_type().map(ChecksumType::from);
                            b.checksum_algorithm(ChecksumAlgorithm::from(Ctx::Regular(*ctx)))
                                .set_checksum_type(checksum_type)
                        }
                        UploadChecksum::None => b,
                    };

                    b.set_tagging(tagging)
                        .set_metadata(metadata)
                        .set_content_type(system_metadata.content_type)
                        .set_cache_control(system_metadata.cache_control)
                        .set_content_disposition(system_metadata.content_disposition)
                        .set_content_encoding(system_metadata.content_encoding)
                        .set_content_language(system_metadata.content_language)
                        .bucket(bucket)
                        .key(key)
                })
                .await
        };

        let result = do_upload(tagging.clone(), metadata.clone(), checksum.clone()).await;

        // Retry if this is a best effort copy and the error was access denied.
        let (upload, err) = if let Err(ref err) = result {
            let err = ApiError::from(err);
            if self.tag_mode.is_best_effort() && err.is_access_denied() {
                (do_upload(None, metadata, checksum).await?, vec![err])
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

    /// The additional checksum algorithm for the SDK or S3 to compute during an upload or copy.
    fn additional_checksum_algorithm(&self, state: &CopyState) -> Option<ChecksumAlgorithm> {
        // No additional checksums if no_request_checksum is specified.
        if self.client.no_request_checksum() {
            return None;
        }

        state
            .additional_ctx()
            .map(Ctx::into_standard)
            .filter(StandardCtx::is_sdk_computable_ctx)
            .map(|ctx| ChecksumAlgorithm::from(Ctx::Regular(ctx)))
    }

    /// A precalculated additional checksum for algorithms that S3 accepts but the SDK cannot
    /// compute during an upload.
    fn precalculated_sum(state: &CopyState) -> Option<(StandardCtx, String)> {
        let ctx = match state.additional_ctx()? {
            Ctx::Regular(ctx) => ctx,
            Ctx::AWSEtag(_) => return None,
        };
        if ctx.is_sdk_computable_ctx() || !ctx.is_aws_additional_ctx() {
            return None;
        }

        // S3 validates the value against the algorithm, so an incorrect sum fails the request
        // rather than storing a bad checksum.
        let digest = hex::decode(state.additional_sum()?).ok()?;

        Some((ctx, BASE64_STANDARD.encode(digest)))
    }

    /// The additional checksum to attach to an upload.
    fn upload_checksum(&self, state: &CopyState) -> UploadChecksum {
        if let Some(algorithm) = self.additional_checksum_algorithm(state) {
            UploadChecksum::Computed(algorithm)
        } else if self.client.no_precalculated_checksum() {
            // Some endpoints reject uploads that declare checksum algorithms which are not
            // computed by the SDK, so no additional checksum can be applied here.
            UploadChecksum::None
        } else if let Some((ctx, sum)) = Self::precalculated_sum(state) {
            UploadChecksum::Precalculated(Box::new(ctx), sum)
        } else {
            UploadChecksum::None
        }
    }

    /// Copy the object using the `CopyObject` operation.
    pub async fn copy_object(&self, state: &CopyState) -> Result<CopyResult> {
        let size = state.size();

        let (tagging, tagging_set) = self.tagging_directive();
        let (metadata, metadata_set) = self.metadata_directive();

        let source = self.get_source()?;
        let destination = self.get_destination()?;

        // When no algorithm is set S3 copies the checksum algorithm from the source object.
        // This asks the server to recompute rather than sending a value, so the orphaned header
        // rule does not apply, but `no_request_checksum` suppresses it anyway to keep a single
        // meaning for the option: copyrite never asks for checksums.
        let additional_checksum = self.additional_checksum_algorithm(state);
        let do_copy = |tagging, tagging_set, metadata, metadata_set, additional_checksum| async {
            let etag = state.etag();
            self.client
                .copy_object(move |b| {
                    b.tagging_directive(tagging)
                        .set_tagging(tagging_set)
                        .metadata_directive(metadata)
                        .set_metadata(metadata_set)
                        .set_checksum_algorithm(additional_checksum)
                        .copy_source(Self::copy_source(&source.key, &source.bucket))
                        .set_copy_source_if_match(etag)
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

    /// Get the copy source. The value must be URL-encoded because S3 decodes the
    /// `x-amz-copy-source` header server-side and the SDK doesn't do this encoding.
    ///
    /// See https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html#AmazonS3-CopyObject-request-header-CopySource
    pub fn copy_source(key: &str, bucket: &str) -> String {
        format!(
            "{}/{}",
            label::fmt_string(bucket, EncodingStrategy::Greedy),
            label::fmt_string(key, EncodingStrategy::Greedy)
        )
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

        // Create the upload id if it doesn't exist or use the existing one.
        let (upload_id, api_errors) = if let Some(upload_id) = &multi_part.upload_id {
            (upload_id.to_string(), vec![])
        } else {
            self.get_multipart_upload(
                &destination.key,
                &destination.bucket,
                tagging,
                state.metadata(),
                state.system_metadata(),
                self.upload_checksum(state),
            )
            .await?
        };

        if let Some(part_number) = multi_part.part_number {
            let part_number_i32 = i32::try_from(part_number)?;
            let range = multi_part
                .format_range()
                .ok_or_else(|| Error::aws_error("invalid range".to_string()))?;
            let etag = state.etag();
            let response = self
                .client
                .upload_part_copy(|b| {
                    b.upload_id(&upload_id)
                        .part_number(part_number_i32)
                        .key(&destination.key)
                        .bucket(&destination.bucket)
                        .copy_source(Self::copy_source(&source.key, &source.bucket))
                        .set_copy_source_if_match(etag)
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

    /// Send the ranged `GetObject` request for the source.
    /// Whether the SDK should validate the response checksum for a read of this range.
    ///
    /// The SDK compares a full-object `x-amz-checksum-*` response header against whatever body
    /// it receives, with no special casing for a 206. It skips composite values by their `-N`
    /// suffix, but not full-object ones, so validating a partial body either fails or passes
    /// vacuously depending on what the endpoint returns. Only whole-object reads are meaningful.
    /// Copyrite verifies ranged reads end-to-end itself regardless.
    fn validate_response_checksum(range: Option<&str>) -> bool {
        range.is_none()
    }

    async fn send_get_object(
        &self,
        multi_part: Option<MultiPartOptions>,
        etag: Option<String>,
    ) -> Result<GetObjectOutput> {
        let source = self.get_source()?;
        let range = multi_part
            .as_ref()
            .and_then(|multi_part| multi_part.format_range());

        let validate_response = Self::validate_response_checksum(range.as_deref());

        Ok(self
            .client
            .get_object_validating(validate_response, |b| {
                b.bucket(&source.bucket)
                    .key(&source.key)
                    .set_range(range)
                    .set_if_match(etag)
            })
            .await?)
    }

    /// Stream the chunks of a reader that is only opened when the stream is first polled.
    fn lazy_reader_stream<F, Fut, R>(
        open: F,
    ) -> impl Stream<Item = result::Result<Bytes, io::Error>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<R>> + Send + 'static,
        R: AsyncRead + Send + Unpin + 'static,
    {
        let (tx, mut rx) =
            mpsc::channel::<result::Result<Bytes, io::Error>>(REOPEN_CHANNEL_CAPACITY);

        let mut pending = Some((tx, open));
        poll_fn(move |cx| {
            if let Some((tx, open)) = pending.take() {
                tokio::spawn(async move {
                    match open().await {
                        Ok(reader) => {
                            let mut stream =
                                ReaderStream::with_capacity(reader, READER_STREAM_CAPACITY);
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
            }

            rx.poll_recv(cx)
        })
    }

    /// Wrap the source object in a reader that only sends the `GetObject` request when it is
    /// first polled.
    fn lazy_object_reader(
        &self,
        multi_part: Option<MultiPartOptions>,
        etag: Option<String>,
    ) -> Box<dyn AsyncRead + Sync + Send + Unpin> {
        let this = self.clone();
        Box::new(StreamReader::new(Self::lazy_reader_stream(
            move || async move {
                Ok(this
                    .send_get_object(multi_part, etag)
                    .await?
                    .body
                    .into_async_read())
            },
        )))
    }

    /// Get the object from S3. The request is not sent until the content is first read.
    pub async fn get_object(
        &self,
        multi_part: Option<MultiPartOptions>,
        etag: Option<String>,
    ) -> Result<CopyContent> {
        if let Some(multipart) = &multi_part
            && multipart.part_number.is_none()
        {
            return Ok(CopyContent::empty());
        }

        let data = self.lazy_object_reader(multi_part.clone(), etag.clone());

        let self_clone = self.clone();
        CopyContent::builder(data)
            .with_reopen(move || self_clone.reopen_get(multi_part.clone(), etag.clone()))
            .build()
    }

    /// Re-derive the object stream from the source.
    fn reopen_get(
        &self,
        multi_part: Option<MultiPartOptions>,
        etag: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<CopyContent>> + Send>> {
        let self_clone = self.clone();
        Box::pin(async move { self_clone.get_object(multi_part, etag).await })
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
        // The SDK creates clones of a retryable body per request but only ever polls one of them,
        // so the reopen call must not be issued until first poll.
        let stream = Self::lazy_reader_stream(move || async move { Ok((*reopen)().await?.data) })
            .map_ok(Frame::data);

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

    /// Buffer a part into memory and compute its checksum value. Declared algorithms without
    /// SDK support have no trailers, so the value must be sent as a header before the body.
    ///
    /// This holds a whole part in memory, so peak usage is bounded by
    /// `concurrency * part_size` while a multipart upload is in flight. With the maximum part
    /// size and the default concurrency that is far more than most machines have, so lower
    /// `--concurrency` or `--part-size` if a copy runs out of memory.
    ///
    /// Only the precalculated path reaches this, i.e. multipart uploads of the algorithms the
    /// SDK cannot compute (MD5, SHA512 and the XXHash variants), see
    /// [`StandardCtx::is_sdk_computable_ctx`]. Everything else streams with a trailer and uses
    /// constant memory. Avoiding the buffer entirely would need a second pass over the part via
    /// the reopen factory to hash before sending, trading a re-read for the memory.
    async fn buffered_part_body(
        mut content: CopyContent,
        ctx: &StandardCtx,
        size_hint: u64,
    ) -> Result<(ByteStream, String)> {
        let mut buf = Vec::with_capacity(usize::try_from(size_hint)?);
        content.data.read_to_end(&mut buf).await?;

        let mut ctx = ctx.reset();
        ctx.update(Arc::from(buf.as_slice()))?;
        let digest = ctx.finalize()?;

        Ok((ByteStream::from(buf), BASE64_STANDARD.encode(digest)))
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
            state,
            i64::try_from(state.size())?,
        )
        .await?;

        CopyResult::new(None, None, state.size(), vec![])
    }

    /// Send a streaming `PutObject` request to the destination. Tags are passed separately from
    /// the state because best-effort tagging retries without them.
    async fn send_put_object(
        &self,
        destination: &BucketKey,
        body: ByteStream,
        tags: Option<String>,
        state: &CopyState,
        content_length: i64,
    ) -> result::Result<PutObjectOutput, SdkError<PutObjectError, HttpResponse>> {
        let bucket = destination.bucket.clone();
        let key = destination.key.clone();
        let metadata = state.metadata();
        let system_metadata = state.system_metadata();
        let checksum = self.upload_checksum(state);
        self.client
            .put_object(move |b| {
                // S3 validates the uploaded data against a precalculated checksum.
                let b = match checksum {
                    UploadChecksum::Computed(algorithm) => b.checksum_algorithm(algorithm),
                    UploadChecksum::Precalculated(ctx, sum) => match *ctx {
                        StandardCtx::MD5(_) => b.checksum_md5(sum),
                        StandardCtx::SHA512(_) => b.checksum_sha512(sum),
                        StandardCtx::XXHash64(_) => b.checksum_xxhash64(sum),
                        StandardCtx::XXHash3(_) => b.checksum_xxhash3(sum),
                        StandardCtx::XXHash128(_) => b.checksum_xxhash128(sum),
                        _ => b,
                    },
                    UploadChecksum::None => b,
                };
                b.set_tagging(tags)
                    .set_metadata(metadata)
                    .set_content_type(system_metadata.content_type)
                    .set_cache_control(system_metadata.cache_control)
                    .set_content_disposition(system_metadata.content_disposition)
                    .set_content_encoding(system_metadata.content_encoding)
                    .set_content_language(system_metadata.content_language)
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
        let content_length = i64::try_from(state.size())?;

        let CopyContent { data, reopen } = content;
        let reopen = Arc::new(reopen);

        let result = self
            .send_put_object(
                destination,
                Self::retryable_body(Some(data), Arc::clone(&reopen)),
                state.tags(),
                state,
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
            state,
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

        // Create the upload id if it doesn't exist or use the existing one.
        let (upload_id, err) = if let Some(upload_id) = multi_part.upload_id.as_ref() {
            (upload_id.to_string(), vec![])
        } else {
            self.get_multipart_upload(
                &destination.key,
                &destination.bucket,
                state.tags(),
                state.metadata(),
                state.system_metadata(),
                self.upload_checksum(state),
            )
            .await?
        };

        if let Some(part_number) = multi_part.part_number {
            let part_number_i32 = i32::try_from(part_number)?;
            let content_length = i64::try_from(multi_part.bytes_transferred())?;
            // Only SDK-computable algorithms have trailers.
            let additional_checksum = self.additional_checksum_algorithm(state);

            // A precalculated algorithm is declared on the upload, so every part must have
            // the checksum value.
            let (body, part_sum) = match self.upload_checksum(state) {
                UploadChecksum::Precalculated(ctx, _) => {
                    let (body, sum) =
                        Self::buffered_part_body(content, &ctx, multi_part.bytes_transferred())
                            .await?;
                    (body, Some((ctx, sum)))
                }
                _ => (Self::upload_body(content), None),
            };

            let part = self
                .client
                .upload_part(|b| {
                    let b = match &part_sum {
                        Some((ctx, sum)) => match ctx.as_ref() {
                            StandardCtx::MD5(_) => b.checksum_md5(sum),
                            StandardCtx::SHA512(_) => b.checksum_sha512(sum),
                            StandardCtx::XXHash64(_) => b.checksum_xxhash64(sum),
                            StandardCtx::XXHash3(_) => b.checksum_xxhash3(sum),
                            StandardCtx::XXHash128(_) => b.checksum_xxhash128(sum),
                            _ => b,
                        },
                        None => b.set_checksum_algorithm(additional_checksum),
                    };
                    b.upload_id(&upload_id)
                        .content_length(content_length)
                        .part_number(part_number_i32)
                        .key(&destination.key)
                        .bucket(&destination.bucket)
                        .body(body)
                })
                .await?;

            let mut result: CopyResult = (part, part_number, upload_id).into();
            // Keep the computed value for the completion in case the endpoint does not return it.
            if let (Some(part), Some((ctx, sum))) = (result.part.as_mut(), part_sum) {
                let to_insert = match ctx.as_ref() {
                    StandardCtx::MD5(_) => Some(&mut part.md5),
                    StandardCtx::SHA512(_) => Some(&mut part.sha512),
                    StandardCtx::XXHash64(_) => Some(&mut part.xxhash64),
                    StandardCtx::XXHash3(_) => Some(&mut part.xxhash3),
                    StandardCtx::XXHash128(_) => Some(&mut part.xxhash128),
                    _ => None,
                };
                if let Some(to_insert) = to_insert {
                    to_insert.get_or_insert(sum);
                }
            }
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

    async fn download(
        &self,
        multi_part: Option<MultiPartOptions>,
        state: &CopyState,
    ) -> Result<CopyContent> {
        self.get_object(multi_part, state.etag()).await
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
        // S3 objects can be at most 48.8 TiB, which is the maximum number of parts multiplied by
        // the maximum part size, i.e. 10000 * 5 GiB.
        // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
        53687091200000
    }

    async fn abort_multipart(&self, upload_id: &str) -> Result<()> {
        let destination = self.get_destination()?;
        self.client
            .abort_multipart_upload(|b| {
                b.bucket(&destination.bucket)
                    .key(&destination.key)
                    .upload_id(upload_id)
            })
            .await?;

        Ok(())
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
    use crate::checksum::standard::test::EXPECTED_MD5_SUM;
    use crate::io::copy::CopyContent;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::config::SharedAsyncSleep;
    use aws_sdk_s3::config::retry::RetryConfig;
    use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadOutput;
    use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
    use aws_sdk_s3::operation::copy_object::CopyObjectOutput;
    use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadOutput;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
    use aws_sdk_s3::operation::upload_part::UploadPartOutput;
    use aws_sdk_s3::operation::upload_part_copy::UploadPartCopyOutput;
    use aws_smithy_async::rt::sleep::TokioSleep;
    use aws_smithy_mocks::{MockResponseInterceptor, Rule, RuleMode, mock};
    use aws_smithy_types::byte_stream::ByteStream;
    use aws_smithy_types::error::ErrorMetadata;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;

    const BUCKET: &str = "bucket";
    const KEY: &str = "key";
    const BODY: &[u8] = b"test";
    const XXHASH64_SUM: &str = "ef46db3751d8e999"; // pragma: allowlist secret

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

    /// Build an S3 destination from a mock client with precalculated checksums disabled.
    fn s3_destination_no_precalculated(client: Client) -> S3 {
        S3Builder::default()
            .with_client(
                S3Client::new(Arc::new(client), false, false).with_no_precalculated_checksum(true),
            )
            .with_copy_tags(MetadataCopy::Copy)
            .with_destination(BUCKET, KEY)
            .build()
            .unwrap()
    }

    /// Build an S3 destination from a mock client with request checksums disabled.
    fn s3_destination_no_request_checksum(client: Client) -> S3 {
        S3Builder::default()
            .with_client(
                S3Client::new(Arc::new(client), false, false).with_no_request_checksum(true),
            )
            .with_copy_tags(MetadataCopy::Copy)
            .with_destination(BUCKET, KEY)
            .build()
            .unwrap()
    }

    /// Test copy state.
    fn copy_state() -> CopyState {
        CopyState::new(BODY.len() as u64, Some("tag=value".to_string()), None)
    }

    /// Test copy state with an additional checksum context and known sum.
    fn copy_state_with_ctx(ctx: &str, sum: Option<&str>) -> CopyState {
        let mut state = copy_state();
        state.set_additional_ctx(ctx.parse().unwrap());
        state.set_additional_sum(sum.map(|sum| sum.to_string()));
        state
    }

    /// Download the mock source.
    async fn download<F, Fut>(get_object: &Rule, upload: F) -> Result<CopyResult>
    where
        F: FnOnce(CopyContent) -> Fut,
        Fut: Future<Output = Result<CopyResult>>,
    {
        let source = s3_source(retrying_mock_client(&[get_object]));
        let content = source.download(None, &CopyState::default()).await?;
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

    /// Upload the mock source with the state's additional checksum applied.
    async fn test_put_with_state(put_object: &Rule, state: CopyState) -> Result<CopyResult> {
        let get_object = get_object_rule();
        download(&get_object, |content| {
            let destination =
                s3_destination(retrying_mock_client(&[put_object]), MetadataCopy::Copy);
            async move { destination.put_object(content, &state).await }
        })
        .await
    }

    /// Build an S3 source with a specific tag_mode from a mock client.
    fn s3_source_with_tag_mode(client: Client, tag_mode: MetadataCopy) -> S3 {
        S3Builder::default()
            .with_client(S3Client::new(Arc::new(client), false, false))
            .with_copy_tags(tag_mode)
            .with_source(BUCKET, KEY)
            .build()
            .unwrap()
    }

    /// A `head_object` rule that returns a valid output with content_length set.
    fn head_object_rule() -> Rule {
        mock!(Client::head_object)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_output(|| {
                HeadObjectOutput::builder()
                    .content_length(BODY.len() as i64)
                    .build()
            })
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
        let content = source
            .download(Some(options.clone()), &CopyState::default())
            .await
            .unwrap();

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

    #[test]
    fn per_part_checksums_round_trip() -> Result<()> {
        let output = UploadPartOutput::builder()
            .checksum_crc32("crc32")
            .checksum_crc32_c("crc32c")
            .checksum_sha1("sha1")
            .checksum_sha256("sha256")
            .checksum_sha512("sha512")
            .checksum_crc64_nvme("crc64nvme")
            .checksum_xxhash64("xxhash64")
            .checksum_xxhash3("xxhash3")
            .checksum_xxhash128("xxhash128")
            .checksum_md5("md5")
            .e_tag("etag")
            .build();
        let completed: CompletedPart = CopyResult::from((output, 1u64, "id".to_string()))
            .part
            .expect("missing part")
            .try_into()?;

        assert_eq!(completed.checksum_crc32(), Some("crc32"));
        assert_eq!(completed.checksum_crc32_c(), Some("crc32c"));
        assert_eq!(completed.checksum_sha1(), Some("sha1"));
        assert_eq!(completed.checksum_sha256(), Some("sha256"));
        assert_eq!(completed.checksum_sha512(), Some("sha512"));
        assert_eq!(completed.checksum_crc64_nvme(), Some("crc64nvme"));
        assert_eq!(completed.checksum_xxhash64(), Some("xxhash64"));
        assert_eq!(completed.checksum_xxhash3(), Some("xxhash3"));
        assert_eq!(completed.checksum_xxhash128(), Some("xxhash128"));
        assert_eq!(completed.checksum_md5(), Some("md5"));
        assert_eq!(completed.e_tag(), Some("etag"));

        let copy_part = CopyPartResult::builder()
            .checksum_md5("md5")
            .checksum_sha512("sha512")
            .checksum_xxhash128("xxhash128")
            .build();
        let part = CopyResult::from((copy_part, 1u64, "id".to_string()))
            .part
            .expect("missing part");
        assert_eq!(part.md5.as_deref(), Some("md5"));
        assert_eq!(part.sha512.as_deref(), Some("sha512"));
        assert_eq!(part.xxhash128.as_deref(), Some("xxhash128"));

        Ok(())
    }

    #[tokio::test]
    async fn reopen_reproduces_source() {
        let get_object = get_object_rule();
        let source = s3_source(retrying_mock_client(&[&get_object]));

        let content = source.download(None, &CopyState::default()).await.unwrap();
        let mut reopened = (content.reopen)().await.unwrap();

        let mut buf = Vec::new();
        reopened.data.read_to_end(buf.as_mut()).await.unwrap();
        assert_eq!(buf, BODY);
    }

    #[tokio::test]
    async fn download_is_lazy() {
        let get_object = get_object_rule();
        let source = s3_source(retrying_mock_client(&[&get_object]));

        let mut content = source.download(None, &CopyState::default()).await.unwrap();
        assert_eq!(get_object.num_calls(), 0);

        let mut buf = Vec::new();
        content.data.read_to_end(buf.as_mut()).await.unwrap();
        assert_eq!(buf, BODY);
        assert_eq!(get_object.num_calls(), 1);
    }

    #[tokio::test]
    async fn get_object_sets_if_etag() {
        let get_object = mock!(Client::get_object)
            .match_requests(|req| {
                req.bucket() == Some(BUCKET)
                    && req.key() == Some(KEY)
                    && req.if_match() == Some("etag")
            })
            .sequence()
            .output(|| {
                GetObjectOutput::builder()
                    .body(ByteStream::from_static(BODY))
                    .build()
            })
            .repeatedly()
            .build();
        let source = s3_source(retrying_mock_client(&[&get_object]));

        let state = CopyState::default().with_etag(Some("etag".to_string()));
        let mut content = source.download(None, &state).await.unwrap();

        let mut buf = Vec::new();
        content.data.read_to_end(buf.as_mut()).await.unwrap();
        assert_eq!(buf, BODY);
        assert_eq!(get_object.num_calls(), 1);
    }

    #[tokio::test]
    async fn copy_object_sets_if_etag() {
        let copy = mock!(Client::copy_object)
            .match_requests(|req| req.copy_source_if_match() == Some("etag"))
            .sequence()
            .output(|| CopyObjectOutput::builder().build())
            .repeatedly()
            .build();

        let s3 = S3Builder::default()
            .with_client(S3Client::new(
                Arc::new(retrying_mock_client(&[&copy])),
                false,
                false,
            ))
            .with_copy_tags(MetadataCopy::Copy)
            .with_source(BUCKET, KEY)
            .with_destination(BUCKET, KEY)
            .build()
            .unwrap();

        let state = copy_state().with_etag(Some("etag".to_string()));
        s3.copy_object(&state).await.unwrap();
        assert_eq!(copy.num_calls(), 1);
    }

    #[tokio::test]
    async fn put_object_md5_uses_precalculated_sum() {
        let expected = BASE64_STANDARD.encode(hex::decode(EXPECTED_MD5_SUM).unwrap());
        let put_object = mock!(Client::put_object)
            .match_requests(move |req| {
                req.checksum_md5() == Some(expected.as_str()) && req.checksum_algorithm().is_none()
            })
            .sequence()
            .output(|| PutObjectOutput::builder().build())
            .build();

        let state = copy_state_with_ctx("md5", Some(EXPECTED_MD5_SUM));
        test_put_with_state(&put_object, state).await.unwrap();
        assert_eq!(put_object.num_calls(), 1);
    }

    #[tokio::test]
    async fn put_object_xxhash64_uses_precalculated_sum() {
        let expected = BASE64_STANDARD.encode(hex::decode(XXHASH64_SUM).unwrap());
        let put_object = mock!(Client::put_object)
            .match_requests(move |req| {
                req.checksum_xxhash64() == Some(expected.as_str())
                    && req.checksum_algorithm().is_none()
            })
            .sequence()
            .output(|| PutObjectOutput::builder().build())
            .build();

        let state = copy_state_with_ctx("xxhash64", Some(XXHASH64_SUM));
        test_put_with_state(&put_object, state).await.unwrap();
        assert_eq!(put_object.num_calls(), 1);
    }

    #[tokio::test]
    async fn put_object_computable_ctx_sets_algorithm() {
        let put_object = mock!(Client::put_object)
            .match_requests(|req| {
                req.checksum_algorithm() == Some(&ChecksumAlgorithm::Crc32)
                    && req.checksum_md5().is_none()
            })
            .sequence()
            .output(|| PutObjectOutput::builder().build())
            .build();

        // A computable checksum uses the SDK trailer even when the sum value is known.
        let state = copy_state_with_ctx("crc32", Some("00000000"));
        test_put_with_state(&put_object, state).await.unwrap();
        assert_eq!(put_object.num_calls(), 1);
    }

    #[tokio::test]
    async fn put_object_skips_non_hex_precalculated_sum() {
        let put_object = mock!(Client::put_object)
            .match_requests(|req| {
                req.checksum_md5().is_none() && req.checksum_algorithm().is_none()
            })
            .sequence()
            .output(|| PutObjectOutput::builder().build())
            .build();

        // The digest cannot be decoded, so no checksum can be applied.
        let state = copy_state_with_ctx("md5", Some("zzzz"));
        test_put_with_state(&put_object, state).await.unwrap();
        assert_eq!(put_object.num_calls(), 1);
    }

    #[tokio::test]
    async fn copy_object_md5_omits_checksum_algorithm() {
        let copy = mock!(Client::copy_object)
            .match_requests(|req| req.checksum_algorithm().is_none())
            .sequence()
            .output(|| CopyObjectOutput::builder().build())
            .repeatedly()
            .build();

        let s3 = S3Builder::default()
            .with_client(S3Client::new(
                Arc::new(retrying_mock_client(&[&copy])),
                false,
                false,
            ))
            .with_copy_tags(MetadataCopy::Copy)
            .with_source(BUCKET, KEY)
            .with_destination(BUCKET, KEY)
            .build()
            .unwrap();

        let state = copy_state_with_ctx("md5", Some(EXPECTED_MD5_SUM));
        s3.copy_object(&state).await.unwrap();
        assert_eq!(copy.num_calls(), 1);
    }

    #[tokio::test]
    async fn multipart_md5_algorithm_completes() {
        let expected_part =
            BASE64_STANDARD.encode(hex::decode("098f6bcd4621d373cade4e832627b4f6").unwrap());
        let create = mock!(Client::create_multipart_upload)
            .match_requests(|req| {
                req.checksum_algorithm() == Some(&ChecksumAlgorithm::Md5)
                    && req.checksum_type() == Some(&ChecksumType::Composite)
            })
            .sequence()
            .output(|| {
                CreateMultipartUploadOutput::builder()
                    .upload_id("upload-id")
                    .build()
            })
            .build();
        let part_sum = expected_part.clone();
        let upload_part = mock!(Client::upload_part)
            .match_requests(move |req| {
                req.checksum_algorithm().is_none() && req.checksum_md5() == Some(part_sum.as_str())
            })
            .sequence()
            .output(|| UploadPartOutput::builder().e_tag("etag").build())
            .build();
        let complete_part_sum = expected_part.clone();
        let complete = mock!(Client::complete_multipart_upload)
            .match_requests(move |req| {
                req.checksum_md5().is_none()
                    && req.checksum_type().is_none()
                    && req.mpu_object_size().is_none()
                    && req.multipart_upload().is_some_and(|upload| {
                        upload
                            .parts()
                            .iter()
                            .all(|part| part.checksum_md5() == Some(complete_part_sum.as_str()))
                    })
            })
            .sequence()
            .output(|| CompleteMultipartUploadOutput::builder().build())
            .build();

        let state = copy_state_with_ctx("md5", Some(EXPECTED_MD5_SUM));
        let get_object = get_object_rule();
        let source = s3_source(retrying_mock_client(&[&get_object]));
        let options = MultiPartOptions {
            part_number: Some(1),
            start: 0,
            end: BODY.len() as u64,
            ..Default::default()
        };
        let content = source
            .download(Some(options.clone()), &CopyState::default())
            .await
            .unwrap();

        let destination = s3_destination(
            retrying_mock_client(&[&create, &upload_part, &complete]),
            MetadataCopy::Copy,
        );
        let result = destination
            .put_object_multipart(content, options, &state)
            .await
            .unwrap();

        let part = result.part.unwrap();
        assert_eq!(part.md5, Some(expected_part));

        let completion = MultiPartOptions {
            part_number: None,
            upload_id: Some("upload-id".to_string()),
            parts: Some(vec![part]),
            ..Default::default()
        };
        destination
            .put_object_multipart(CopyContent::empty(), completion, &state)
            .await
            .unwrap();

        assert_eq!(create.num_calls(), 1);
        assert_eq!(upload_part.num_calls(), 1);
        assert_eq!(complete.num_calls(), 1);
    }

    #[tokio::test]
    async fn multipart_copy_md5_without_sum_omits_algorithm() {
        let create = mock!(Client::create_multipart_upload)
            .match_requests(|req| {
                req.checksum_algorithm().is_none() && req.checksum_type().is_none()
            })
            .sequence()
            .output(|| {
                CreateMultipartUploadOutput::builder()
                    .upload_id("upload-id")
                    .build()
            })
            .build();
        let upload_part_copy = mock!(Client::upload_part_copy)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .sequence()
            .output(|| {
                UploadPartCopyOutput::builder()
                    .copy_part_result(CopyPartResult::builder().e_tag("etag").build())
                    .build()
            })
            .build();

        let s3 = S3Builder::default()
            .with_client(S3Client::new(
                Arc::new(retrying_mock_client(&[&create, &upload_part_copy])),
                false,
                false,
            ))
            .with_copy_tags(MetadataCopy::Copy)
            .with_source(BUCKET, KEY)
            .with_destination(BUCKET, KEY)
            .build()
            .unwrap();

        // Without a known sum no additional checksum can be applied, so the create request
        // must not declare an algorithm for a checksum the SDK cannot compute.
        let state = copy_state_with_ctx("md5", None);
        let options = MultiPartOptions {
            part_number: Some(1),
            start: 0,
            end: BODY.len() as u64,
            ..Default::default()
        };
        s3.copy_object_multipart(options, &state).await.unwrap();

        assert_eq!(create.num_calls(), 1);
        assert_eq!(upload_part_copy.num_calls(), 1);
    }

    #[tokio::test]
    async fn multipart_ctx_completes_without_precalculated_sum() {
        let complete = mock!(Client::complete_multipart_upload)
            .match_requests(|req| req.checksum_sha256().is_none() && req.checksum_type().is_none())
            .sequence()
            .output(|| CompleteMultipartUploadOutput::builder().build())
            .build();

        let destination = s3_destination(retrying_mock_client(&[&complete]), MetadataCopy::Copy);
        let state = copy_state_with_ctx("sha256", None);
        let completion = MultiPartOptions {
            part_number: None,
            upload_id: Some("upload-id".to_string()),
            parts: Some(vec![Part {
                part_number: 1,
                e_tag: Some("etag".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        destination
            .put_object_multipart(CopyContent::empty(), completion, &state)
            .await
            .unwrap();

        assert_eq!(complete.num_calls(), 1);
    }

    #[test]
    fn only_whole_object_reads_validate_response_checksums() {
        // The mock harness does not run the response checksum interceptor, so assert the
        // decision itself rather than trying to observe the config override through a request.
        assert!(S3::validate_response_checksum(None));
        assert!(!S3::validate_response_checksum(Some("bytes=0-3")));
        assert!(!S3::validate_response_checksum(Some("bytes=0-8388607")));
    }

    #[tokio::test]
    async fn ranged_download_reads_partial_body() {
        // A ranged read returns a partial body alongside a full-object checksum header, which is
        // the shape that SDK validation cannot handle.
        let get_object = mock!(Client::get_object)
            .match_requests(|req| req.range() == Some("bytes=0-3"))
            .sequence()
            .output(|| {
                GetObjectOutput::builder()
                    .body(ByteStream::from_static(&BODY[..4]))
                    .checksum_crc32("AAAAAA==")
                    .build()
            })
            .build();

        let source = s3_source(retrying_mock_client(&[&get_object]));
        let options = MultiPartOptions {
            part_number: Some(1),
            start: 0,
            end: 4,
            ..Default::default()
        };
        let mut content = source
            .download(Some(options), &CopyState::default())
            .await
            .unwrap();

        let mut buf = Vec::new();
        content.data.read_to_end(buf.as_mut()).await.unwrap();
        assert_eq!(buf, &BODY[..4]);
        assert_eq!(get_object.num_calls(), 1);
    }

    #[tokio::test]
    async fn put_object_no_request_checksum() {
        let put_object = mock!(Client::put_object)
            .match_requests(|req| req.checksum_algorithm().is_none())
            .sequence()
            .output(|| PutObjectOutput::builder().build())
            .build();

        let get_object = get_object_rule();
        // The default context when no sums file exists, so this is the common upload path.
        let state = copy_state_with_ctx("crc64nvme", None);
        download(&get_object, |content| {
            let destination =
                s3_destination_no_request_checksum(retrying_mock_client(&[&put_object]));
            async move { destination.put_object(content, &state).await }
        })
        .await
        .unwrap();
        assert_eq!(put_object.num_calls(), 1);
    }

    #[tokio::test]
    async fn multipart_no_request_checksum() {
        let create = mock!(Client::create_multipart_upload)
            .match_requests(|req| req.checksum_algorithm().is_none())
            .sequence()
            .output(|| {
                CreateMultipartUploadOutput::builder()
                    .upload_id("upload-id")
                    .build()
            })
            .build();
        let upload_part = mock!(Client::upload_part)
            .match_requests(|req| req.checksum_algorithm().is_none())
            .sequence()
            .output(|| UploadPartOutput::builder().e_tag("etag").build())
            .build();

        let state = copy_state_with_ctx("crc64nvme", None);
        let get_object = get_object_rule();
        let source = s3_source(retrying_mock_client(&[&get_object]));
        let options = MultiPartOptions {
            part_number: Some(1),
            start: 0,
            end: BODY.len() as u64,
            ..Default::default()
        };
        let content = source
            .download(Some(options.clone()), &CopyState::default())
            .await
            .unwrap();

        let destination =
            s3_destination_no_request_checksum(retrying_mock_client(&[&create, &upload_part]));
        destination
            .put_object_multipart(content, options, &state)
            .await
            .unwrap();

        assert_eq!(create.num_calls(), 1);
        assert_eq!(upload_part.num_calls(), 1);
    }

    #[tokio::test]
    async fn copy_object_no_request_checksum() {
        let copy = mock!(Client::copy_object)
            .match_requests(|req| req.checksum_algorithm().is_none())
            .sequence()
            .output(|| CopyObjectOutput::builder().build())
            .build();

        let s3 = S3Builder::default()
            .with_client(
                S3Client::new(Arc::new(retrying_mock_client(&[&copy])), false, false)
                    .with_no_request_checksum(true),
            )
            .with_copy_tags(MetadataCopy::Copy)
            .with_source(BUCKET, KEY)
            .with_destination(BUCKET, KEY)
            .build()
            .unwrap();

        let state = copy_state_with_ctx("crc64nvme", None);
        s3.copy_object(&state).await.unwrap();
        assert_eq!(copy.num_calls(), 1);
    }

    #[tokio::test]
    async fn no_request_checksum_still_sends_precalculated() {
        let put_object = mock!(Client::put_object)
            .match_requests(|req| {
                req.checksum_algorithm().is_none() && req.checksum_md5().is_some()
            })
            .sequence()
            .output(|| PutObjectOutput::builder().build())
            .build();

        let get_object = get_object_rule();
        let state = copy_state_with_ctx("md5", Some(EXPECTED_MD5_SUM));
        download(&get_object, |content| {
            let destination =
                s3_destination_no_request_checksum(retrying_mock_client(&[&put_object]));
            async move { destination.put_object(content, &state).await }
        })
        .await
        .unwrap();
        assert_eq!(put_object.num_calls(), 1);
    }

    #[tokio::test]
    async fn put_object_no_precalculated_checksum_omits_sum() {
        let put_object = mock!(Client::put_object)
            .match_requests(|req| {
                req.checksum_md5().is_none() && req.checksum_algorithm().is_none()
            })
            .sequence()
            .output(|| PutObjectOutput::builder().build())
            .build();

        let get_object = get_object_rule();
        let state = copy_state_with_ctx("md5", Some(EXPECTED_MD5_SUM));
        download(&get_object, |content| {
            let destination = s3_destination_no_precalculated(retrying_mock_client(&[&put_object]));
            async move { destination.put_object(content, &state).await }
        })
        .await
        .unwrap();
        assert_eq!(put_object.num_calls(), 1);
    }

    #[tokio::test]
    async fn multipart_no_precalculated_checksum_omits_declaration() {
        let create = mock!(Client::create_multipart_upload)
            .match_requests(|req| {
                req.checksum_algorithm().is_none() && req.checksum_type().is_none()
            })
            .sequence()
            .output(|| {
                CreateMultipartUploadOutput::builder()
                    .upload_id("upload-id")
                    .build()
            })
            .build();
        let upload_part = mock!(Client::upload_part)
            .match_requests(|req| req.checksum_algorithm().is_none())
            .sequence()
            .output(|| UploadPartOutput::builder().e_tag("etag").build())
            .build();
        let complete = mock!(Client::complete_multipart_upload)
            .match_requests(|req| req.checksum_md5().is_none() && req.checksum_type().is_none())
            .sequence()
            .output(|| CompleteMultipartUploadOutput::builder().build())
            .build();

        let state = copy_state_with_ctx("md5", Some(EXPECTED_MD5_SUM));
        let get_object = get_object_rule();
        let source = s3_source(retrying_mock_client(&[&get_object]));
        let options = MultiPartOptions {
            part_number: Some(1),
            start: 0,
            end: BODY.len() as u64,
            ..Default::default()
        };
        let content = source
            .download(Some(options.clone()), &CopyState::default())
            .await
            .unwrap();

        let destination = s3_destination_no_precalculated(retrying_mock_client(&[
            &create,
            &upload_part,
            &complete,
        ]));
        destination
            .put_object_multipart(content, options, &state)
            .await
            .unwrap();

        let completion = MultiPartOptions {
            part_number: None,
            upload_id: Some("upload-id".to_string()),
            parts: Some(vec![Part {
                part_number: 1,
                e_tag: Some("etag".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        destination
            .put_object_multipart(CopyContent::empty(), completion, &state)
            .await
            .unwrap();

        assert_eq!(create.num_calls(), 1);
        assert_eq!(upload_part.num_calls(), 1);
        assert_eq!(complete.num_calls(), 1);
    }

    /// Preservation: tag_mode = Copy with successful GetObjectTagging returns formatted tags.
    #[tokio::test]
    async fn preservation_copy_mode_with_tags_returns_formatted_tags() {
        let head_object = head_object_rule();
        let get_object_tagging = mock!(Client::get_object_tagging)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_output(|| {
                GetObjectTaggingOutput::builder()
                    .tag_set(Tag::builder().key("env").value("prod").build().unwrap())
                    .tag_set(Tag::builder().key("team").value("data").build().unwrap())
                    .build()
                    .unwrap()
            });

        let client = retrying_mock_client(&[&head_object, &get_object_tagging]);
        let s3 = s3_source_with_tag_mode(client, MetadataCopy::Copy);

        let result = s3
            .initialize_state(KEY.to_string(), BUCKET.to_string())
            .await;

        assert!(
            result.is_ok(),
            "Copy mode with valid tags should succeed, got: {:?}",
            result.err()
        );
        let state = result.unwrap();
        assert_eq!(
            state.tags(),
            Some("env=prod&team=data".to_string()),
            "Copy mode should format tags as key=value pairs joined by &"
        );
        assert_eq!(state.size(), BODY.len() as u64);
    }

    /// Preservation: tag_mode = Copy with GetObjectTagging error propagates the error.
    #[tokio::test]
    async fn preservation_copy_mode_with_tagging_error_propagates_error() {
        let head_object = head_object_rule();
        let get_object_tagging = mock!(Client::get_object_tagging)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_error(|| {
                GetObjectTaggingError::generic(
                    ErrorMetadata::builder()
                        .code("AccessDenied")
                        .message("Access Denied")
                        .build(),
                )
            });

        let client = retrying_mock_client(&[&head_object, &get_object_tagging]);
        let s3 = s3_source_with_tag_mode(client, MetadataCopy::Copy);

        let result = s3
            .initialize_state(KEY.to_string(), BUCKET.to_string())
            .await;

        assert!(
            result.is_err(),
            "Copy mode should propagate tagging errors, but got Ok"
        );
    }

    /// Best effort copies proceed without tags on a tagging error, but the error must still be
    /// recorded so that it appears in the copy stats rather than being swallowed.
    #[tokio::test]
    async fn best_effort_tagging_error_is_recorded() {
        let head_object = head_object_rule();
        let get_object_tagging = mock!(Client::get_object_tagging)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_error(|| {
                GetObjectTaggingError::generic(
                    ErrorMetadata::builder()
                        .code("AccessDenied")
                        .message("Access Denied")
                        .build(),
                )
            });

        let client = retrying_mock_client(&[&head_object, &get_object_tagging]);
        let s3 = s3_source_with_tag_mode(client, MetadataCopy::BestEffort);

        let state = s3
            .initialize_state(KEY.to_string(), BUCKET.to_string())
            .await
            .unwrap();

        assert!(state.tags().is_none());
        assert_eq!(state.api_errors().len(), 1);
        assert!(state.api_errors()[0].is_access_denied());
    }

    /// Tagging support varies across S3-compatible endpoints, so a best effort copy proceeds on
    /// any tagging error, not only access denied. The error is still recorded.
    #[tokio::test]
    async fn best_effort_tagging_records_non_access_denied_errors() {
        let head_object = head_object_rule();
        let get_object_tagging = mock!(Client::get_object_tagging)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_error(|| {
                GetObjectTaggingError::generic(
                    ErrorMetadata::builder()
                        .code("InvalidArgument")
                        .message("unsupported")
                        .build(),
                )
            });

        let client = retrying_mock_client(&[&head_object, &get_object_tagging]);
        let s3 = s3_source_with_tag_mode(client, MetadataCopy::BestEffort);

        let state = s3
            .initialize_state(KEY.to_string(), BUCKET.to_string())
            .await
            .unwrap();

        assert!(state.tags().is_none());
        assert_eq!(state.api_errors().len(), 1);
        assert!(!state.api_errors()[0].is_access_denied());
    }

    /// Preservation: HeadObject size and metadata extraction is consistent regardless of tag_mode.
    #[tokio::test]
    async fn preservation_head_object_size_consistent_across_tag_modes() {
        // Copy mode - size should be extracted correctly
        let head_object = head_object_rule();
        let get_object_tagging = mock!(Client::get_object_tagging)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_output(|| {
                GetObjectTaggingOutput::builder()
                    .tag_set(Tag::builder().key("k").value("v").build().unwrap())
                    .build()
                    .unwrap()
            });

        let client = retrying_mock_client(&[&head_object, &get_object_tagging]);
        let s3 = s3_source_with_tag_mode(client, MetadataCopy::Copy);

        let result = s3
            .initialize_state(KEY.to_string(), BUCKET.to_string())
            .await
            .unwrap();
        assert_eq!(
            result.size(),
            BODY.len() as u64,
            "Copy mode: size should match content_length"
        );
    }

    /// Preservation: HeadObject metadata is passed through in Copy mode.
    #[tokio::test]
    async fn preservation_head_object_metadata_passed_through() {
        let head_object = mock!(Client::head_object)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_output(|| {
                HeadObjectOutput::builder()
                    .content_length(BODY.len() as i64)
                    .metadata("custom-key", "custom-value")
                    .build()
            });

        let get_object_tagging = mock!(Client::get_object_tagging)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_output(|| {
                GetObjectTaggingOutput::builder()
                    .tag_set(Tag::builder().key("k").value("v").build().unwrap())
                    .build()
                    .unwrap()
            });

        let client = retrying_mock_client(&[&head_object, &get_object_tagging]);
        let s3 = s3_source_with_tag_mode(client, MetadataCopy::Copy);

        let result = s3
            .initialize_state(KEY.to_string(), BUCKET.to_string())
            .await
            .unwrap();

        let metadata = result.metadata().expect("metadata should be present");
        assert_eq!(
            metadata.get("custom-key").map(String::as_str),
            Some("custom-value")
        );
    }

    /// The maximum object size is the maximum number of parts multiplied by the maximum part
    /// size, so it must stay consistent with those limits.
    /// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
    #[test]
    fn max_object_size_matches_the_part_limits() {
        let s3 = s3_destination(retrying_mock_client(&[]), MetadataCopy::Copy);

        assert_eq!(s3.max_object_size(), 53687091200000);
        assert_eq!(s3.max_object_size(), s3.max_parts() * s3.max_part_size());
    }

    #[test]
    fn copy_source_url_encodes_special_characters() {
        assert_eq!(S3::copy_source("key", "bucket"), "bucket/key");
        assert_eq!(
            S3::copy_source("prefix/file with spaces+plus%percent~tilde", "bucket"),
            "bucket/prefix/file%20with%20spaces%2Bplus%25percent~tilde"
        );
        assert_eq!(S3::copy_source("🦀.rs", "bucket"), "bucket/%F0%9F%A6%80.rs");
    }

    #[test]
    fn sdk_encoders_match_uri_encode_reference() {
        let inputs = ('\0'..='\u{7f}')
            .map(String::from)
            .chain(["prefix/file with spaces+plus%percent~tilde".to_string()]);

        for input in inputs {
            let expected = urlencoding::encode(&input).into_owned();
            assert_eq!(query::fmt_string(&input), expected,);
            assert_eq!(
                label::fmt_string(&input, EncodingStrategy::Greedy),
                expected.replace("%2F", "/"),
            );
        }
    }

    #[tokio::test]
    async fn initialize_state_url_encodes_tags() {
        let head_object = head_object_rule();
        let get_object_tagging = mock!(Client::get_object_tagging)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_output(|| {
                GetObjectTaggingOutput::builder()
                    .tag_set(
                        Tag::builder()
                            .key("my key")
                            .value("a+b=c/d@e")
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap()
            });

        let client = retrying_mock_client(&[&head_object, &get_object_tagging]);
        let s3 = s3_source_with_tag_mode(client, MetadataCopy::Copy);

        let state = s3
            .initialize_state(KEY.to_string(), BUCKET.to_string())
            .await
            .unwrap();

        assert_eq!(state.tags(), Some("my%20key=a%2Bb%3Dc%2Fd%40e".to_string()));
    }

    #[tokio::test]
    async fn initialize_state_captures_system_metadata() {
        let head_object = mock!(Client::head_object)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_output(|| {
                HeadObjectOutput::builder()
                    .content_length(BODY.len() as i64)
                    .content_type("application/json")
                    .cache_control("no-cache")
                    .content_disposition("inline")
                    .content_encoding("gzip")
                    .content_language("en-AU")
                    .build()
            });
        let get_object_tagging = mock!(Client::get_object_tagging)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_output(|| {
                GetObjectTaggingOutput::builder()
                    .set_tag_set(Some(vec![]))
                    .build()
                    .unwrap()
            });

        let client = retrying_mock_client(&[&head_object, &get_object_tagging]);
        let s3 = s3_source_with_tag_mode(client, MetadataCopy::Copy);

        let state = s3
            .initialize_state(KEY.to_string(), BUCKET.to_string())
            .await
            .unwrap();

        let system_metadata = state.system_metadata();
        assert_eq!(
            system_metadata.content_type.as_deref(),
            Some("application/json")
        );
        assert_eq!(system_metadata.cache_control.as_deref(), Some("no-cache"));
        assert_eq!(
            system_metadata.content_disposition.as_deref(),
            Some("inline")
        );
        assert_eq!(system_metadata.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(system_metadata.content_language.as_deref(), Some("en-AU"));
    }

    #[tokio::test]
    async fn initialize_state_suppresses_metadata() {
        let head_object = mock!(Client::head_object)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .then_output(|| {
                HeadObjectOutput::builder()
                    .content_length(BODY.len() as i64)
                    .content_type("application/json")
                    .metadata("custom-key", "custom-value")
                    .build()
            });

        let client = retrying_mock_client(&[&head_object]);
        let s3 = S3Builder::default()
            .with_client(S3Client::new(Arc::new(client), false, false))
            .with_copy_metadata(MetadataCopy::Suppress)
            .with_copy_tags(MetadataCopy::Suppress)
            .with_source(BUCKET, KEY)
            .build()
            .unwrap();

        let state = s3
            .initialize_state(KEY.to_string(), BUCKET.to_string())
            .await
            .unwrap();

        assert_eq!(state.tags(), None);
        assert_eq!(state.metadata(), None);
        assert_eq!(state.system_metadata().content_type, None);
    }

    #[tokio::test]
    async fn put_object_has_system_metadata() {
        let get_object = get_object_rule();
        let put_object = mock!(Client::put_object)
            .match_requests(|req| {
                req.content_type() == Some("text/plain")
                    && req.cache_control() == Some("max-age=60")
                    && req.content_disposition() == Some("attachment")
                    && req.content_encoding() == Some("gzip")
                    && req.content_language() == Some("en")
            })
            .sequence()
            .output(|| PutObjectOutput::builder().build())
            .build();

        let state = copy_state().with_system_metadata(SystemMetadata {
            content_type: Some("text/plain".to_string()),
            cache_control: Some("max-age=60".to_string()),
            content_disposition: Some("attachment".to_string()),
            content_encoding: Some("gzip".to_string()),
            content_language: Some("en".to_string()),
        });

        let result = download(&get_object, |content| {
            let destination =
                s3_destination(retrying_mock_client(&[&put_object]), MetadataCopy::Copy);
            async move { destination.put_object(content, &state).await }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(put_object.num_calls(), 1);
    }

    #[tokio::test]
    async fn multipart_create_has_system_metadata() {
        let create = mock!(Client::create_multipart_upload)
            .match_requests(|req| {
                req.content_type() == Some("text/plain")
                    && req.cache_control() == Some("max-age=60")
            })
            .sequence()
            .output(|| {
                CreateMultipartUploadOutput::builder()
                    .upload_id("upload-id")
                    .build()
            })
            .build();
        let upload_part = mock!(Client::upload_part)
            .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
            .sequence()
            .output(|| UploadPartOutput::builder().e_tag("etag").build())
            .build();

        let state = copy_state().with_system_metadata(SystemMetadata {
            content_type: Some("text/plain".to_string()),
            cache_control: Some("max-age=60".to_string()),
            ..Default::default()
        });

        let get_object = get_object_rule();
        let source = s3_source(retrying_mock_client(&[&get_object]));
        let options = MultiPartOptions {
            part_number: Some(1),
            start: 0,
            end: BODY.len() as u64,
            ..Default::default()
        };
        let content = source
            .download(Some(options.clone()), &CopyState::default())
            .await
            .unwrap();

        let destination = s3_destination(
            retrying_mock_client(&[&create, &upload_part]),
            MetadataCopy::Copy,
        );
        destination
            .put_object_multipart(content, options, &state)
            .await
            .unwrap();

        assert_eq!(create.num_calls(), 1);
        assert_eq!(upload_part.num_calls(), 1);
    }

    #[tokio::test]
    async fn abort_multipart_sends_abort() {
        let abort = mock!(Client::abort_multipart_upload)
            .match_requests(|req| {
                req.bucket() == Some(BUCKET)
                    && req.key() == Some(KEY)
                    && req.upload_id() == Some("upload-id")
            })
            .sequence()
            .output(|| AbortMultipartUploadOutput::builder().build())
            .build();

        let destination = s3_destination(retrying_mock_client(&[&abort]), MetadataCopy::Copy);
        destination.abort_multipart("upload-id").await.unwrap();

        assert_eq!(abort.num_calls(), 1);
    }
}

// These proptest-based tests verify that Copy mode correctly formats arbitrary
// tag sets and that behavior is consistent across tag_mode values.
#[cfg(test)]
mod preservation_property_tests {
    use super::*;
    use crate::cli::MetadataCopy;
    use crate::io::S3Client;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::config::SharedAsyncSleep;
    use aws_sdk_s3::config::retry::RetryConfig;
    use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
    use aws_sdk_s3::operation::head_object::HeadObjectOutput;
    use aws_sdk_s3::types::Tag;
    use aws_smithy_async::rt::sleep::TokioSleep;
    use aws_smithy_mocks::{MockResponseInterceptor, Rule, RuleMode, mock};
    use proptest::prelude::*;
    use std::sync::Arc;

    const BUCKET: &str = "bucket";
    const KEY: &str = "key";

    /// Build a mock client with given rules.
    fn mock_client(rules: &[&Rule]) -> Client {
        let mut interceptor = MockResponseInterceptor::new().rule_mode(RuleMode::MatchAny);
        for rule in rules {
            interceptor = interceptor.with_rule(rule);
        }
        Client::from_conf(
            aws_sdk_s3::config::Config::builder()
                .with_test_defaults_v2()
                .http_client(aws_smithy_mocks::create_mock_http_client())
                .sleep_impl(SharedAsyncSleep::new(TokioSleep::new()))
                .retry_config(RetryConfig::standard().with_max_attempts(1))
                .interceptor(interceptor)
                .build(),
        )
    }

    /// Build an S3 source with a specific tag_mode.
    fn s3_with_tag_mode(client: Client, tag_mode: MetadataCopy) -> S3 {
        S3Builder::default()
            .with_client(S3Client::new(Arc::new(client), false, false))
            .with_copy_tags(tag_mode)
            .with_source(BUCKET, KEY)
            .build()
            .unwrap()
    }

    /// Format tags the same way initialize_state does for comparison.
    fn format_tags(tags: &[(String, String)]) -> String {
        tags.iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&")
    }

    /// Strategy for generating valid tag key/value strings (non-empty, no special chars that
    /// would interfere with formatting).
    fn tag_string_strategy() -> impl Strategy<Value = String> {
        "[a-zA-Z0-9_-]{1,20}"
    }

    /// Strategy for generating a tag set of 1..10 tags with random keys and values.
    fn tag_set_strategy() -> impl Strategy<Value = Vec<(String, String)>> {
        prop::collection::vec((tag_string_strategy(), tag_string_strategy()), 1..=10)
    }

    // Property: Copy mode formats any valid tag set as key1=value1&key2=value2&...
    // This runs on unfixed code to capture the baseline formatting behavior.
    proptest! {
        #[test]
        fn copy_mode_formats_tags_as_query_string(tags in tag_set_strategy()) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let expected = format_tags(&tags);

                let head_object = mock!(Client::head_object)
                    .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
                    .then_output(|| {
                        HeadObjectOutput::builder()
                            .content_length(100i64)
                            .build()
                    });

                let tags_clone = tags.clone();
                let get_object_tagging = mock!(Client::get_object_tagging)
                    .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
                    .then_output(move || {
                        let mut builder = GetObjectTaggingOutput::builder();
                        for (k, v) in &tags_clone {
                            builder = builder.tag_set(
                                Tag::builder()
                                    .key(k.as_str())
                                    .value(v.as_str())
                                    .build()
                                    .unwrap(),
                            );
                        }
                        builder.build().unwrap()
                    });

                let client = mock_client(&[&head_object, &get_object_tagging]);
                let s3 = s3_with_tag_mode(client, MetadataCopy::Copy);

                let result = s3
                    .initialize_state(KEY.to_string(), BUCKET.to_string())
                    .await;

                prop_assert!(result.is_ok(), "Copy mode should succeed with valid tags");
                let state = result.unwrap();
                prop_assert_eq!(
                    state.tags(),
                    Some(expected),
                    "Copy mode should format tags as key=value pairs joined by &"
                );
                prop_assert_eq!(state.size(), 100u64);
                Ok(())
            })?;
        }
    }

    // Property: HeadObject size extraction is consistent regardless of tag_mode.
    // For Copy mode with valid tags, the size should always match content_length.
    proptest! {
        #[test]
        fn head_object_size_consistent_with_copy_mode(size in 1u64..=10_000_000u64) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let size_i64 = size as i64;

                let head_object = mock!(Client::head_object)
                    .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
                    .then_output(move || {
                        HeadObjectOutput::builder()
                            .content_length(size_i64)
                            .build()
                    });

                let get_object_tagging = mock!(Client::get_object_tagging)
                    .match_requests(|req| req.bucket() == Some(BUCKET) && req.key() == Some(KEY))
                    .then_output(|| {
                        GetObjectTaggingOutput::builder()
                            .tag_set(
                                Tag::builder().key("k").value("v").build().unwrap(),
                            )
                            .build()
                            .unwrap()
                    });

                let client = mock_client(&[&head_object, &get_object_tagging]);
                let s3 = s3_with_tag_mode(client, MetadataCopy::Copy);

                let result = s3
                    .initialize_state(KEY.to_string(), BUCKET.to_string())
                    .await;

                prop_assert!(result.is_ok(), "Should succeed with valid head object");
                let state = result.unwrap();
                prop_assert_eq!(state.size(), size, "Size should match content_length");
                Ok(())
            })?;
        }
    }
}
