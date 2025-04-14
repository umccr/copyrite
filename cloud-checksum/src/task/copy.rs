//! The copy command task implementation.
//!

use crate::checksum::aws_etag::PART_SIZE_ORDERING;
use crate::checksum::file::SumsFile;
use crate::checksum::Ctx;
use crate::error::Error::CopyError;
use crate::error::{Error, Result};
use crate::io::copy::{CopyResult, CopyState, MultiPartOptions, ObjectCopy, ObjectCopyBuilder};
use crate::io::sums::ObjectSumsBuilder;
use crate::io::Provider;
use crate::{CopyMode, MetadataCopy};
use aws_sdk_s3::Client;
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use std::future::Future;
use std::sync::Arc;

pub const DEFAULT_MULTIPART_THRESHOLD: u64 = 20 * 1024 * 1024; // 20mib

/// Build a copy task.
#[derive(Default, Clone)]
pub struct CopyTaskBuilder {
    source: String,
    destination: String,
    multipart_threshold: Option<u64>,
    part_size: Option<u64>,
    metadata_mode: MetadataCopy,
    copy_mode: CopyMode,
    client: Option<Arc<Client>>,
    concurrency: Option<usize>,
}

/// Settings that determine the part size and additional checksums to use.
#[derive(Debug)]
pub struct CopySettings {
    part_size: Option<u64>,
    ctx: Ctx,
    object_size: u64,
}

#[derive(Debug)]
struct ObjectInfo {
    size: u64,
    max_parts: u64,
    max_part_size: u64,
    min_part_size: u64,
}

impl CopySettings {
    /// Create new settings.
    pub fn new(part_size: Option<u64>, ctx: Ctx, object_size: u64) -> Self {
        Self {
            part_size,
            ctx,
            object_size,
        }
    }

    /// Get the inner values.
    pub fn into_inner(self) -> (Option<u64>, Ctx, u64) {
        (self.part_size, self.ctx, self.object_size)
    }
}

impl CopyTaskBuilder {
    /// Set the source
    pub fn with_source(mut self, source: String) -> Self {
        self.source = source;
        self
    }

    /// Set the destination.
    pub fn with_destination(mut self, destination: String) -> Self {
        self.destination = destination;
        self
    }

    /// Set the metadata mode.
    pub fn with_metadata_mode(mut self, metadata_mode: MetadataCopy) -> Self {
        self.metadata_mode = metadata_mode;
        self
    }

    /// Set the copy mode.
    pub fn with_copy_mode(mut self, copy_mode: CopyMode) -> Self {
        self.copy_mode = copy_mode;
        self
    }

    /// Set the multipart threshold.
    pub fn with_multipart_threshold(mut self, multipart_threshold: Option<u64>) -> Self {
        self.multipart_threshold = multipart_threshold;
        self
    }

    /// Set the part size.
    pub fn with_part_size(mut self, part_size: Option<u64>) -> Self {
        self.part_size = part_size;
        self
    }

    /// Set the S3 client to use for S3 copies.
    pub fn with_client(mut self, client: Arc<Client>) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the S3 client to use for S3 copies.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    /// Return whether multipart is available.
    fn is_multipart(
        object_size: u64,
        part_size: u64,
        max_parts: u64,
        max_part_size: u64,
        min_part_size: u64,
    ) -> bool {
        if part_size > max_part_size || part_size < min_part_size {
            return false;
        }

        object_size.div_ceil(part_size) < max_parts
    }

    /// Return whether single part is available.
    fn is_single_part(object_size: u64, single_part_limit: u64) -> bool {
        object_size < single_part_limit
    }

    /// Determine the settings from an existing sums file.
    fn use_settings_from_sums(
        &self,
        sums: &SumsFile,
        info: ObjectInfo,
        destination: Provider,
    ) -> Result<CopySettings> {
        // First, check if the original was a multipart upload and if a valid and preferred
        // multipart checksum exists, using this if it is the case.
        let ctx = sums
            .checksums
            .keys()
            .find_map(|ctx| {
                ctx.is_preferred_multipart(&destination)
                    .map(|part_size| (part_size, ctx.clone()))
            })
            .take_if(|(part_size, _)| {
                Self::is_multipart(
                    info.size,
                    *part_size,
                    info.max_parts,
                    info.max_part_size,
                    info.min_part_size,
                )
            });
        if let Some((part_size, ctx)) = ctx {
            return Ok(CopySettings::new(Some(part_size), ctx, info.size));
        }

        // Otherwise, check if a preferred single part checksum exists.
        let ctx = sums
            .checksums
            .keys()
            .find(|ctx| ctx.is_preferred_single_part(&destination))
            .take_if(|_| Self::is_single_part(info.size, info.max_part_size));
        if let Some(ctx) = ctx {
            return Ok(CopySettings::new(None, ctx.clone(), info.size));
        }

        // If none of the above apply, then extract the best additional checksum to use.
        Ok(CopySettings::new(
            None,
            sums.checksums.keys().next().cloned().unwrap_or_default(),
            info.size,
        ))
    }

    /// Determine the settings to use for multipart or single part uploads, and any additional
    /// checksums to set. The goal of this function is to find the best settings to copy with,
    /// so that checks on copied objects perform the least computation and are most likely to
    /// contain common checksums across SDKs and CLIs.
    ///
    /// The order of preference is:
    /// 1. If `part_size` is set for the builder, use multipart copies when the size of the object
    ///    reaches the `multipart_threshold`.
    /// 2. Use an existing `.sums` file to determine single part or multipart copies. If the source
    ///    object contains a multipart checksum, match that and use a multipart copy, otherwise if
    ///    it contains a single part copy, match the single part copy. The `multipart_threshold`
    ///    does not affect this logic.
    /// 3. Use the `PART_SIZE_ORDERING` to find the best multipart copy part size if the size
    ///    reaches the `multipart_threshold` or otherwise use single part copies if possible.
    pub async fn use_settings(
        self,
        destination: Provider,
        destination_copy: &(dyn ObjectCopy + Send),
        state: &CopyState,
    ) -> Result<CopySettings> {
        let size = state
            .size()
            .ok_or_else(|| CopyError("failed to get size".to_string()))?;
        let max_part_size = destination_copy.max_part_size();
        let max_parts = destination_copy.max_parts();
        let min_part_size = destination_copy.min_part_size();

        // Only use the sums file if the size is not set.
        let sums = if self.part_size.is_none() {
            ObjectSumsBuilder::default()
                .set_client(self.client.clone())
                .build(self.source.to_string())
                .await?
                .sums_file()
                .await?
        } else {
            None
        };

        // If there are existing sums, try the best part size.
        let settings = if let Some(sums) = sums {
            let settings = self.use_settings_from_sums(
                &sums,
                ObjectInfo {
                    size,
                    max_parts,
                    max_part_size,
                    min_part_size,
                },
                destination,
            )?;
            if self.part_size.is_none() {
                return Ok(settings);
            } else {
                Some(settings)
            }
        } else {
            None
        };

        // Use the additional sum from the settings if available or the default.
        let additional_ctx = settings
            .map(|settings| settings.into_inner().1)
            .unwrap_or_default();

        let threshold = self
            .multipart_threshold
            .unwrap_or(DEFAULT_MULTIPART_THRESHOLD);

        // If the part size is set, use that.
        if let Some(part_size) = self.part_size {
            if size > threshold {
                return if Self::is_multipart(
                    size,
                    part_size,
                    max_parts,
                    max_part_size,
                    min_part_size,
                ) {
                    Ok(CopySettings::new(Some(part_size), additional_ctx, size))
                } else {
                    Err(CopyError(format!(
                        "invalid part size `{}` and threshold `{}` for the object size `{}`",
                        part_size, threshold, size
                    )))
                };
            }
        }

        let err = || {
            CopyError(format!(
                "failed to find a valid part size for the threshold `{}` with object size `{}`",
                threshold, size
            ))
        };
        // Use multipart if the size reaches the threshold.
        if size > threshold {
            let part_size = PART_SIZE_ORDERING.keys().copied().find(|part_size| {
                Self::is_multipart(size, *part_size, max_parts, max_part_size, min_part_size)
            });

            return if let Some(part_size) = part_size {
                Ok(CopySettings::new(Some(part_size), additional_ctx, size))
            } else {
                Err(err())
            };
        }

        // Otherwise use single part if possible.
        if Self::is_single_part(size, max_part_size) {
            return Ok(CopySettings::new(None, additional_ctx, size));
        }

        // This condition may occur if the size is greater than the possible single part upload
        // limit but lower than the threshold.
        Err(err())
    }

    /// Build a generate task.
    pub async fn build(self) -> Result<CopyTask> {
        if self.source.is_empty() || self.destination.is_empty() {
            return Err(CopyError("source and destination required".to_string()));
        }

        let source = Provider::try_from(self.source.as_str())?;
        let destination = Provider::try_from(self.destination.as_str())?;

        let is_same_provider =
            (source.is_file() && destination.is_file()) || (source.is_s3() && destination.is_s3());
        let copy_mode = if is_same_provider {
            if self.copy_mode.is_download_upload() {
                CopyMode::DownloadUpload
            } else {
                CopyMode::ServerSide
            }
        } else {
            CopyMode::DownloadUpload
        };

        let (source_copy, destination_copy) = if is_same_provider {
            let source = ObjectCopyBuilder::default()
                .with_copy_metadata(self.metadata_mode)
                .set_client(self.client.clone())
                .set_source(Some(source))
                .set_destination(Some(destination.clone()))
                .build()
                .await?;

            (source.clone(), source)
        } else {
            (
                ObjectCopyBuilder::default()
                    .with_copy_metadata(self.metadata_mode)
                    .set_client(self.client.clone())
                    .set_source(Some(source))
                    .build()
                    .await?,
                ObjectCopyBuilder::default()
                    .with_copy_metadata(self.metadata_mode)
                    .set_client(self.client.clone())
                    .set_destination(Some(destination.clone()))
                    .build()
                    .await?,
            )
        };
        let state = source_copy.initialize_state().await?;

        let concurrency = self
            .concurrency
            .ok_or_else(|| CopyError("concurrency not set".to_string()))?;
        let settings = self
            .use_settings(destination.clone(), destination_copy.as_ref(), &state)
            .await?;

        let copy_task = CopyTask {
            additional_sums: settings.ctx,
            part_size: settings.part_size,
            source_copy,
            destination_copy,
            copy_mode,
            object_size: settings.object_size,
            concurrency,
            state,
            ordered_upload: destination.is_file(),
        };

        Ok(copy_task)
    }
}

/// Output of the copy task.
#[derive(Debug, Serialize, Deserialize)]
pub struct CopyInfo {
    total_bytes: Option<u64>,
}

impl CopyInfo {
    /// Convert to a JSON string.
    pub fn to_json_string(&self) -> Result<String> {
        Ok(to_string(&self)?)
    }
}

/// Execute the copy task.
pub struct CopyTask {
    additional_sums: Ctx,
    part_size: Option<u64>,
    source_copy: Box<dyn ObjectCopy + Send + Sync>,
    destination_copy: Box<dyn ObjectCopy + Send + Sync>,
    copy_mode: CopyMode,
    object_size: u64,
    concurrency: usize,
    state: CopyState,
    ordered_upload: bool,
}

impl CopyTask {
    async fn run_multipart<FnC, FutC, FnR, FutR, R>(
        &self,
        part_size: u64,
        download_fn: FnC,
        upload_fn: FnR,
    ) -> Result<Option<u64>>
    where
        FnC: FnOnce(MultiPartOptions, CopyState) -> FutC + Clone + Send + 'static,
        FutC: Future<Output = Result<R>> + Send,
        FnR: FnOnce(R, MultiPartOptions, CopyState) -> FutR + Clone + Send + 'static,
        FutR: Future<Output = Result<CopyResult>> + Send,
        R: Send + 'static,
    {
        let n_parts = self.object_size.div_ceil(part_size);

        let mut start = 0;
        let mut end = part_size;

        let mut parts = Vec::with_capacity(usize::try_from(n_parts)?);
        let push_part = |parts: &mut Vec<_>, part| {
            if let Some(part) = part {
                parts.push(part);
            }
        };

        let mut upload_id = None;
        let resolve_result =
            |upload_id: &mut Option<String>, parts: &mut Vec<_>, result: CopyResult| {
                *upload_id = result.upload_id;
                push_part(parts, result.part);
            };

        // First part must be run without concurrency to set the upload id for subsequent parts.
        for chunk in [[1].as_slice()].into_iter().chain(
            (2..n_parts + 1)
                .collect::<Vec<_>>()
                .chunks(self.concurrency),
        ) {
            let mut copy_tasks = Vec::with_capacity(self.concurrency);

            for part_number in chunk {
                if end > self.object_size {
                    end = self.object_size;
                }

                let options = MultiPartOptions {
                    part_number: Some(*part_number),
                    start,
                    end,
                    upload_id: upload_id.clone(),
                    parts: parts.clone(),
                };

                let state = self.state.clone();

                let copy_fn = download_fn.clone();
                copy_tasks.push(tokio::spawn(async move {
                    (options.clone(), copy_fn(options, state).await)
                }));

                start += part_size;
                end += part_size;
            }

            if self.ordered_upload {
                // If the uploads should be ordered, then wait for each task to finish before uploading.
                for result in join_all(copy_tasks).await {
                    let (options, result) = result?;
                    resolve_result(
                        &mut upload_id,
                        &mut parts,
                        upload_fn.clone()(result?, options, self.state.clone()).await?,
                    );
                }
            } else {
                // Otherwise, concurrently run the upload tasks.
                for result in join_all(copy_tasks).await {
                    let (options, result) = result?;
                    let mut tasks = Vec::with_capacity(self.concurrency);

                    let upload_fn = upload_fn.clone();
                    let state = self.state.clone();
                    tasks.push(tokio::spawn(async move {
                        upload_fn(result?, options, state).await
                    }));

                    join_all(tasks).await.into_iter().try_for_each(|result| {
                        let result = result??;
                        resolve_result(&mut upload_id, &mut parts, result);
                        Ok::<_, Error>(())
                    })?;
                }
            }
        }

        // Complete the upload
        download_fn(
            MultiPartOptions {
                part_number: None,
                start,
                end,
                upload_id: upload_id.clone(),
                parts: parts.clone(),
            },
            self.state.clone(),
        )
        .await?;

        Ok(self.state.size())
    }

    /// Runs the copy task and return the output.
    pub async fn run(mut self) -> Result<CopyInfo> {
        self.state.set_additional_ctx(self.additional_sums.clone());

        let total = match (self.copy_mode, self.part_size) {
            (CopyMode::ServerSide, None) => {
                self.source_copy.copy(None, &self.state).await?;

                self.state.size()
            }
            (CopyMode::ServerSide, Some(part_size)) => {
                let source = self.source_copy.clone();
                self.run_multipart(
                    part_size,
                    |option, state| async move { source.copy(Some(option), &state).await },
                    |result, _, _| async move { Ok(result) },
                )
                .await?
            }
            (CopyMode::DownloadUpload, None) => {
                let data = self.source_copy.download(None).await?;
                self.destination_copy
                    .upload(data, None, &self.state)
                    .await?;

                self.state.size()
            }
            (CopyMode::DownloadUpload, Some(part_size)) => {
                let source = self.source_copy.clone();
                let destination = self.destination_copy.clone();

                self.run_multipart(
                    part_size,
                    |option, _| async move { source.download(Some(option.clone())).await },
                    |data, options, state| async move {
                        destination.upload(data, Some(options), &state).await
                    },
                )
                .await?
            }
        };

        Ok(CopyInfo { total_bytes: total })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::io::sums::aws::test::{
        mock_multi_part_etag_only_rule, mock_single_part_etag_only_rule,
    };
    use crate::test::{TestFileBuilder, TEST_FILE_SIZE};
    use anyhow::Result;
    use aws_sdk_s3::operation::get_object::GetObjectError;
    use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
    use aws_sdk_s3::operation::head_object::HeadObjectOutput;
    use aws_sdk_s3::types::error::NoSuchKey;
    use aws_sdk_s3::Client;
    use aws_smithy_mocks_experimental::{mock, mock_client, Rule, RuleMode};
    use tempfile::tempdir;
    use tokio::fs::File;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_copy() -> Result<()> {
        let tmp = tempdir()?;
        let source = tmp.path().join("source");
        let destination = tmp.path().join("destination");

        let mut file = File::create(&source).await?;
        file.write_all("test".as_bytes()).await?;

        let copy = CopyTaskBuilder::default()
            .with_concurrency(10)
            .with_source(source.to_string_lossy().to_string())
            .with_destination(destination.to_string_lossy().to_string())
            .build()
            .await?
            .run()
            .await?;

        assert_eq!(copy.total_bytes, Some(4));

        let mut file = File::open(destination).await?;
        let mut contents = vec![];
        file.read_to_end(&mut contents).await?;

        assert_eq!(contents, b"test");

        Ok(())
    }

    #[tokio::test]
    async fn copy_settings() -> Result<()> {
        let test_file = TestFileBuilder::default().generate_test_defaults()?;

        let single_part = vec![mock_single_part_etag_only_rule()];
        let multipart = mock_multi_part_etag_only_rule();

        let builder = CopyTaskBuilder::default()
            .with_concurrency(10)
            .with_source("s3://bucket/key".to_string())
            .with_destination("s3://bucket/key2".to_string());

        let lt_threshold = builder
            .clone()
            .with_multipart_threshold(Some(TEST_FILE_SIZE + 1))
            .with_client(Arc::new(mock_size(TEST_FILE_SIZE, single_part.as_slice())));
        assert_eq!(lt_threshold.build().await?.part_size, None);

        // S3 to S3 will always prefer the original upload settings so even if the size is greater than the
        // threshold, it should still be single part.
        let gt_threshold = builder
            .clone()
            .with_client(Arc::new(mock_size(TEST_FILE_SIZE, single_part.as_slice())));
        assert_eq!(gt_threshold.build().await?.part_size, None);

        // If it was originally multipart, it should prefer that even if below the threshold.
        let multipart_lt_threshold = builder
            .clone()
            .with_multipart_threshold(Some(TEST_FILE_SIZE + 1))
            .with_client(Arc::new(mock_size(TEST_FILE_SIZE, multipart.as_slice())));
        assert_eq!(
            multipart_lt_threshold.build().await?.part_size,
            Some(214748365)
        );

        let multipart_gt_threshold = builder
            .clone()
            .with_client(Arc::new(mock_size(TEST_FILE_SIZE, multipart.as_slice())));
        assert_eq!(
            multipart_gt_threshold.build().await?.part_size,
            Some(214748365)
        );

        // If the part size was set, then it should use that.
        let part_size_set = builder
            .clone()
            .with_part_size(Some(5242880))
            .with_client(Arc::new(mock_size(TEST_FILE_SIZE, single_part.as_slice())));
        assert_eq!(part_size_set.build().await?.part_size, Some(5242880));
        let part_size_set_multipart = builder
            .clone()
            .with_part_size(Some(5242880))
            .with_client(Arc::new(mock_size(TEST_FILE_SIZE, multipart.as_slice())));
        assert_eq!(
            part_size_set_multipart.build().await?.part_size,
            Some(5242880)
        );

        // If there are no AWS metadata sums, then use a defaulted value.
        let no_metadata_sums = builder
            .clone()
            .with_source(test_file.to_string_lossy().to_string());
        assert_eq!(no_metadata_sums.build().await?.part_size, Some(8388608));
        let no_metadata_sums_part_size = builder
            .clone()
            .with_part_size(Some(5242880))
            .with_source(test_file.to_string_lossy().to_string());
        assert_eq!(
            no_metadata_sums_part_size.build().await?.part_size,
            Some(5242880)
        );
        let no_metadata_sums_single_part = builder
            .clone()
            .with_multipart_threshold(Some(TEST_FILE_SIZE))
            .with_source(test_file.to_string_lossy().to_string());
        assert_eq!(no_metadata_sums_single_part.build().await?.part_size, None);

        // If the part size exceeds the limits, this should be an error.
        let part_size_err_max = builder
            .clone()
            .with_part_size(Some(60000000000))
            .with_client(Arc::new(mock_size(TEST_FILE_SIZE, single_part.as_slice())));
        assert!(part_size_err_max.build().await.is_err());
        // If the part size exceeds the limits, this should be an error.
        let part_size_err_min = builder
            .clone()
            .with_part_size(Some(1))
            .with_client(Arc::new(mock_size(TEST_FILE_SIZE, single_part.as_slice())));
        assert!(part_size_err_min.build().await.is_err());

        Ok(())
    }

    fn mock_size(size: u64, attributes: &[Rule]) -> Client {
        let get_object = mock_not_found_rule("key.sums".to_string());
        let head_object = mock!(Client::head_object)
            .match_requests(move |req| {
                req.bucket() == Some("bucket")
                    && req.key() == Some("key")
                    && req.part_number().is_none()
            })
            .then_output(move || {
                HeadObjectOutput::builder()
                    .content_length(size as i64)
                    .build()
            });
        let tagging = mock!(Client::get_object_tagging)
            .match_requests(move |req| req.bucket() == Some("bucket") && req.key() == Some("key"))
            .then_output(move || {
                GetObjectTaggingOutput::builder()
                    .set_tag_set(Some(vec![]))
                    .build()
                    .unwrap()
            });

        mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            &[&[head_object, tagging], attributes, &[get_object]].concat()
        )
    }

    pub(crate) fn mock_not_found_rule(key: String) -> Rule {
        mock!(Client::get_object)
            .match_requests(move |req| req.bucket() == Some("bucket") && req.key() == Some(&key))
            .then_error(move || GetObjectError::NoSuchKey(NoSuchKey::builder().build()))
    }
}
