//! The copy command task implementation.
//!

use crate::checksum::aws_etag::PART_SIZE_ORDERING;
use crate::checksum::Ctx;
use crate::error::Error::CopyError;
use crate::error::Result;
use crate::io::copy::{MultiPartOptions, ObjectCopy, ObjectCopyBuilder};
use crate::io::sums::ObjectSumsBuilder;
use crate::io::Provider;
use crate::{CopyMode, MetadataCopy};
use serde::{Deserialize, Serialize};
use serde_json::to_string;

pub const DEFAULT_MULTIPART_THRESHOLD: u64 = 20 * 1024 * 1024; // 20mib

/// Build a copy task.
#[derive(Default)]
pub struct CopyTaskBuilder {
    source: String,
    destination: String,
    multipart_threshold: Option<u64>,
    part_size: Option<u64>,
    metadata_mode: MetadataCopy,
    copy_mode: CopyMode,
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

    /// Determine the correct part size to use based on existing sums.
    async fn use_multipart(
        self,
        source: Provider,
        source_copy: &(dyn ObjectCopy + Send),
        destination: Provider,
        destination_copy: &(dyn ObjectCopy + Send),
    ) -> Result<(Option<u64>, Option<Ctx>)> {
        let sums = ObjectSumsBuilder
            .build(self.source.to_string())
            .await?
            .sums_file()
            .await?;

        // If there are existing sums, try and determine the best part size.
        let ctx = if let Some(sums) = sums {
            // First, check if the original was a multipart upload and if a valid and preferred
            // multipart checksum exists.
            let ctx = sums.checksums.keys().find_map(|ctx| {
                ctx.is_preferred_multipart()
                    .map(|part_size| (part_size, ctx.clone()))
            });
            if let Some((part_size, ctx)) = ctx {
                let size = source_copy.size(source.clone()).await?;
                if let Some(size) = size {
                    // Only use multipart if it is allowed at the destination.
                    if self.part_size.is_none()
                        && destination_copy.multipart(size, part_size).await?
                    {
                        return Ok((Some(part_size), Some(ctx)));
                    }
                }
            }

            // Otherwise, check if a preferred single part checksum exists.
            let ctx = sums
                .checksums
                .keys()
                .find(|ctx| ctx.is_preferred_single_part());
            if let Some(ctx) = ctx {
                let size = source_copy.size(source.clone()).await?;
                if let Some(size) = size {
                    // Only use single part uploads if it is possible based on the part size.
                    if destination_copy.single_part(size).await? && self.part_size.is_none() {
                        return Ok((None, Some(ctx.clone())));
                    }
                }
            }

            // If none of the above apply, then extract the best additional checksum to use.
            sums.checksums.keys().next().cloned().unwrap_or_default()
        } else {
            Default::default()
        };

        // Otherwise if the part size is set, use that.
        if let Some(part_size) = self.part_size {
            let size = destination_copy.size(destination.clone()).await?;
            if let Some(size) = size {
                // Only use multipart if it is allowed at the destination.
                return if destination_copy.multipart(size, part_size).await? {
                    Ok((Some(part_size), Some(ctx)))
                } else {
                    Err(CopyError(
                        "invalid part size for the object size".to_string(),
                    ))
                };
            }
        }

        // If it is not set determine the best part size based on the threshold.
        let threshold = self
            .multipart_threshold
            .unwrap_or(DEFAULT_MULTIPART_THRESHOLD);
        let size = source_copy.size(source).await?;

        if let Some(size) = size {
            if size > threshold {
                for possible_part_size in PART_SIZE_ORDERING.keys() {
                    if destination_copy
                        .multipart(size, *possible_part_size)
                        .await?
                    {
                        return Ok((Some(*possible_part_size), Some(ctx)));
                    }
                }

                return Err(CopyError(
                    "failed to find a valid part size for the object size threshold".to_string(),
                ));
            } else if destination_copy.single_part(size).await? {
                return Ok((None, Some(ctx.clone())));
            }
        }

        Err(CopyError(
            "failed to find a valid part size for the object".to_string(),
        ))
    }

    /// Build a generate task.
    pub async fn build(self) -> Result<CopyTask> {
        if self.source.is_empty() || self.destination.is_empty() {
            return Err(CopyError("source and destination required".to_string()));
        }

        let source = Provider::try_from(self.source.as_str())?;
        let destination = Provider::try_from(self.destination.as_str())?;

        let copy_mode = if (source.is_file() && destination.is_file())
            || (source.is_s3() && destination.is_s3())
        {
            if self.copy_mode.is_download_upload() {
                CopyMode::DownloadUpload
            } else {
                CopyMode::ServerSide
            }
        } else {
            CopyMode::DownloadUpload
        };

        let source_copy = ObjectCopyBuilder::default()
            .with_copy_metadata(self.metadata_mode)
            .build(self.source.clone())
            .await?;
        let destination_copy = ObjectCopyBuilder::default()
            .with_copy_metadata(self.metadata_mode)
            .build(self.destination.clone())
            .await?;

        let (part_size, ctx) = self
            .use_multipart(
                source.clone(),
                source_copy.as_ref(),
                destination.clone(),
                destination_copy.as_ref(),
            )
            .await?;

        let copy_task = CopyTask {
            source,
            destination,
            _additional_sums: ctx,
            part_size,
            source_copy,
            destination_copy,
            copy_mode,
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
    source: Provider,
    destination: Provider,
    _additional_sums: Option<Ctx>,
    part_size: Option<u64>,
    source_copy: Box<dyn ObjectCopy + Send>,
    destination_copy: Box<dyn ObjectCopy + Send>,
    copy_mode: CopyMode,
}

impl CopyTask {
    /// Runs the copy task and return the output.
    pub async fn run(mut self) -> Result<CopyInfo> {
        let total = match (self.copy_mode, self.part_size) {
            (CopyMode::ServerSide, None) => {
                self.source_copy
                    .copy(self.source, self.destination, None)
                    .await?
            }
            (CopyMode::ServerSide, Some(part_size)) => {
                let mut size = self
                    .source_copy
                    .size(self.source.clone())
                    .await?
                    .ok_or_else(|| CopyError("failed to get object size".to_string()))?;
                let mut start = 0;
                let mut end = part_size;
                let mut part_number = 1;
                let mut total = 0;
                while size > 0 {
                    total += self
                        .source_copy
                        .copy(
                            self.source.clone(),
                            self.destination.clone(),
                            Some(MultiPartOptions {
                                part_number: Some(part_number),
                                start,
                                end,
                            }),
                        )
                        .await?
                        .unwrap_or_default();

                    part_number += 1;
                    start += part_size;
                    end += part_size;
                    size -= part_size;
                }

                Some(total)
            }
            (CopyMode::DownloadUpload, None) => {
                let data = self.source_copy.download(self.source, None).await?;
                self.destination_copy
                    .upload(self.destination, data, None)
                    .await?
            }
            (CopyMode::DownloadUpload, Some(part_size)) => {
                let mut size = self
                    .source_copy
                    .size(self.source.clone())
                    .await?
                    .ok_or_else(|| CopyError("failed to get object size".to_string()))?;
                let mut start = 0;
                let mut end = part_size;
                let mut part_number = 1;
                let total = 0;
                while size > 0 {
                    let data = self
                        .source_copy
                        .download(
                            self.source.clone(),
                            Some(MultiPartOptions {
                                part_number: Some(part_number),
                                start,
                                end,
                            }),
                        )
                        .await?;
                    self.destination_copy
                        .upload(
                            self.destination.clone(),
                            data,
                            Some(MultiPartOptions {
                                part_number: Some(part_number),
                                start,
                                end,
                            }),
                        )
                        .await?;

                    part_number += 1;
                    start += part_size;
                    end += part_size;
                    size -= part_size;
                }

                Some(total)
            }
        };

        Ok(CopyInfo { total_bytes: total })
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use anyhow::Result;
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
}
