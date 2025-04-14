//! The copy command task implementation.
//!

use crate::error::Error::CopyError;
use crate::error::Result;
use crate::io::copy::{ObjectCopy, ObjectCopyBuilder};
use crate::io::Provider;
use crate::{CopyMode, MetadataCopy};
use serde::{Deserialize, Serialize};
use serde_json::to_string;

/// Build a copy task.
#[derive(Default)]
pub struct CopyTaskBuilder {
    source: String,
    destination: String,
    // multipart_threshold: Option<u64>,
    // part_size: Option<u64>,
    metadata_mode: MetadataCopy,
    tag_mode: MetadataCopy,
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

    /// Set the metadata mode.
    pub fn with_tag_mode(mut self, tag_mode: MetadataCopy) -> Self {
        self.tag_mode = tag_mode;
        self
    }

    /// Set the copy mode.
    pub fn with_copy_mode(mut self, copy_mode: CopyMode) -> Self {
        self.copy_mode = copy_mode;
        self
    }

    // /// Set the multipart threshold.
    // pub fn with_multipart_threshold(mut self, multipart_threshold: u64) -> Self {
    //     self.multipart_threshold = Some(multipart_threshold);
    //     self
    // }
    //
    // /// Set the part size.
    // pub fn with_part_size(mut self, part_size: u64) -> Self {
    //     self.part_size = Some(part_size);
    //     self
    // }

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
            .with_copy_tags(self.tag_mode)
            .build(self.source)
            .await?;
        let destination_copy = ObjectCopyBuilder::default()
            .with_copy_metadata(self.metadata_mode)
            .with_copy_tags(self.tag_mode)
            .build(self.destination)
            .await?;

        let copy_task = CopyTask {
            source,
            destination,
            // multipart_threshold: self.multipart_threshold,
            // part_size: self.part_size,
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
    // multipart_threshold: Option<u64>,
    // part_size: Option<u64>,
    source_copy: Box<dyn ObjectCopy + Send>,
    destination_copy: Box<dyn ObjectCopy + Send>,
    copy_mode: CopyMode,
}

impl CopyTask {
    /// Runs the copy task and return the output.
    pub async fn run(self) -> Result<CopyInfo> {
        let total = match self.copy_mode {
            CopyMode::ServerSide => {
                self.source_copy
                    .copy_object(self.source, self.destination)
                    .await?
            }
            CopyMode::DownloadUpload => {
                let data = self.source_copy.download(self.source).await?;
                self.destination_copy.upload(self.destination, data).await?
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
