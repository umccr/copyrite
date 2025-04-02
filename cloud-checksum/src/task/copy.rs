//! The copy command task implementation.
//!

use crate::error::Error::CopyError;
use crate::error::Result;
use crate::io::copy::{ObjectCopy, ObjectCopyBuilder};
use crate::io::Provider;
use serde::{Deserialize, Serialize};
use serde_json::to_string;

/// Build a copy task.
#[derive(Default)]
pub struct CopyTaskBuilder {
    source: String,
    destination: String,
    multipart_threshold: Option<u64>,
    part_size: Option<u64>,
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

    /// Set the multipart threshold.
    pub fn with_multipart_threshold(mut self, multipart_threshold: u64) -> Self {
        self.multipart_threshold = Some(multipart_threshold);
        self
    }

    /// Set the part size.
    pub fn with_part_size(mut self, part_size: u64) -> Self {
        self.part_size = Some(part_size);
        self
    }

    /// Build a generate task.
    pub async fn build(self) -> Result<CopyTask> {
        if self.source.is_empty() || self.destination.is_empty() {
            return Err(CopyError("source and destination required".to_string()));
        }

        let source = Provider::try_from(self.source.as_str())?;
        let destination = Provider::try_from(self.destination.as_str())?;

        let mode = if (source.is_file() && destination.is_file())
            || (source.is_s3() && destination.is_s3())
        {
            Mode::ServerSide
        } else {
            Mode::DownloadUpload
        };

        let source_copy = ObjectCopyBuilder.build(self.source).await?;
        let destination_copy = ObjectCopyBuilder.build(self.destination).await?;

        let copy_task = CopyTask {
            source,
            destination,
            // multipart_threshold: self.multipart_threshold,
            // part_size: self.part_size,
            source_copy,
            destination_copy,
            mode,
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

/// Mode to execute copy task in.
#[derive(Debug)]
pub enum Mode {
    ServerSide,
    DownloadUpload,
}

/// Execute the copy task.
pub struct CopyTask {
    source: Provider,
    destination: Provider,
    // multipart_threshold: Option<u64>,
    // part_size: Option<u64>,
    source_copy: Box<dyn ObjectCopy + Send>,
    destination_copy: Box<dyn ObjectCopy + Send>,
    mode: Mode,
}

impl CopyTask {
    /// Runs the copy task and return the output.
    pub async fn run(self) -> Result<CopyInfo> {
        let total = match self.mode {
            Mode::ServerSide => {
                self.source_copy
                    .copy_object(self.source, self.destination)
                    .await?
            }
            Mode::DownloadUpload => {
                let data = self.source_copy.download(self.source).await?;
                self.destination_copy.upload(self.destination, data).await?
            }
        };

        Ok(CopyInfo { total_bytes: total })
    }
}
