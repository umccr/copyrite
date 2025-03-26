//! The copy command task implementation.
//!

use crate::error::Result;
use crate::io::reader::ObjectRead;
use crate::io::writer::ObjectWrite;
use crate::io::IoBuilder;
use serde::{Deserialize, Serialize};
use serde_json::to_string;

/// Build a copy task.
#[derive(Default)]
pub struct CopyTaskBuilder {
    source: String,
    destination: String,
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

    /// Build a generate task.
    pub async fn build(self) -> Result<CopyTask> {
        let object_read = IoBuilder.build_read(self.source.to_string()).await?;
        let object_write = IoBuilder.build_write(self.source.to_string()).await?;

        let copy_task = CopyTask {
            source: self.source,
            destination: self.destination,
            object_read,
            object_write,
        };

        Ok(copy_task)
    }
}

/// Output of the copy task.
#[derive(Debug, Serialize, Deserialize)]
pub struct CopyInfo {
    source: String,
    destination: String,
    total_bytes: u64,
}

impl CopyInfo {
    /// Convert to a JSON string.
    pub fn to_json_string(&self) -> Result<String> {
        Ok(to_string(&self)?)
    }
}

/// Execute the copy task.
pub struct CopyTask {
    source: String,
    destination: String,
    object_read: Box<dyn ObjectRead + Send>,
    object_write: Box<dyn ObjectWrite + Send>,
}

impl CopyTask {
    /// Runs the copy task and return the output.
    pub async fn run(mut self) -> Result<CopyInfo> {
        // let existing = self.object_read.sums_file().await?;

        let total = self
            .object_write
            .copy_object(self.destination.clone())
            .await?;

        Ok(CopyInfo {
            source: self.source,
            destination: self.destination,
            total_bytes: total,
        })
    }
}
