//! The copy command task implementation.
//!

use crate::error::Result;
use crate::io::reader::ObjectRead;
use crate::io::writer::ObjectWrite;

/// Output of the copy task.
pub struct CopyInfo {
    source: String,
    destination: String,
    total_bytes: u64,
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

        Ok(CopyInfo {
            source: self.source,
            destination: self.destination,
            total_bytes: 0,
        })
    }
}
