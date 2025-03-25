//! The copy command task implementation.
//!

use crate::error::Result;

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
}

impl CopyTask {
    /// Runs the copy task and return the output.
    pub async fn run(self) -> Result<CopyInfo> {
        todo!()
    }
}
