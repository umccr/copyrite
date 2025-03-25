//! IO related to writing data.
//!

pub mod aws;
pub mod file;

use crate::checksum::file::SumsFile;
use crate::error::Result;
use crate::io::ObjectMeta;

/// Write operations on file based or cloud files.
#[async_trait::async_trait]
pub trait ObjectWrite: ObjectMeta {
    /// Write data to the configured location.
    async fn write_sums_file(&self, sums_file: &SumsFile) -> Result<()>;
}
