//! File-based sums file logic.
//!

use crate::checksum::file::SumsFile;
use crate::error::Result;
use crate::io::writer::ObjectWrite;
use crate::io::ObjectMeta;
use tokio::fs;
use tokio::fs::copy;

/// A file object.
#[derive(Debug, Clone)]
pub struct File {
    file: String,
}

impl File {
    /// Create a new file.
    pub fn new(file: String) -> Self {
        Self { file }
    }

    /// Write the sums file to the configured location.
    pub async fn write_sums(&self, sums_file: &SumsFile) -> Result<()> {
        let path = SumsFile::format_sums_file(&self.file);
        fs::write(&path, sums_file.to_json_string()?).await?;
        Ok(())
    }

    /// Copy the file to the destination.
    pub async fn copy(&self, destination: String) -> Result<u64> {
        Ok(copy(&self.file, destination).await?)
    }
}

impl ObjectMeta for File {
    fn location(&self) -> String {
        self.file.to_string()
    }
}

#[async_trait::async_trait]
impl ObjectWrite for File {
    async fn write_sums_file(&self, sums_file: &SumsFile) -> Result<()> {
        self.write_sums(sums_file).await
    }

    async fn copy_object(&self, destination: String) -> Result<u64> {
        self.copy(destination).await
    }
}
