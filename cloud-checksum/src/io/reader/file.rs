//! File-based sums file logic.
//!

use crate::checksum::file::SumsFile;
use crate::error::Result;
use crate::io::reader::ObjectRead;
use crate::io::ObjectMeta;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt};

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

    /// Get an existing sums file.
    pub async fn get_existing_sums(&self) -> Result<Option<SumsFile>> {
        let path = SumsFile::format_sums_file(&self.file);

        if !PathBuf::from(&path).exists() {
            return Ok(None);
        }

        let mut file = fs::File::open(&path).await?;
        let mut buf = vec![];
        file.read_to_end(&mut buf).await?;

        let sums = SumsFile::read_from_slice(&buf).await?;
        Ok(Some(sums))
    }

    /// Get the reader to the sums file.
    pub async fn sums_reader(&self) -> Result<impl AsyncRead> {
        let path = SumsFile::format_target_file(&self.file);
        Ok(fs::File::open(&path).await?)
    }

    /// Get the size of the target file.
    pub async fn size(&self) -> Result<Option<u64>> {
        Ok(fs::metadata(SumsFile::format_target_file(&self.file))
            .await
            .ok()
            .map(|metadata| metadata.len()))
    }
}

impl ObjectMeta for File {
    fn location(&self) -> String {
        self.file.to_string()
    }
}

#[async_trait::async_trait]
impl ObjectRead for File {
    async fn sums_file(&mut self) -> Result<Option<SumsFile>> {
        self.get_existing_sums().await
    }

    async fn reader(&mut self) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
        Ok(Box::new(self.sums_reader().await?))
    }

    async fn file_size(&mut self) -> Result<Option<u64>> {
        self.size().await
    }
}
