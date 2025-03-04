//! File-based sums file logic.
//!

use crate::checksum::file::{State, SumsFile};
use crate::cloud::ObjectSums;
use crate::error::Error::ParseError;
use crate::error::Result;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncRead, AsyncReadExt};

/// Build a file based sums object.
#[derive(Debug, Default)]
pub struct FileBuilder {
    file: Option<String>,
}

impl FileBuilder {
    /// Set the file location.
    pub fn with_file(mut self, file: String) -> Self {
        self.file = Some(file);
        self
    }

    /// Build using the file name.
    pub fn build(self) -> Result<File> {
        let file = Self::parse_url(
            &self
                .file
                .ok_or_else(|| ParseError("file is required for `FileBuilder`".to_string()))?,
        );
        Ok(File::new(file))
    }

    /// Parse from a string a file name which can optionally be prefixed with `file://`
    pub fn parse_url(s: &str) -> String {
        s.strip_prefix("file://").unwrap_or(s).to_string()
    }
}

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

        let sums =
            SumsFile::read_from_slice(&buf, State::try_from(self.file.to_string()).await?).await?;
        Ok(Some(sums))
    }

    /// Get the reader to the sums file.
    pub async fn sums_reader(&self) -> Result<impl AsyncRead> {
        let path = SumsFile::format_target_file(&self.file);
        Ok(fs::File::open(&path).await?)
    }

    /// Get the size of the target file.
    pub async fn file_size(&self) -> Result<u64> {
        Ok(fs::metadata(SumsFile::format_target_file(&self.file))
            .await?
            .len())
    }

    /// Write the sums file to the configured location.
    pub async fn write_sums(&self, data: String) -> Result<()> {
        let path = SumsFile::format_sums_file(&self.file);
        fs::write(&path, data).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectSums for File {
    async fn sums_file(&mut self) -> Result<Option<SumsFile>> {
        self.get_existing_sums().await
    }

    async fn reader(&mut self) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
        Ok(Box::new(self.sums_reader().await?))
    }

    async fn file_size(&mut self) -> Result<u64> {
        self.file_size().await
    }

    async fn write_data(&self, data: String) -> Result<()> {
        self.write_sums(data).await
    }

    fn cloned(&self) -> Box<dyn ObjectSums + Send> {
        Box::new(self.clone())
    }
}
