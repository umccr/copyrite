//! Builders for file-based operations.
//!

use crate::error::Error::ParseError;
use crate::error::Result;
use crate::io::{reader, writer};

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

    fn get_components(self) -> Result<String> {
        Ok(self.file.ok_or_else(|| {
            ParseError("file is required for `FileBuilder`".to_string())
        })?)
    }

    /// Build using the file name.
    pub fn build_reader(self) -> Result<reader::file::File> {
        Ok(self.get_components()?.into())
    }

    /// Build using the file name.
    pub fn build_writer(self) -> Result<writer::file::File> {
        Ok(self.get_components()?.into())
    }

    /// Parse from a string a file name which can optionally be prefixed with `file://`
    pub fn parse_from_url(mut self, s: &str) -> Self {
        self.file = Some(s.strip_prefix("file://").unwrap_or(s).to_string());
        self
    }
}

impl From<String> for reader::file::File {
    fn from(file: String) -> Self {
        Self::new(file)
    }
}

impl From<String> for writer::file::File {
    fn from(file: String) -> Self {
        Self::new(file)
    }
}
