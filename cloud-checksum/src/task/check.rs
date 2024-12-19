//! Performs the check task to determine if files are identical from .sums files.
//!

use futures_util::future::join_all;
use crate::checksum::file::SumsFile;
use crate::error::Result;

/// Build a check task.
#[derive(Debug, Default)]
pub struct CheckTaskBuilder {
    files: Vec<String>,
}

impl CheckTaskBuilder {
    /// Set the input files.
    pub fn with_input_files(mut self, files: Vec<String>) -> Self {
        self.files = files;
        self
    }

    /// Build a check task.
    pub async fn build(self) -> Result<CheckTask> {
        let files = join_all(self.files.into_iter().map(|file| async {
            SumsFile::read_from(file).await
        })).await.into_iter().collect::<Result<Vec<_>>>()?;

        Ok(CheckTask {
            files
        })
    }
}

/// Execute the check task.
#[derive(Debug, Default)]
pub struct CheckTask {
    files: Vec<SumsFile>
}

impl CheckTask {
    /// Runs the check task, returning whether the files match.
    pub async fn run(self) -> Result<bool> {
        Ok(self.files.iter().zip(self.files.iter().skip(1)).map(|(a, b)| {
            a.is_same(b)
        }).collect::<Result<Vec<_>>>()?.into_iter().all(|x| x))
    }
}
