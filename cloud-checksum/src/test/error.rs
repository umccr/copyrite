use std::{io, result};
use thiserror::Error;

/// The result type for the test_for_filemanager module.
pub type Result<T> = result::Result<T, Error>;

/// Error types for the test_for_filemanager module.
#[derive(Error, Debug)]
pub enum Error {
    #[error("error generating file: {0}")]
    FileGenerate(String),
    #[error("io error: {0}")]
    IoError(#[from] io::Error),
}
