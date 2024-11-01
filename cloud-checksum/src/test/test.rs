//! Generate test files.
//! 

use std::fs;
use std::path::{PathBuf};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use crate::test::error::Result;

const CONSTANT_SEED: u64 = 42;

/// The default file size.
pub const FILE_SIZE: usize = 10485760;

/// The default file name.
pub const FILE_NAME: &str = "test_file";

/// The default directory name.
pub const DIRECTORY: &str = "data";

/// Generate large test files in an ignored directory.
pub struct TestFileBuilder {
    directory: PathBuf,
    rng: StdRng,
    file_size: usize,
    file_name: String,
    overwrite: bool,
}

impl Default for TestFileBuilder {
    fn default() -> Self {
        Self {
            directory: DIRECTORY.parse().expect("expected valid directory"),
            rng: StdRng::from_entropy(),
            file_size: FILE_SIZE,
            file_name: FILE_NAME.to_string(),
            overwrite: false,
        }
    }
}

impl TestFileBuilder {
    /// Add the random seed to generate the file with.
    pub fn with_random_seed(mut self, seed: u64) -> Self {
        self.rng = StdRng::seed_from_u64(seed);
        self
    }

    /// Use a constant seed with repeatable results to generate the file.
    pub fn with_constant_seed(self) -> Self {
        self.with_random_seed(CONSTANT_SEED)
    }

    /// Set the file size.
    pub fn with_file_size(mut self, file_size: usize) -> Self {
        self.file_size = file_size;
        self
    }

    /// Set the file name.
    pub fn with_file_name(mut self, file_name: String) -> Self {
        self.file_name = file_name;
        self
    }

    /// Set the directory to write the file to.
    pub fn with_directory(mut self, directory: PathBuf) -> Self {
        self.directory = directory;
        self
    }

    /// Always overwrite the file, even if it exists.
    pub fn overwrite(mut self) -> Self {
        self.overwrite = true;
        self
    }

    /// Generate the file.
    pub fn generate(&mut self) -> Result<PathBuf> {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();

        let directory = root.join(&self.directory);
        fs::create_dir_all(&directory)?;

        let file = directory.join(&self.file_name);

        if !file.exists() {
            let mut buf = vec![0; self.file_size];
            self.rng.fill_bytes(&mut buf);

            fs::write(&file, buf)?;
        }

        Ok(file)
    }
}