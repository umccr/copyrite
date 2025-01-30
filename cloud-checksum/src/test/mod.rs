//! Test related functionality used internally within cloud-checksum.
//!

#![doc(hidden)]

pub mod error;

use crate::test::error::Error::FileGenerate;
use crate::test::error::Result;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};

const CONSTANT_SEED: u64 = 42;

/// The default benchmark file size. 10 MB.
pub const BENCH_FILE_SIZE: u64 = 10485760;

/// The default benchmark file name.
pub const BENCH_FILE_NAME: &str = "bench_file";

/// The default test_for_filemanager file size. 1 GB.
pub const TEST_FILE_SIZE: u64 = 1073741824;

/// The default test_for_filemanager file name.
pub const TEST_FILE_NAME: &str = "test_file";

/// The default directory name.
pub const DIRECTORY: &str = "data";

/// Keep some state locked behind a mutex for writing files synchronously when running tests in
/// parallel.
static GENERATED_FILES: LazyLock<Mutex<HashMap<String, PathBuf>>> =
    LazyLock::new(|| Mutex::new(HashMap::default()));

/// Generate large test_for_filemanager files in an ignored directory.
pub struct TestFileBuilder {
    directory: PathBuf,
    rng: StdRng,
    file_size: u64,
    file_name: String,
    overwrite: bool,
    constant_value: Option<u8>,
}

impl Default for TestFileBuilder {
    fn default() -> Self {
        Self {
            directory: DIRECTORY.parse().expect("expected valid directory"),
            rng: StdRng::from_entropy(),
            file_size: TEST_FILE_SIZE,
            file_name: TEST_FILE_NAME.to_string(),
            overwrite: false,
            constant_value: None,
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

    /// Use a constant repeated value to generate the file.
    pub fn with_constant_value(mut self, value: u8) -> Self {
        self.constant_value = Some(value);
        self
    }

    /// Set the file size.
    pub fn with_file_size(mut self, file_size: u64) -> Self {
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

    /// Get the inner random number generator.
    pub fn into_rng(self) -> StdRng {
        self.rng
    }

    /// Generate a file for benchmarking.
    pub fn generate_bench_defaults(self) -> Result<PathBuf> {
        self.with_constant_seed()
            .with_file_size(BENCH_FILE_SIZE)
            .with_file_name(BENCH_FILE_NAME.to_string())
            .generate()
    }

    /// Generate a file for testing.
    pub fn generate_test_defaults(self) -> Result<PathBuf> {
        self.with_constant_seed().generate()
    }

    /// Generate the file.
    pub fn generate(&mut self) -> Result<PathBuf> {
        // The files must be generated synchronously for the whole process because otherwise some
        // tests might see an incomplete file before it is written. Keep global static state is
        // probably the simplest way to accomplish this.
        let mut files = GENERATED_FILES
            .lock()
            .map_err(|err| FileGenerate(err.to_string()))?;

        if files.contains_key(&self.file_name) {
            return Ok(files[&self.file_name].clone());
        }

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();

        let directory = root.join(&self.directory);
        fs::create_dir_all(&directory)?;

        let file = directory.join(&self.file_name);

        if !file.exists() {
            let buf = if let Some(value) = self.constant_value {
                vec![value; self.file_size as usize]
            } else {
                let mut buf = vec![0; self.file_size as usize];
                self.rng.fill_bytes(&mut buf);
                buf
            };

            fs::write(&file, buf)?;
        }

        files.insert(self.file_name.to_string(), file.clone());

        Ok(file)
    }
}
