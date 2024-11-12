//! Defines the file format that outputs checksum results
//!

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A file containing multiple checksums.
#[derive(Debug, Serialize, Deserialize)]
pub struct OutputFile {
    name: String,
    size: u64,
    checksums: HashMap<String, OutputChecksum>,
}

/// The output of a checksum.
#[derive(Debug, Serialize, Deserialize)]
pub struct OutputChecksum {
    checksum: String,
    part_checksums: Option<String>,
}

// #[cfg(test)]
// pub(crate) mod test {
//
//     #[tokio::test]
//     async fn serialize_output_file() -> Result<()> {
//     }
// }
