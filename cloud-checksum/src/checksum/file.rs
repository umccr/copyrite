//! Defines the file format that outputs checksum results
//!

use crate::error::Result;
use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;
use std::collections::HashMap;
use tokio::fs;

/// A file containing multiple checksums.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputFile {
    pub(crate) name: String,
    pub(crate) size: u64,
    #[serde(flatten)]
    pub(crate) checksums: HashMap<String, OutputChecksum>,
}

impl OutputFile {
    /// Create an output file.
    pub fn new(name: String, size: u64, checksums: HashMap<String, OutputChecksum>) -> Self {
        Self {
            name,
            size,
            checksums,
        }
    }

    /// Write the output file.
    pub async fn write(&self) -> Result<()> {
        let path = format!("{}.sums", self.name);
        Ok(fs::write(path, to_string_pretty(&self)?).await?)
    }
}

/// The output of a checksum.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutputChecksum {
    pub(crate) checksum: String,
    pub(crate) part_size: Option<u64>,
    pub(crate) part_checksums: Option<Vec<String>>,
}

impl OutputChecksum {
    /// Create an output checksum.
    pub fn new(
        checksum: String,
        part_size: Option<u64>,
        part_checksums: Option<Vec<String>>,
    ) -> Self {
        Self {
            checksum,
            part_size,
            part_checksums,
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::checksum::test::expected_md5_sum;
    use serde_json::{from_value, json, to_value, Value};

    #[test]
    fn serialize_output_file() -> Result<()> {
        let expected_md5 = expected_md5_sum();
        let value = expected_output_file(expected_md5);
        let result = to_value(&value)?;
        let expected = expected_output_json(expected_md5);

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn deserialize_output_file() -> Result<()> {
        let expected_md5 = expected_md5_sum();
        let value = expected_output_json(expected_md5);
        let result: OutputFile = from_value(value)?;
        let expected = expected_output_file(expected_md5);

        assert_eq!(result, expected);

        Ok(())
    }

    fn expected_output_file(expected_md5: &str) -> OutputFile {
        let checksums = vec![(
            "aws-etag".to_string(),
            OutputChecksum::new(
                expected_md5.to_string(),
                Some(1),
                Some(vec![expected_md5.to_string()]),
            ),
        )];
        OutputFile::new(
            "name".to_string(),
            123,
            HashMap::from_iter(checksums.into_iter()),
        )
    }

    fn expected_output_json(expected_md5: &str) -> Value {
        json!({
            "name": "name",
            "size": 123,
            "aws-etag": {
                "checksum": expected_md5,
                "part_size": 1,
                "part_checksums": [expected_md5]
            }
        })
    }
}
