//! Defines the file format that outputs checksum results
//!

use crate::error::Error::OutputFileError;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_string_pretty};
use std::collections::HashMap;
use tokio::fs;

/// The current version of the output file.
pub const OUTPUT_FILE_VERSION: &str = "0.1.0";

/// A file containing multiple checksums.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct OutputFile {
    #[serde(skip)]
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) size: u64,
    // The name of the checksum is always the most canonical form.
    // E.g. no -be prefix for big-endian, and bytes as the unit for
    // AWS checksums.
    #[serde(flatten)]
    pub(crate) checksums: HashMap<String, OutputChecksum>,
}

impl OutputFile {
    /// Create an output file.
    pub fn new(name: String, size: u64, checksums: HashMap<String, OutputChecksum>) -> Self {
        Self {
            name,
            version: OUTPUT_FILE_VERSION.to_string(),
            size,
            checksums,
        }
    }

    fn format_file(name: &str) -> String {
        format!("{}.sums", name)
    }

    /// Convert to a JSON string.
    pub fn to_json_string(&self) -> Result<String> {
        Ok(to_string_pretty(&self)?)
    }

    /// Write the output file.
    pub async fn write(&self) -> Result<()> {
        let path = Self::format_file(&self.name);
        Ok(fs::write(path, self.to_json_string()?).await?)
    }

    /// Read an existing output file.
    pub async fn read_from(name: String) -> Result<Self> {
        let path = Self::format_file(&name);
        let mut value: Self = from_slice(&fs::read(&path).await?)?;
        value.name = name;

        Ok(value)
    }

    /// Merge with another output file, overwriting existing checksums.
    pub fn merge(mut self, other: Self) -> Result<Self> {
        if self.name != other.name && self.size != other.size {
            return Err(OutputFileError(
                "the name and size of output files do not match".to_string(),
            ));
        }

        for (key, checksum) in other.checksums {
            self.checksums.insert(key, checksum);
        }

        Ok(self)
    }
}

/// The output of a checksum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
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
    use crate::checksum::standard::test::expected_md5_sum;
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

    #[test]
    fn merge() -> Result<()> {
        let expected_md5 = expected_md5_sum();
        let mut file_one = expected_output_file(expected_md5);
        file_one.checksums.insert(
            "aws-etag".to_string(),
            OutputChecksum::new(
                expected_md5.to_string(),
                Some(2),
                Some(vec![expected_md5.to_string()]),
            ),
        );

        let mut file_two = expected_output_file(expected_md5);
        file_two.checksums.insert(
            "md5".to_string(),
            OutputChecksum::new(
                expected_md5.to_string(),
                Some(1),
                Some(vec![expected_md5.to_string()]),
            ),
        );

        let result = file_one.clone().merge(file_two)?;
        assert_eq!(result.name, file_one.name);
        assert_eq!(result.size, file_one.size);
        assert_eq!(
            result.checksums,
            HashMap::from_iter(vec![
                (
                    "md5".to_string(),
                    OutputChecksum::new(
                        expected_md5.to_string(),
                        Some(1),
                        Some(vec![expected_md5.to_string()]),
                    ),
                ),
                (
                    "aws-etag".to_string(),
                    OutputChecksum::new(
                        expected_md5.to_string(),
                        Some(1),
                        Some(vec![expected_md5.to_string()]),
                    )
                ),
            ])
        );

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
        OutputFile::new("".to_string(), 123, HashMap::from_iter(checksums))
    }

    fn expected_output_json(expected_md5: &str) -> Value {
        json!({
            "version": OUTPUT_FILE_VERSION,
            "size": 123,
            "aws-etag": {
                "checksum": expected_md5,
                "part-size": 1,
                "part-checksums": [expected_md5]
            }
        })
    }
}
