//! Defines the file format that outputs checksum results
//!

use crate::checksum::Ctx;
use crate::error::Error::SumsFileError;
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_string};
use serde_with::serde_as;
use serde_with::DisplayFromStr;
use std::collections::{BTreeMap, BTreeSet};
use tokio::fs;

/// The current version of the output file.
pub const OUTPUT_FILE_VERSION: &str = "1";

/// The file ending of a sums file.
pub const SUMS_FILE_ENDING: &str = ".sums";

/// A file containing multiple checksums.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[serde(rename_all = "kebab-case")]
pub struct SumsFile {
    // Names are only used internally for writing files, they should not
    // be encoded in the actual sums file format.
    #[serde(skip)]
    pub(crate) names: BTreeSet<String>,
    pub(crate) version: String,
    pub(crate) size: Option<u64>,
    // The name of the checksum is always the most canonical form.
    // E.g. no -be prefix for big-endian, and the number of parts as
    // the suffix for AWS checksums.
    #[serde_as(as = "BTreeMap<DisplayFromStr, _>")]
    #[serde(flatten)]
    pub(crate) checksums: BTreeMap<Ctx, Checksum>,
}

impl Default for SumsFile {
    fn default() -> Self {
        Self::new(BTreeSet::new(), None, BTreeMap::new())
    }
}

impl SumsFile {
    /// Create an output file.
    pub fn new(
        names: BTreeSet<String>,
        size: Option<u64>,
        checksums: BTreeMap<Ctx, Checksum>,
    ) -> Self {
        Self {
            names,
            version: OUTPUT_FILE_VERSION.to_string(),
            size,
            checksums,
        }
    }

    /// Format a sums file with the ending.
    pub fn format_sums_file(name: &str) -> String {
        if name.ends_with(SUMS_FILE_ENDING) {
            name.to_string()
        } else {
            format!("{}{}", name, SUMS_FILE_ENDING)
        }
    }

    /// Format the target file that the sums file is for.
    pub fn format_target_file(name: &str) -> String {
        name.strip_suffix(SUMS_FILE_ENDING)
            .unwrap_or(name)
            .to_string()
    }

    /// Convert to a JSON string.
    pub fn to_json_string(&self) -> Result<String> {
        Ok(to_string(&self)?)
    }

    /// Write the output file.
    pub async fn write(&self) -> Result<()> {
        for name in &self.names {
            let path = Self::format_sums_file(name);
            fs::write(path, self.to_json_string()?).await?
        }

        Ok(())
    }

    /// Read an existing output file.
    pub async fn read_from(name: String) -> Result<Self> {
        let path = Self::format_sums_file(&name);
        let mut value: Self = fs::read(&path).await?.as_slice().try_into()?;
        value.names = BTreeSet::from_iter(vec![name]);

        Ok(value)
    }

    /// Read from a slice and add the name.
    pub async fn read_from_slice(slice: &[u8], name: String) -> Result<Self> {
        let mut value: Self = slice.try_into()?;
        value.names = BTreeSet::from_iter(vec![name]);
        Ok(value)
    }

    /// Merge with another output file, overwriting existing checksums,
    /// taking ownership of self. Returns an error if the name and size
    /// of the file do not match.
    pub fn merge(mut self, other: Self) -> Result<Self> {
        if self.names != other.names && self.size != other.size {
            return Err(SumsFileError(
                "the name and size of output files do not match".to_string(),
            ));
        }

        self.merge_mut(other);
        Ok(self)
    }

    /// Merge with another output file, overwriting existing checksums. Does not
    /// check if the file name and size is the same.
    pub fn merge_mut(&mut self, other: Self) {
        for (key, checksum) in other.checksums {
            self.checksums.insert(key, checksum);
        }
        for name in other.names {
            self.names.insert(name);
        }
    }

    /// Split the sums file into multiple sums files, one for each checksum.
    pub fn split(self) -> Vec<SumsFile> {
        self.checksums
            .iter()
            .map(|(ctx, checksum)| {
                let mut sums_file = Self::default().with_size(self.size);
                sums_file.names = self.names.clone();
                sums_file.add_checksum(ctx.clone(), checksum.clone());

                sums_file
            })
            .collect()
    }

    /// Check if the sums file is the same as another according to all available checksums
    /// in the sums file.
    pub fn is_same(&self, other: &Self) -> bool {
        if self.size != other.size {
            return false;
        }

        for (key, checksum) in &self.checksums {
            if let Some(other_checksum) = other.checksums.get(key) {
                // Two checksums are the same if they have the same top-level checksum and identical
                // part sizes. It is not necessary to use the part checksum values to determine if
                // the sums are the same because the sizes alone determine the top-level checksum.
                if checksum.checksum == other_checksum.checksum
                    && Self::get_part_sizes(checksum) == Self::get_part_sizes(other_checksum)
                {
                    return true;
                }
            }
        }

        false
    }

    /// Get only the part sizes from part checksums
    fn get_part_sizes(checksum: &Checksum) -> Option<Vec<u64>> {
        checksum.part_checksums.as_ref().map(|sums| {
            sums.0
                .iter()
                .filter_map(|part| part.part_size)
                .collect::<Vec<_>>()
        })
    }

    /// Check if the sums file is comparable to another sums file because it contains at least
    /// one of the same checksum type.
    pub fn comparable(&self, other: &Self) -> bool {
        if self.size != other.size {
            return false;
        }

        self.checksums
            .keys()
            .any(|key| other.checksums.contains_key(key))
    }

    /// Get a reference to the names of the sums file.
    pub fn names(&self) -> &BTreeSet<String> {
        &self.names
    }

    /// Get to the names of the sums file.
    pub fn into_names(self) -> BTreeSet<String> {
        self.names
    }

    /// Add a name to the sums file.
    pub fn add_name(&mut self, name: String) {
        self.names.insert(name);
    }

    /// Set the size.
    pub fn with_size(mut self, size: Option<u64>) -> Self {
        self.set_size(size);
        self
    }

    /// Set the size from a mutable reference.
    pub fn set_size(&mut self, size: Option<u64>) {
        self.size = size;
    }

    /// Add a checksum to the sums file.
    pub fn add_checksum(&mut self, ctx: Ctx, checksum: Checksum) {
        self.checksums.insert(ctx, checksum);
    }
}

impl TryFrom<&[u8]> for SumsFile {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self> {
        Ok(from_slice(value)?)
    }
}

/// The output of a checksum.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[serde(rename_all = "kebab-case")]
pub struct Checksum {
    pub(crate) checksum: String,
    pub(crate) part_checksums: Option<PartChecksums>,
}

impl Checksum {
    /// Create an output checksum.
    pub fn new(checksum: String, part_checksums: Option<PartChecksums>) -> Self {
        Self {
            checksum,
            part_checksums,
        }
    }
}

/// A part checksum with the part size.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[serde(rename_all = "kebab-case")]
pub struct PartChecksum {
    pub(crate) part_size: Option<u64>,
    pub(crate) part_checksum: Option<String>,
}

impl PartChecksum {
    /// Create a part checksum.
    pub fn new(part_size: Option<u64>, part_checksum: Option<String>) -> Self {
        Self {
            part_size,
            part_checksum,
        }
    }
}

/// A list of part checksums.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[serde(rename_all = "kebab-case")]
pub struct PartChecksums(Vec<PartChecksum>);

impl PartChecksums {
    /// Create a new part checksums.
    pub fn new(part_checksums: Vec<PartChecksum>) -> Self {
        Self(part_checksums)
    }

    /// Get the inner vector of values.
    pub fn into_inner(self) -> Vec<PartChecksum> {
        self.0
    }

    /// Get a reference to the inner values.
    pub fn get_ref(&self) -> &[PartChecksum] {
        &self.0
    }
}

impl From<Vec<(Option<u64>, Option<String>)>> for PartChecksums {
    fn from(part_sums: Vec<(Option<u64>, Option<String>)>) -> Self {
        Self::new(
            part_sums
                .into_iter()
                .map(|(part_size, part_sum)| PartChecksum::new(part_size, part_sum))
                .collect(),
        )
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::checksum::aws_etag::test::expected_md5_1gib;
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
        let result: SumsFile = from_value(value)?;
        let expected = expected_output_file(expected_md5);

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn is_same() -> Result<()> {
        let expected_md5 = expected_md5_sum();
        let file_one = expected_output_file(expected_md5);
        let mut file_two = file_one.clone();
        file_two.checksums.insert(
            "md5".parse()?,
            Checksum::new(
                expected_md5_1gib().to_string(),
                Some(vec![(Some(1), Some(expected_md5.to_string()))].into()),
            ),
        );
        assert!(file_one.is_same(&file_two));

        let mut file_two = file_one.clone();
        file_two.checksums = BTreeMap::from_iter(vec![(
            "aws-etag".parse()?,
            Checksum::new(
                expected_md5_1gib().to_string(),
                Some(vec![(Some(1), Some(expected_md5.to_string()))].into()),
            ),
        )]);
        assert!(!file_one.is_same(&file_two));

        Ok(())
    }

    #[test]
    fn comparable() -> Result<()> {
        let expected_md5 = expected_md5_sum();
        let file_one = expected_output_file(expected_md5);
        let mut file_two = file_one.clone();
        file_two.checksums.insert(
            "aws-etag".parse()?,
            Checksum::new(
                expected_md5_1gib().to_string(),
                Some(vec![(Some(1), Some(expected_md5.to_string()))].into()),
            ),
        );
        assert!(file_one.comparable(&file_two));

        let mut file_two = file_one.clone();
        file_two.checksums = BTreeMap::from_iter(vec![(
            "md5".parse()?,
            Checksum::new(
                expected_md5_1gib().to_string(),
                Some(vec![(Some(1), Some(expected_md5.to_string()))].into()),
            ),
        )]);
        assert!(!file_one.comparable(&file_two));

        Ok(())
    }

    #[test]
    fn merge() -> Result<()> {
        let expected_md5 = expected_md5_sum();
        let mut file_one = expected_output_file(expected_md5);
        file_one.checksums.insert(
            "aws-etag".parse()?,
            Checksum::new(
                expected_md5.to_string(),
                Some(vec![(Some(2), Some(expected_md5.to_string()))].into()),
            ),
        );

        let mut file_two = expected_output_file(expected_md5);
        file_two.checksums.insert(
            "md5".parse()?,
            Checksum::new(
                expected_md5.to_string(),
                Some(vec![(Some(1), Some(expected_md5.to_string()))].into()),
            ),
        );

        let result = file_one.clone().merge(file_two)?;
        assert_eq!(result.names, file_one.names);
        assert_eq!(result.size, file_one.size);
        assert_eq!(
            result.checksums,
            BTreeMap::from_iter(vec![
                (
                    "md5".parse()?,
                    Checksum::new(
                        expected_md5.to_string(),
                        Some(vec![(Some(1), Some(expected_md5.to_string()))].into()),
                    ),
                ),
                (
                    "aws-etag".parse()?,
                    Checksum::new(
                        expected_md5.to_string(),
                        Some(vec![(Some(1), Some(expected_md5.to_string()))].into()),
                    )
                ),
            ])
        );

        Ok(())
    }

    fn expected_output_file(expected_md5: &str) -> SumsFile {
        let checksums = vec![(
            "aws-etag".parse().unwrap(),
            Checksum::new(
                expected_md5.to_string(),
                Some(vec![(Some(1), Some(expected_md5.to_string()))].into()),
            ),
        )];
        SumsFile::new(BTreeSet::new(), Some(123), BTreeMap::from_iter(checksums))
    }

    fn expected_output_json(expected_md5: &str) -> Value {
        json!({
            "version": OUTPUT_FILE_VERSION,
            "size": 123,
            "md5-aws-1": {
                "checksum": expected_md5,
                "part-checksums": [{
                    "part-size": 1,
                    "part-checksum": expected_md5
                }]
            }
        })
    }
}
