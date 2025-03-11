//! Defines the file format that outputs checksum results
//!

use crate::checksum::Ctx;
use crate::error::Error::SumsFileError;
use crate::error::{Error, Result};
use crate::reader::{ObjectSums, ObjectSumsBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_string};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};

/// The current version of the output file.
pub const OUTPUT_FILE_VERSION: &str = "1";

/// The file ending of a sums file.
pub const SUMS_FILE_ENDING: &str = ".sums";

/// Sums file state to enable writing and reading.
pub struct State {
    pub(crate) name: String,
    pub(crate) object_sums: Box<dyn ObjectSums + Send>,
}

impl State {
    /// Build from a name.
    pub async fn try_from(name: String) -> Result<Self> {
        Ok(Self {
            object_sums: ObjectSumsBuilder
                .build(SumsFile::format_target_file(&name))
                .await?,
            name,
        })
    }

    /// Get the inner values.
    pub fn into_inner(self) -> (String, Box<dyn ObjectSums + Send>) {
        (self.name, self.object_sums)
    }
}

impl Debug for State {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State").field("name", &self.name).finish()
    }
}

impl Eq for State {}

impl PartialEq for State {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Hash for State {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Clone for State {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            object_sums: dyn_clone::clone_box(&*self.object_sums),
        }
    }
}

/// A file containing multiple checksums.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[serde(rename_all = "kebab-case")]
pub struct SumsFile {
    pub(crate) version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) size: Option<u64>,
    // The name of the checksum is always the most canonical form.
    // E.g. no -be prefix for big-endian, and the part size as
    // the suffix for AWS checksums.
    #[serde(flatten)]
    pub(crate) checksums: BTreeMap<Ctx, Checksum>,
}

impl Default for SumsFile {
    fn default() -> Self {
        Self::new(None, BTreeMap::new())
    }
}

impl SumsFile {
    /// Create an output file.
    pub fn new(size: Option<u64>, checksums: BTreeMap<Ctx, Checksum>) -> Self {
        Self {
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

    /// Read from a slice and add the name.
    pub async fn read_from_slice(slice: &[u8]) -> Result<Self> {
        slice.try_into()
    }

    /// Merge with another output file, overwriting existing checksums,
    /// taking ownership of self. Returns an error if the size of the files
    /// do not match, and both files are not empty.
    pub fn merge(mut self, other: Self) -> Result<Self> {
        if self.size != other.size && !self.checksums.is_empty() && !other.checksums.is_empty() {
            return Err(SumsFileError(
                "the size of output files do not match".to_string(),
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
    }

    /// Split the sums file into multiple sums files, one for each checksum.
    pub fn split(self) -> Vec<SumsFile> {
        self.checksums
            .iter()
            .map(|(ctx, checksum)| {
                let mut sums_file = Self::default().with_size(self.size);
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
                // Two checksums are the same if they have the same top-level checksum. Since the
                // top level checksum encodes part information for AWS sums, there is no need to
                // compare the part checksums.
                if checksum == other_checksum {
                    return true;
                }
            }
        }

        false
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
pub struct Checksum(String);

impl Checksum {
    /// Create an output checksum.
    pub fn new(checksum: String) -> Self {
        Self(checksum)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::checksum::aws_etag::test::expected_md5_1gib;
    use crate::checksum::standard::test::EXPECTED_MD5_SUM;
    use serde_json::{from_value, json, to_value, Value};

    const EXPECTED_ETAG: &str = "1c3490f45b0cdc4299a128410def3a1d-b";

    #[test]
    fn serialize_output_file() -> Result<()> {
        let value = expected_output_file();
        let result = to_value(&value)?;
        let expected = expected_output_json();

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn deserialize_output_file() -> Result<()> {
        let value = expected_output_json();
        let result: SumsFile = from_value(value)?;
        let expected = expected_output_file();

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn is_same() -> Result<()> {
        let file_one = expected_output_file();
        let mut file_two = file_one.clone();
        let mut aws: Ctx = "md5-aws-123b".parse()?;
        aws.set_file_size(Some(123));

        file_two
            .checksums
            .insert(aws, Checksum::new(EXPECTED_ETAG.to_string()));
        assert!(file_one.is_same(&file_two));

        let mut file_two = file_one.clone();
        let mut aws: Ctx = "aws-etag-1b".parse()?;
        aws.set_file_size(Some(1));
        set_checksums(&mut file_two, aws);

        assert!(!file_one.is_same(&file_two));

        Ok(())
    }

    #[test]
    fn comparable() -> Result<()> {
        let file_one = expected_output_file();
        let mut file_two = file_one.clone();

        let mut aws: Ctx = "md5-aws-1b".parse()?;
        aws.set_file_size(Some(1));
        file_two
            .checksums
            .insert(aws, Checksum::new(expected_md5_1gib().to_string()));
        assert!(file_one.comparable(&file_two));

        let mut file_two = file_one.clone();
        let mut aws: Ctx = "aws-etag-1b".parse()?;
        aws.set_file_size(Some(1));
        set_checksums(&mut file_two, aws);

        assert!(!file_one.comparable(&file_two));

        Ok(())
    }

    #[test]
    fn merge() -> Result<()> {
        let expected_md5 = EXPECTED_MD5_SUM;
        let mut file_one = expected_output_file();

        let mut aws_one: Ctx = "aws-etag-123b".parse()?;
        aws_one.set_file_size(Some(123));
        file_one
            .checksums
            .insert(aws_one.clone(), Checksum::new(expected_md5.to_string()));

        let mut file_two = expected_output_file();
        let mut aws_two: Ctx = "md5-aws-123b".parse()?;
        aws_two.set_file_size(Some(123));
        set_checksums(&mut file_two, aws_two.clone());

        let result = file_one.clone().merge(file_two)?;
        assert_eq!(result.size, file_one.size);
        assert_eq!(
            result.checksums,
            BTreeMap::from_iter(vec![
                (aws_two, Checksum::new(expected_md5_1gib().to_string()),),
                (aws_one, Checksum::new(expected_md5_1gib().to_string())),
            ])
        );

        Ok(())
    }

    fn set_checksums(file_two: &mut SumsFile, aws: Ctx) {
        file_two.checksums =
            BTreeMap::from_iter(vec![(aws, Checksum::new(expected_md5_1gib().to_string()))]);
    }

    fn expected_output_file() -> SumsFile {
        let mut aws: Ctx = "md5-aws-123b".parse().unwrap();
        aws.set_file_size(Some(123));
        let checksums = vec![(aws, Checksum::new(EXPECTED_ETAG.to_string()))];
        SumsFile::new(Some(123), BTreeMap::from_iter(checksums))
    }

    fn expected_output_json() -> Value {
        json!({
            "version": OUTPUT_FILE_VERSION,
            "size": 123,
            "md5-aws-123b": EXPECTED_ETAG,
        })
    }
}
