//! Performs the check task to determine if files are identical from .sums files.
//!

use crate::checksum::file::SumsFile;
use crate::error::Result;
use clap::ValueEnum;
use futures_util::future::join_all;
use std::hash::{DefaultHasher, Hash, Hasher};

/// Build a check task.
#[derive(Debug, Default)]
pub struct CheckTaskBuilder {
    files: Vec<String>,
    group_by: GroupBy,
}

impl CheckTaskBuilder {
    /// Set the input files.
    pub fn with_input_files(mut self, files: Vec<String>) -> Self {
        self.files = files;
        self
    }

    /// Set the group by mode.
    pub fn with_group_by(mut self, group_by: GroupBy) -> Self {
        self.group_by = group_by;
        self
    }

    /// Generate missing checksums that are required to check for equality.
    pub fn generate_missing(mut self, group_by: GroupBy) -> Self {
        self.group_by = group_by;
        self
    }

    /// Build a check task.
    pub async fn build(self) -> Result<CheckTask> {
        let group_by = self.group_by;
        let files = join_all(
            self.files
                .into_iter()
                .map(|file| async { SumsFile::read_from(file).await }),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        Ok(CheckTask { files, group_by })
    }
}

/// The kind of check group by function to use.
#[derive(Debug, Default, Clone, Copy, ValueEnum)]
pub enum GroupBy {
    /// Shows groups of sums files that are equal.
    #[default]
    Equality,
    /// Shows groups of sums files that are comparable. This means that at least one checksum
    /// overlaps, although it does not necessarily mean that they are equal.
    Comparability,
}

/// Execute the check task.
#[derive(Debug, Default)]
pub struct CheckTask {
    files: Vec<SumsFile>,
    group_by: GroupBy,
}

impl CheckTask {
    fn hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Groups sums files based on a comparison function.
    async fn merge_fn<F>(mut self, compare: F) -> Result<Self>
    where
        F: Fn(&SumsFile, &SumsFile) -> bool,
    {
        // This might be more efficient using graph algorithms to find a set of connected
        // graphs based on the equality of the sums files.

        self.files.sort();
        let mut state = Self::hash(&self.files);
        let mut prev_state = state.wrapping_add(1);
        // Loop until the set of sums files does not change between iterations, i.e.
        // until the hash of the previous and current iteration is the same.
        while prev_state != state {
            let mut reprocess = Vec::with_capacity(self.files.len());

            // Process a single sums file at a time.
            'outer: while let Some(a) = self.files.pop() {
                // Check to see if it can be merged with another sums file in the list.
                for b in self.files.iter_mut() {
                    if compare(&a, b) {
                        b.merge_mut(a);
                        continue 'outer;
                    }
                }

                // If it could not be merged, add it back into the list for re-processing.
                reprocess.push(a);
            }

            self.files = reprocess;
            self.files.sort();

            // Update the hashes of the current and previous lists.
            prev_state = state;
            state = Self::hash(&self.files);
        }

        Ok(self)
    }

    /// Merges the set of input sums files that are the same until no more merges can
    /// be performed. This can find sums files that are indirectly identical through
    /// other files. E.g. a.sums is equal to b.sums, and b.sums is equal to c.sums, but
    /// a.sums is not directly equal to c.sums because of different checksum types.
    pub async fn merge_same(mut self) -> Result<Self> {
        self = self.merge_fn(|a, b| a.is_same(b)).await?;
        Ok(self)
    }

    /// Determine the set of checksums for all files.
    pub async fn merge_comparable(mut self) -> Result<Self> {
        self = self.merge_fn(|a, b| a.comparable(b)).await?;
        // The checksum value doesn't mean much if two sums files are comparable but not equal,
        // so it should be cleared.
        self.files.iter_mut().for_each(|file| {
            file.checksums
                .iter_mut()
                .for_each(|(_, checksum)| *checksum = Default::default());
        });

        Ok(self)
    }

    /// Runs the check task, returning the list of matching files.
    pub async fn run(self) -> Result<Vec<SumsFile>> {
        match self.group_by {
            GroupBy::Equality => Ok(self.merge_same().await?.files),
            GroupBy::Comparability => Ok(self.merge_comparable().await?.files),
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::checksum::file::Checksum;
    use crate::error::Error;
    use crate::test::TEST_FILE_SIZE;
    use anyhow::Result;
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;
    use tempfile::{tempdir, TempDir};

    #[tokio::test]
    async fn test_check() -> Result<()> {
        let tmp = tempdir()?;
        let files = write_test_files_one_group(tmp).await?;

        let check = CheckTaskBuilder::default()
            .with_input_files(files.clone())
            .build()
            .await?;

        let result = check.run().await?;

        assert_eq!(
            result,
            vec![SumsFile::new(
                BTreeSet::from_iter(files),
                TEST_FILE_SIZE,
                BTreeMap::from_iter(vec![
                    (
                        "md5".to_string(),
                        Checksum::new("123".to_string(), None, None),
                    ),
                    (
                        "sha1".to_string(),
                        Checksum::new("456".to_string(), None, None),
                    ),
                    (
                        "sha256".to_string(),
                        Checksum::new("789".to_string(), None, None),
                    ),
                    (
                        "crc32".to_string(),
                        Checksum::new("012".to_string(), None, None),
                    )
                ])
            )]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_check_comparable() -> Result<()> {
        let tmp = tempdir()?;
        let files = write_test_files_multiple_groups(tmp).await?;

        let check = CheckTaskBuilder::default()
            .with_input_files(files.clone())
            .with_group_by(GroupBy::Comparability)
            .build()
            .await?;

        let result = check.run().await?;

        assert_eq!(
            result,
            vec![SumsFile::new(
                BTreeSet::from_iter(files),
                TEST_FILE_SIZE,
                BTreeMap::from_iter(vec![
                    ("md5".to_string(), Default::default(),),
                    ("sha1".to_string(), Default::default(),),
                    ("sha256".to_string(), Default::default(),),
                    ("crc32".to_string(), Default::default(),),
                    ("crc".to_string(), Default::default(),)
                ])
            )]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_check_multiple_groups() -> Result<()> {
        let tmp = tempdir()?;
        let files = write_test_files_multiple_groups(tmp).await?;

        let check = CheckTaskBuilder::default()
            .with_input_files(files.clone())
            .build()
            .await?;

        let result = check.run().await?;

        assert_eq!(
            result,
            vec![
                SumsFile::new(
                    BTreeSet::from_iter(files.clone().into_iter().take(2)),
                    TEST_FILE_SIZE,
                    BTreeMap::from_iter(vec![
                        (
                            "md5".to_string(),
                            Checksum::new("123".to_string(), None, None),
                        ),
                        (
                            "sha1".to_string(),
                            Checksum::new("456".to_string(), None, None),
                        ),
                        (
                            "sha256".to_string(),
                            Checksum::new("789".to_string(), None, None),
                        )
                    ])
                ),
                SumsFile::new(
                    BTreeSet::from_iter(files.clone().into_iter().skip(2)),
                    TEST_FILE_SIZE,
                    BTreeMap::from_iter(vec![
                        (
                            "sha256".to_string(),
                            Checksum::new("abc".to_string(), None, None),
                        ),
                        (
                            "crc32".to_string(),
                            Checksum::new("efg".to_string(), None, None),
                        ),
                        (
                            "crc".to_string(),
                            Checksum::new("hij".to_string(), None, None),
                        )
                    ])
                )
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_check_comparable_multiple_groups() -> Result<()> {
        let tmp = tempdir()?;
        let files = write_test_files_not_comparable(tmp).await?;

        let check = CheckTaskBuilder::default()
            .with_input_files(files.clone())
            .with_group_by(GroupBy::Comparability)
            .build()
            .await?;

        let result = check.run().await?;

        assert_eq!(
            result,
            vec![
                SumsFile::new(
                    BTreeSet::from_iter(files.clone().into_iter().take(2)),
                    TEST_FILE_SIZE,
                    BTreeMap::from_iter(vec![
                        ("md5".to_string(), Default::default(),),
                        ("sha1".to_string(), Default::default(),),
                        ("sha256".to_string(), Default::default(),)
                    ])
                ),
                SumsFile::new(
                    BTreeSet::from_iter(files.clone().into_iter().skip(2)),
                    TEST_FILE_SIZE,
                    BTreeMap::from_iter(vec![
                        ("crc32".to_string(), Default::default(),),
                        ("crc".to_string(), Default::default(),)
                    ])
                )
            ]
        );

        Ok(())
    }

    pub(crate) async fn write_test_files_one_group(tmp: TempDir) -> Result<Vec<String>, Error> {
        let path = tmp.into_path();

        let mut names = write_test_files(&path).await?;

        let c_name = path.join("c").to_string_lossy().to_string();
        let c = SumsFile::new(
            BTreeSet::from_iter(vec![c_name.to_string()]),
            TEST_FILE_SIZE,
            BTreeMap::from_iter(vec![
                (
                    "sha256".to_string(),
                    Checksum::new("789".to_string(), None, None),
                ),
                (
                    "crc32".to_string(),
                    Checksum::new("012".to_string(), None, None),
                ),
            ]),
        );
        c.write().await?;

        names.push(c_name);

        Ok(names)
    }

    pub(crate) async fn write_test_files_not_comparable(
        tmp: TempDir,
    ) -> Result<Vec<String>, Error> {
        let path = tmp.into_path();

        let mut names = write_test_files(&path).await?;

        let c_name = path.join("c").to_string_lossy().to_string();
        let c = SumsFile::new(
            BTreeSet::from_iter(vec![c_name.to_string()]),
            TEST_FILE_SIZE,
            BTreeMap::from_iter(vec![
                (
                    "crc".to_string(),
                    Checksum::new("789".to_string(), None, None),
                ),
                (
                    "crc32".to_string(),
                    Checksum::new("012".to_string(), None, None),
                ),
            ]),
        );
        c.write().await?;

        names.push(c_name);

        Ok(names)
    }

    pub(crate) async fn write_test_files_multiple_groups(
        tmp: TempDir,
    ) -> Result<Vec<String>, Error> {
        let path = tmp.into_path();

        let mut names = write_test_files(&path).await?;

        let c_name = path.join("c").to_string_lossy().to_string();
        let c = SumsFile::new(
            BTreeSet::from_iter(vec![c_name.to_string()]),
            TEST_FILE_SIZE,
            BTreeMap::from_iter(vec![
                (
                    "sha256".to_string(),
                    Checksum::new("abc".to_string(), None, None),
                ),
                (
                    "crc32".to_string(),
                    Checksum::new("efg".to_string(), None, None),
                ),
            ]),
        );
        c.write().await?;

        let d_name = path.join("d").to_string_lossy().to_string();
        let d = SumsFile::new(
            BTreeSet::from_iter(vec![d_name.to_string()]),
            TEST_FILE_SIZE,
            BTreeMap::from_iter(vec![
                (
                    "crc32".to_string(),
                    Checksum::new("efg".to_string(), None, None),
                ),
                (
                    "crc".to_string(),
                    Checksum::new("hij".to_string(), None, None),
                ),
            ]),
        );
        d.write().await?;

        names.extend(vec![c_name, d_name]);

        Ok(names)
    }

    async fn write_test_files(path: &Path) -> Result<Vec<String>, Error> {
        let a_name = path.join("a").to_string_lossy().to_string();
        let a = SumsFile::new(
            BTreeSet::from_iter(vec![a_name.to_string()]),
            TEST_FILE_SIZE,
            BTreeMap::from_iter(vec![
                (
                    "md5".to_string(),
                    Checksum::new("123".to_string(), None, None),
                ),
                (
                    "sha1".to_string(),
                    Checksum::new("456".to_string(), None, None),
                ),
            ]),
        );
        a.write().await?;

        let b_name = path.join("b").to_string_lossy().to_string();
        let b = SumsFile::new(
            BTreeSet::from_iter(vec![b_name.to_string()]),
            TEST_FILE_SIZE,
            BTreeMap::from_iter(vec![
                (
                    "sha1".to_string(),
                    Checksum::new("456".to_string(), None, None),
                ),
                (
                    "sha256".to_string(),
                    Checksum::new("789".to_string(), None, None),
                ),
            ]),
        );
        b.write().await?;

        Ok(vec![a_name, b_name])
    }
}
