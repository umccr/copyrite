//! Performs the check task to determine if files are identical from .sums files.
//!

use crate::checksum::file::SumsFile;
use crate::checksum::Ctx;
use crate::error::Result;
use futures_util::future::join_all;
use std::collections::HashSet;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::str::FromStr;

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
        let files = join_all(
            self.files
                .into_iter()
                .map(|file| async { SumsFile::read_from(file).await }),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        Ok(CheckTask { files })
    }
}

/// Execute the check task.
#[derive(Debug, Default)]
pub struct CheckTask {
    files: Vec<SumsFile>,
}

impl CheckTask {
    fn hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Merges the set of input sums files that are the same until no more merges can
    /// be performed. This can find sums files that are indirectly identical through
    /// other files. E.g. a.sums is equal to b.sums, and b.sums is equal to c.sums, but
    /// a.sums is not directly equal to c.sums because of different checksum types.
    pub async fn merge_same(mut self) -> Result<Self> {
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
                    if b.is_same(&a) {
                        b.merge_mut(a.clone());
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

    /// Determine the set of checksums for all files.
    pub async fn checksum_set(&self) -> Result<HashSet<Ctx>> {
        self.files.iter().try_fold(HashSet::new(), |mut set, file| {
            for checksum in file.checksums.keys() {
                set.insert(Ctx::from_str(checksum)?);
            }

            Ok(set)
        })
    }

    /// Runs the check task, returning the list of matching files.
    pub async fn run(self) -> Result<Vec<SumsFile>> {
        Ok(self.merge_same().await?.files)
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
    async fn test_checksum_set() -> Result<()> {
        let tmp = tempdir()?;
        let files = write_test_files_one_group(tmp).await?;

        let check = CheckTaskBuilder::default()
            .with_input_files(files.clone())
            .build()
            .await?;

        let result = check.checksum_set().await?;

        assert_eq!(
            result,
            HashSet::from_iter(vec![
                Ctx::from_str("md5")?,
                Ctx::from_str("sha1")?,
                Ctx::from_str("sha256")?,
                Ctx::from_str("crc32")?,
            ])
        );

        Ok(())
    }

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
    async fn test_check_multiple_groups() -> Result<()> {
        let tmp = tempdir()?;
        let files = write_test_files_multiple_group(tmp).await?;

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

    pub(crate) async fn write_test_files_multiple_group(
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
