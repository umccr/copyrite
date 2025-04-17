//! Performs the check task to determine if files are identical from .sums files.
//!

use crate::checksum::file::SumsFile;
use crate::error::{Error, Result};
use crate::io::sums::{ObjectSums, ObjectSumsBuilder};
use aws_sdk_s3::Client;
use clap::ValueEnum;
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

/// Build a check task.
#[derive(Debug, Default)]
pub struct CheckTaskBuilder {
    files: Vec<String>,
    group_by: GroupBy,
    update: bool,
    client: Option<Arc<Client>>,
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

    /// Update the checked files by writing them back.
    pub fn with_update(mut self, update: bool) -> Self {
        self.update = update;
        self
    }

    /// Set the S3 client to use.
    pub fn with_client(mut self, client: Arc<Client>) -> Self {
        self.client = Some(client);
        self
    }

    /// Build a check task.
    pub async fn build(self) -> Result<CheckTask> {
        let group_by = self.group_by;
        let objects = join_all(self.files.into_iter().map(|file| {
            let client = self.client.clone();

            async move {
                let mut sums = ObjectSumsBuilder::default()
                    .set_client(client)
                    .build(file.to_string())
                    .await?;

                let file_size = sums.file_size().await?;
                let existing = sums
                    .sums_file()
                    .await?
                    .unwrap_or_else(|| SumsFile::new(file_size, Default::default()));

                Ok((existing, BTreeSet::from_iter(vec![State(sums)])))
            }
        }))
        .await
        .into_iter()
        .collect::<Result<BTreeMap<_, _>>>()?;

        Ok(CheckTask {
            objects: CheckObjects(objects),
            group_by,
            update: self.update,
        })
    }
}

/// The kind of check group by function to use.
#[derive(Debug, Default, Clone, Copy, ValueEnum, Serialize, Deserialize)]
pub enum GroupBy {
    /// Shows groups of sums files that are equal.
    #[default]
    Equality,
    /// Shows groups of sums files that are comparable. This means that at least one checksum
    /// overlaps, although it does not necessarily mean that they are equal.
    Comparability,
}

/// Representation of file state to implement equality and hashing.
pub struct State(pub(crate) Box<dyn ObjectSums + Send>);

impl Debug for State {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("State")
    }
}

impl Hash for State {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.location().hash(state);
    }
}

impl Eq for State {}

impl PartialEq for State {
    fn eq(&self, other: &Self) -> bool {
        self.0.location() == other.0.location()
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.location().cmp(&other.0.location())
    }
}

/// Objects processed from the check task.
#[derive(Default, Debug)]
pub struct CheckObjects(pub(crate) BTreeMap<SumsFile, BTreeSet<State>>);

impl CheckObjects {
    /// Get the inner value.
    pub fn into_inner(self) -> BTreeMap<SumsFile, BTreeSet<State>> {
        self.0
    }
}

impl Hash for CheckObjects {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.keys().for_each(|key| key.hash(state));
    }
}

/// Execute the check task.
#[derive(Default, Debug)]
pub struct CheckTask {
    objects: CheckObjects,
    group_by: GroupBy,
    update: bool,
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

        let mut state = Self::hash(&self.objects);
        let mut prev_state = state.wrapping_add(1);
        // Loop until the set of sums files does not change between iterations, i.e.
        // until the hash of the previous and current iteration is the same.
        while prev_state != state {
            // BTreeMap files are sorted already.
            let mut objects = self.objects.0.into_iter().collect::<Vec<_>>();
            let mut reprocess = Vec::with_capacity(objects.len());

            // Process a single sums file at a time.
            'outer: while let Some((a, mut a_locations)) = objects.pop() {
                // Check to see if it can be merged with another sums file in the list.
                for (b, b_locations) in objects.iter_mut() {
                    // If it can be merged with another file, do the merge and add it back in for
                    // the next loop.
                    if compare(&a, b) {
                        b.merge_mut(a);
                        b_locations.append(&mut a_locations);
                        continue 'outer;
                    }
                }

                // If it could not be merged, add it back into the list for re-processing.
                reprocess.push((a, a_locations));
            }

            self.objects = CheckObjects(BTreeMap::from_iter(reprocess));

            // Update the hashes of the current and previous lists.
            prev_state = state;
            state = Self::hash(&self.objects);
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
        let mut files = BTreeMap::new();
        while let Some((mut file, locations)) = self.objects.0.pop_last() {
            file.checksums
                .iter_mut()
                .for_each(|(_, checksum)| *checksum = Default::default());
            files.insert(file, locations);
        }
        self.objects = CheckObjects(files);

        Ok(self)
    }

    /// Runs the check task, returning the list of matching files.
    pub async fn run(self) -> Result<CheckObjects> {
        let update = self.update && matches!(self.group_by, GroupBy::Equality);
        let result = match self.group_by {
            GroupBy::Equality => Ok::<_, Error>(self.merge_same().await?.objects),
            GroupBy::Comparability => Ok(self.merge_comparable().await?.objects),
        }?;

        if update {
            for (file, locations) in &result.0 {
                for location in locations {
                    location.0.write_sums_file(file).await?;
                }
            }
        }

        Ok(result)
    }
}

impl From<(CheckObjects, GroupBy)> for CheckOutput {
    fn from((objects, group_by): (CheckObjects, GroupBy)) -> Self {
        let mut groups = Vec::with_capacity(objects.0.len());
        for (_, state) in objects.0 {
            groups.push(state.iter().map(|state| state.0.location()).collect());
        }
        CheckOutput::new(groups, group_by)
    }
}

/// Output type when checking files.
#[derive(Debug, Serialize, Deserialize)]
pub struct CheckOutput {
    group_by: GroupBy,
    groups: Vec<Vec<String>>,
}

impl CheckOutput {
    /// Create a new check output.
    pub fn new(groups: Vec<Vec<String>>, group_by: GroupBy) -> Self {
        Self { groups, group_by }
    }

    /// Convert to a JSON string.
    pub fn to_json_string(&self) -> Result<String> {
        Ok(to_string(&self)?)
    }

    /// Get the grouping option.
    pub fn group_by(&self) -> GroupBy {
        self.group_by
    }

    /// Get the groups.
    pub fn groups(&self) -> &[Vec<String>] {
        self.groups.as_slice()
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::checksum::file::Checksum;
    use crate::error::Error;
    use crate::io::sums::file::FileBuilder;
    use crate::test::TEST_FILE_SIZE;
    use anyhow::Result;
    use std::collections::BTreeMap;
    use std::path::Path;
    use tempfile::{tempdir, TempDir};

    #[tokio::test]
    async fn test_check() -> Result<()> {
        let tmp = tempdir()?;
        let files = write_test_files_one_group(tmp).await?;

        let check = CheckTaskBuilder::default()
            .with_input_files(files.iter().map(|name| name.to_string()).collect())
            .build()
            .await?;

        let result: Vec<_> = check.run().await?.0.into_keys().collect();

        assert_eq!(
            result,
            vec![SumsFile::new(
                Some(TEST_FILE_SIZE),
                BTreeMap::from_iter(vec![
                    ("md5".parse()?, Checksum::new("123".to_string()),),
                    ("sha1".parse()?, Checksum::new("456".to_string()),),
                    ("sha256".parse()?, Checksum::new("789".to_string()),),
                    ("crc32".parse()?, Checksum::new("012".to_string()),)
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
            .with_input_files(files.iter().map(|name| name.to_string()).collect())
            .with_group_by(GroupBy::Comparability)
            .build()
            .await?;

        let result: Vec<_> = check.run().await?.0.into_keys().collect();

        assert_eq!(
            result,
            vec![SumsFile::new(
                Some(TEST_FILE_SIZE),
                BTreeMap::from_iter(vec![
                    ("md5".parse()?, Default::default(),),
                    ("sha1".parse()?, Default::default(),),
                    ("sha256".parse()?, Default::default(),),
                    ("crc32".parse()?, Default::default(),),
                    ("crc32c".parse()?, Default::default(),)
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
            .with_input_files(files.iter().map(|name| name.to_string()).collect())
            .build()
            .await?;

        let result: Vec<_> = check.run().await?.0.into_keys().collect();

        assert_eq!(
            result,
            vec![
                SumsFile::new(
                    Some(TEST_FILE_SIZE),
                    BTreeMap::from_iter(vec![
                        ("sha256".parse()?, Checksum::new("abc".to_string()),),
                        ("crc32".parse()?, Checksum::new("efg".to_string()),),
                        ("crc32c".parse()?, Checksum::new("hij".to_string()),)
                    ])
                ),
                SumsFile::new(
                    Some(TEST_FILE_SIZE),
                    BTreeMap::from_iter(vec![
                        ("md5".parse()?, Checksum::new("123".to_string()),),
                        ("sha1".parse()?, Checksum::new("456".to_string()),),
                        ("sha256".parse()?, Checksum::new("789".to_string()),)
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
            .with_input_files(files.iter().map(|name| name.to_string()).collect())
            .with_group_by(GroupBy::Comparability)
            .build()
            .await?;

        let result: Vec<_> = check.run().await?.0.into_keys().collect();

        assert_eq!(
            result,
            vec![
                SumsFile::new(
                    Some(TEST_FILE_SIZE),
                    BTreeMap::from_iter(vec![
                        ("crc32".parse()?, Default::default(),),
                        ("crc32c".parse()?, Default::default(),)
                    ])
                ),
                SumsFile::new(
                    Some(TEST_FILE_SIZE),
                    BTreeMap::from_iter(vec![
                        ("md5".parse()?, Default::default(),),
                        ("sha1".parse()?, Default::default(),),
                        ("sha256".parse()?, Default::default(),)
                    ])
                ),
            ]
        );

        Ok(())
    }

    pub(crate) async fn write_test_files_one_group(tmp: TempDir) -> Result<Vec<String>, Error> {
        let path = tmp.into_path();

        let mut names = write_test_files(&path).await?;

        let c_name = path.join("c").to_string_lossy().to_string();
        let c = SumsFile::new(
            Some(TEST_FILE_SIZE),
            BTreeMap::from_iter(vec![
                ("sha256".parse()?, Checksum::new("789".to_string())),
                ("crc32".parse()?, Checksum::new("012".to_string())),
            ]),
        );
        FileBuilder::default()
            .with_file(c_name.to_string())
            .build()?
            .write_sums(&c)
            .await?;

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
            Some(TEST_FILE_SIZE),
            BTreeMap::from_iter(vec![
                ("crc32c".parse()?, Checksum::new("789".to_string())),
                ("crc32".parse()?, Checksum::new("012".to_string())),
            ]),
        );
        FileBuilder::default()
            .with_file(c_name.to_string())
            .build()?
            .write_sums(&c)
            .await?;

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
            Some(TEST_FILE_SIZE),
            BTreeMap::from_iter(vec![
                ("sha256".parse()?, Checksum::new("abc".to_string())),
                ("crc32".parse()?, Checksum::new("efg".to_string())),
            ]),
        );
        FileBuilder::default()
            .with_file(c_name.to_string())
            .build()?
            .write_sums(&c)
            .await?;

        let d_name = path.join("d").to_string_lossy().to_string();
        let d = SumsFile::new(
            Some(TEST_FILE_SIZE),
            BTreeMap::from_iter(vec![
                ("crc32".parse()?, Checksum::new("efg".to_string())),
                ("crc32c".parse()?, Checksum::new("hij".to_string())),
            ]),
        );
        FileBuilder::default()
            .with_file(d_name.to_string())
            .build()?
            .write_sums(&d)
            .await?;

        names.extend(vec![c_name, d_name]);

        Ok(names)
    }

    async fn write_test_files(path: &Path) -> Result<Vec<String>, Error> {
        let a_name = path.join("a").to_string_lossy().to_string();
        let a = SumsFile::new(
            Some(TEST_FILE_SIZE),
            BTreeMap::from_iter(vec![
                ("md5".parse()?, Checksum::new("123".to_string())),
                ("sha1".parse()?, Checksum::new("456".to_string())),
            ]),
        );
        FileBuilder::default()
            .with_file(a_name.to_string())
            .build()?
            .write_sums(&a)
            .await?;

        let b_name = path.join("b").to_string_lossy().to_string();
        let b = SumsFile::new(
            Some(TEST_FILE_SIZE),
            BTreeMap::from_iter(vec![
                ("sha1".parse()?, Checksum::new("456".to_string())),
                ("sha256".parse()?, Checksum::new("789".to_string())),
            ]),
        );
        FileBuilder::default()
            .with_file(b_name.to_string())
            .build()?
            .write_sums(&b)
            .await?;

        Ok(vec![a_name, b_name])
    }
}
