//! Performs the check task to determine if files are identical from .sums files.
//!

use crate::checksum::file::{Checksum, SumsFile};
use crate::checksum::Ctx;
use crate::error::{ApiError, Error, Result};
use crate::io::sums::{ObjectSums, ObjectSumsBuilder};
use crate::stats::{CheckComparison, ChecksumPair};
use aws_sdk_s3::Client;
use clap::ValueEnum;
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::{Debug, Formatter};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use std::{fmt, mem, result};

/// Build a check task.
#[derive(Debug)]
pub struct CheckTaskBuilder {
    files: Vec<String>,
    sums_files: Vec<(String, SumsFile)>,
    group_by: GroupBy,
    update: bool,
    clients: Vec<Option<Arc<Client>>>,
    avoid_get_object_attributes: bool,
}

impl Default for CheckTaskBuilder {
    fn default() -> Self {
        Self {
            files: Default::default(),
            sums_files: Default::default(),
            group_by: Default::default(),
            update: Default::default(),
            // Ensure at least one element in the vector to repeat.
            clients: vec![None],
            avoid_get_object_attributes: Default::default(),
        }
    }
}

impl CheckTaskBuilder {
    /// Set the input files.
    pub fn with_input_files(mut self, files: Vec<String>) -> Self {
        self.files = files;
        self
    }

    /// Set the sums file directly without reading from input files.
    pub fn with_sums_files(mut self, files: Vec<(String, SumsFile)>) -> Self {
        self.sums_files = files;
        self
    }

    /// Set the S3 client to use for each input file.
    pub fn with_clients(mut self, clients: Vec<Arc<Client>>) -> Self {
        self.clients = clients.into_iter().map(Some).collect();
        self
    }

    /// Set the group by mode.
    pub fn with_group_by(mut self, group_by: GroupBy) -> Self {
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
        self.clients = vec![Some(client)];
        self
    }

    /// Avoid `GetObjectAttributes` calls.
    pub fn with_avoid_get_object_attributes(mut self, avoid_get_object_attributes: bool) -> Self {
        self.avoid_get_object_attributes = avoid_get_object_attributes;
        self
    }

    /// Build a check task.
    pub async fn build(mut self) -> Result<CheckTask> {
        let group_by = self.group_by;

        // Remove elements that are already set by in-memory sums files.
        let in_memory = self
            .sums_files
            .iter()
            .map(|(sums, _)| sums)
            .collect::<Vec<_>>();
        self.files.retain(|file| !in_memory.contains(&file));

        let (objects, errors): (Vec<_>, Vec<_>) = join_all(
            self.files
                .into_iter()
                .zip(self.clients.into_iter().cycle())
                .map(|(file, client)| async move {
                    let mut sums = ObjectSumsBuilder::default()
                        .with_avoid_get_object_attributes(self.avoid_get_object_attributes)
                        .set_client(client)
                        .build(file.to_string())
                        .await?;

                    let file_size = sums.file_size().await?;
                    let existing = sums
                        .sums_file()
                        .await?
                        .unwrap_or_else(|| SumsFile::new(file_size, Default::default()));

                    let errors = sums.api_errors();
                    Ok((
                        (
                            SumsKey((existing, sums.location())),
                            BTreeSet::from_iter(vec![State::ObjectSums(sums)]),
                        ),
                        errors,
                    ))
                }),
        )
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();

        let mut objects = BTreeMap::from_iter(objects);
        let errors = HashSet::from_iter(
            errors
                .into_iter()
                .flat_map(|err| err.into_iter().collect::<Vec<_>>()),
        );

        for (location, sums) in self.sums_files {
            objects.insert(
                SumsKey((sums.clone(), location.to_string())),
                BTreeSet::from_iter(vec![State::ExistingSums((location, sums))]),
            );
        }

        Ok(CheckTask {
            objects: CheckObjects(objects),
            group_by,
            update: self.update,
            recoverable_errors: errors,
            ..Default::default()
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
#[derive(Clone)]
pub enum State {
    ObjectSums(Box<dyn ObjectSums + Send>),
    ExistingSums((String, SumsFile)),
}

impl State {
    /// Get the location of the state.
    pub fn location(&self) -> String {
        match self {
            State::ObjectSums(object) => object.location(),
            State::ExistingSums((location, _)) => location.to_string(),
        }
    }

    /// Get the sums file.
    pub async fn sums_file(&mut self) -> Result<Option<SumsFile>> {
        match self {
            State::ObjectSums(object) => object.sums_file().await,
            State::ExistingSums((_, sums)) => Ok(Some(sums.clone())),
        }
    }

    /// Get the api errors.
    pub fn api_errors(&mut self) -> HashSet<ApiError> {
        match self {
            State::ObjectSums(object) => object.api_errors(),
            _ => HashSet::new(),
        }
    }

    /// Write the sums file to the location. If no object sums are used then this creates a new
    /// object sums to write the file.
    pub async fn write_sums_file(
        &self,
        sums: &SumsFile,
        client: Option<Arc<Client>>,
        avoid_get_object_attributes: bool,
    ) -> Result<()> {
        match self {
            State::ObjectSums(object) => object.write_sums_file(sums).await,
            State::ExistingSums((location, _)) => {
                ObjectSumsBuilder::default()
                    .set_client(client)
                    .with_avoid_get_object_attributes(avoid_get_object_attributes)
                    .build(location.to_string())
                    .await?
                    .write_sums_file(sums)
                    .await
            }
        }
    }
}

impl Debug for State {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("State")
    }
}

impl Hash for State {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.location().hash(state);
    }
}

impl Eq for State {}

impl PartialEq for State {
    fn eq(&self, other: &Self) -> bool {
        self.location() == other.location()
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        self.location().cmp(&other.location())
    }
}

/// Tracks information related to the sums file as it is being processed.
#[derive(Default, Debug)]
pub struct SumsKey(pub(crate) (SumsFile, String));

impl Hash for SumsKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl Eq for SumsKey {}

impl PartialEq for SumsKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl PartialOrd for SumsKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SumsKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

/// Objects processed from the check task.
#[derive(Default, Debug)]
pub struct CheckObjects(pub(crate) BTreeMap<SumsKey, BTreeSet<State>>);

impl CheckObjects {
    /// Get the inner value.
    pub fn into_inner(self) -> BTreeMap<SumsKey, BTreeSet<State>> {
        self.0
    }

    /// Get the groups of locations that were compared.
    pub fn to_groups(&self) -> Vec<Vec<String>> {
        let mut groups = Vec::with_capacity(self.0.len());
        for state in self.0.values() {
            groups.push(state.iter().map(|state| state.location()).collect());
        }
        groups
    }
}

impl Hash for CheckObjects {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.keys().for_each(|key| key.0.hash(state));
    }
}

/// The check type error with the task information when the error occurred.
pub struct CheckTaskError {
    pub task: CheckTask,
    pub error: Error,
}

impl Debug for CheckTaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.error)
    }
}

impl From<CheckTaskError> for Error {
    fn from(error: CheckTaskError) -> Self {
        error.error
    }
}

impl From<(CheckTask, Error)> for CheckTaskError {
    fn from((task, error): (CheckTask, Error)) -> Self {
        Self { task, error }
    }
}

/// The check task result type.
pub type CheckTaskResult = result::Result<CheckTask, CheckTaskError>;

/// Execute the check task.
#[derive(Default, Debug)]
pub struct CheckTask {
    objects: CheckObjects,
    group_by: GroupBy,
    update: bool,
    compared_directly: Vec<CheckComparison>,
    updated: Vec<String>,
    client: Option<Arc<Client>>,
    avoid_get_object_attributes: bool,
    recoverable_errors: HashSet<ApiError>,
}

impl CheckTask {
    fn hash<T: Hash>(value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    /// Groups sums files based on a comparison function.
    async fn merge_fn<F>(&mut self, compare: F)
    where
        for<'a> F: Fn(&'a SumsFile, &'a SumsFile) -> Option<(&'a Ctx, &'a Checksum)>,
    {
        // This might be more efficient using graph algorithms to find a set of connected
        // graphs based on the equality of the sums files.

        let mut state = Self::hash(&self.objects);
        let mut prev_state = state.wrapping_add(1);
        // Loop until the set of sums files does not change between iterations, i.e.
        // until the hash of the previous and current iteration is the same.
        while prev_state != state {
            // BTreeMap files are sorted already.
            let objects = mem::take(&mut self.objects);
            let mut objects = objects.0.into_iter().collect::<Vec<_>>();
            let mut reprocess = Vec::with_capacity(objects.len());

            // Process a single sums file at a time.
            'outer: while let Some((SumsKey((a, a_location)), mut a_locations)) = objects.pop() {
                // Check to see if it can be merged with another sums file in the list.
                for (SumsKey((b, b_location)), b_locations) in objects.iter_mut() {
                    // If it can be merged with another file, do the merge and add it back in for
                    // the next loop.
                    if let Some((ctx, checksum)) = compare(&a, b) {
                        self.compared_directly.push(CheckComparison::new(
                            vec![a_location, b_location.to_string()],
                            ChecksumPair::new(ctx.clone(), checksum.clone()),
                        ));

                        b_locations.append(&mut a_locations);

                        b.merge_mut(a);

                        continue 'outer;
                    }
                }

                // If it could not be merged, add it back into the list for re-processing.
                reprocess.push((SumsKey((a, a_location)), a_locations));
            }

            self.objects = CheckObjects(BTreeMap::from_iter(reprocess));

            // Update the hashes of the current and previous lists.
            prev_state = state;
            state = Self::hash(&self.objects);
        }
    }

    /// Merges the set of input sums files that are the same until no more merges can
    /// be performed. This can find sums files that are indirectly identical through
    /// other files. E.g. a.sums is equal to b.sums, and b.sums is equal to c.sums, but
    /// a.sums is not directly equal to c.sums because of different checksum types.
    pub async fn merge_same(&mut self) {
        self.merge_fn(|a, b| a.is_same(b)).await;
    }

    /// Determine the set of checksums for all files.
    pub async fn merge_comparable(&mut self) {
        self.merge_fn(|a, b| a.comparable(b)).await;
        // The checksum value doesn't mean much if two sums files are comparable but not equal,
        // so it should be cleared.
        let mut files = BTreeMap::new();
        while let Some((mut key, locations)) = self.objects.0.pop_last() {
            key.0
                 .0
                .checksums
                .iter_mut()
                .for_each(|(_, checksum)| *checksum = Default::default());
            files.insert(key, locations);
        }
        self.objects = CheckObjects(files);
    }

    async fn do_check(&mut self) -> Result<()> {
        let update = self.update && matches!(self.group_by, GroupBy::Equality);
        let avoid_get_object_attributes = self.avoid_get_object_attributes;
        let client = self.client.clone();
        match self.group_by {
            GroupBy::Equality => self.merge_same().await,
            GroupBy::Comparability => self.merge_comparable().await,
        };

        let mut updated_sums = vec![];
        if update {
            for (SumsKey((file, _)), locations) in &self.objects.0 {
                for location in locations {
                    let mut location = location.clone();
                    let current = location.sums_file().await?;

                    self.recoverable_errors.extend(location.api_errors());
                    if current.as_ref() != Some(file) {
                        location
                            .write_sums_file(file, client.clone(), avoid_get_object_attributes)
                            .await?;
                        updated_sums.push(location.location());
                    }
                }
            }
        }

        self.updated = updated_sums;

        Ok(())
    }

    /// Runs the check task, returning the list of matching files.
    pub async fn run(mut self) -> CheckTaskResult {
        match self.do_check().await {
            Ok(_) => Ok(self),
            Err(err) => Err((self, err).into()),
        }
    }

    /// Get the inner values.
    pub fn into_inner(
        self,
    ) -> (
        CheckObjects,
        Vec<CheckComparison>,
        Vec<String>,
        HashSet<ApiError>,
    ) {
        (
            self.objects,
            self.compared_directly,
            self.updated,
            self.recoverable_errors,
        )
    }

    /// Get the inner state objects.
    pub fn state_objects(&self) -> &BTreeMap<SumsKey, BTreeSet<State>> {
        &self.objects.0
    }

    /// Get the comparisons.
    pub fn compared_directly(&self) -> &[CheckComparison] {
        self.compared_directly.as_slice()
    }

    /// Get the api errors.
    pub fn api_errors(self) -> HashSet<ApiError> {
        self.recoverable_errors.clone()
    }

    /// Does the state of the check task contain no checksums in any sums files.
    pub fn is_empty(&self) -> bool {
        self.objects.0.iter().all(|(key, _)| {
            let SumsKey((sums, _)) = key;
            sums.is_empty()
        })
    }

    /// Get the group by type.
    pub fn group_by(&self) -> GroupBy {
        self.group_by
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

        let result: Vec<_> = check
            .run()
            .await
            .unwrap()
            .objects
            .0
            .into_keys()
            .map(|key| key.0 .0)
            .collect();

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

        let result: Vec<_> = check
            .run()
            .await
            .unwrap()
            .objects
            .0
            .into_keys()
            .map(|key| key.0 .0)
            .collect();

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

        let result: Vec<_> = check
            .run()
            .await
            .unwrap()
            .objects
            .0
            .into_keys()
            .map(|key| key.0 .0)
            .collect();

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

        let result: Vec<_> = check
            .run()
            .await
            .unwrap()
            .objects
            .0
            .into_keys()
            .map(|key| key.0 .0)
            .collect();

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
        let path = tmp.keep();

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
        let path = tmp.keep();

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
        let path = tmp.keep();

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
