//! Structs related to output statistics.
//!

use crate::checksum::file::{Checksum, SumsFile};
use crate::checksum::Ctx;
use crate::cli::CopyMode;
use crate::error::{ApiError, Error};
use crate::task::check::{CheckTask, CheckTaskError, GroupBy};
use crate::task::copy::{CopyTask, CopyTaskError};
use crate::task::generate::{GenerateTask, GenerateTaskError, GenerateTaskResult};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

/// The result type for stats.
pub type Result<T> = std::result::Result<T, Box<T>>;

/// Stats from running a `generate` command.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct GenerateStats {
    /// Time taken in seconds.
    pub(crate) elapsed_seconds: f64,
    /// The stats for individual file objects.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) stats: Vec<GenerateFileStats>,
    /// Stats from running `check` for comparability when computing sums with `--missing`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) check_stats: Option<Box<CheckStats>>,
    /// The API errors if there was permission issues for object attributes.
    #[serde(skip_serializing_if = "HashSet::is_empty")]
    pub(crate) recoverable_errors: HashSet<ApiError>,
    /// An unrecoverable error occurred, causing the execution to stop.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unrecoverable_error: Option<Error>,
    #[serde(skip)]
    pub(crate) sums: Option<Vec<(String, SumsFile)>>,
}

impl From<Error> for Box<GenerateStats> {
    fn from(err: Error) -> Self {
        Box::new(GenerateStats {
            unrecoverable_error: Some(err),
            ..Default::default()
        })
    }
}

impl From<GenerateTaskError> for Box<GenerateStats> {
    fn from(err: GenerateTaskError) -> Self {
        err.error.into()
    }
}

impl GenerateStats {
    /// Create new generate stats.
    pub fn new(
        elapsed_seconds: f64,
        stats: Vec<GenerateFileStats>,
        check_stats: Option<CheckStats>,
    ) -> Self {
        let mut result = Self {
            elapsed_seconds,
            ..Default::default()
        };

        stats.into_iter().for_each(|stat| result.push_stats(stat));
        result.set_check_stats(check_stats);

        result
    }

    /// Create stats from a sums file.
    pub fn from_sums(sums: Vec<(String, SumsFile)>) -> Self {
        Self {
            sums: Some(sums),
            ..Default::default()
        }
    }

    fn push_stats(&mut self, stats: GenerateFileStats) {
        if !stats.checksums_generated.0.is_empty() {
            self.stats.push(stats);
        }
    }

    fn push_task(&mut self, task: GenerateTask) {
        self.push_stats(GenerateFileStats::from_task(task));
    }

    /// Add generate stats for a file.
    pub fn add_stats(mut self, task: GenerateTaskResult) -> Result<Self> {
        match task {
            Ok(task) => {
                self.push_task(task);
                Ok(self)
            }
            Err(err) => {
                self.push_task(err.task);
                Err(Box::new(self))
            }
        }
    }

    /// Set the seconds of the task.
    pub fn set_elapsed_seconds(&mut self, elapsed_seconds: f64) {
        self.elapsed_seconds = elapsed_seconds;
    }

    /// Set the seconds of the task.
    pub fn set_sums_files(&mut self, sums: Vec<(String, SumsFile)>) {
        self.sums = Some(sums);
    }

    /// Set the check stats.
    pub fn set_check_stats(&mut self, check_stats: Option<CheckStats>) {
        self.check_stats = check_stats.map(Box::new);
    }

    /// Set the recoverable errors.
    pub fn set_recoverable_errors(&mut self, recoverable_errors: HashSet<ApiError>) {
        self.recoverable_errors = recoverable_errors;
    }
}

/// A checksum pair represents the reason that a check command succeeded.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChecksumPair {
    /// The kind of checksum, e.g. `md5`.
    pub(crate) kind: Ctx,
    /// The value of the checksum.
    pub(crate) value: Checksum,
}

impl ChecksumPair {
    /// Create a new checksum pair.
    pub fn new(kind: Ctx, value: Checksum) -> Self {
        Self { kind, value }
    }
}

impl From<&CheckStats> for Option<ChecksumPair> {
    fn from(stats: &CheckStats) -> Self {
        stats
            .compared
            .first()
            .map(|compared| compared.reason.clone())
    }
}

/// A list of checksum pair "reasons".
#[derive(Serialize, Deserialize, Debug)]
pub struct ChecksumStats(Vec<ChecksumPair>);

impl From<BTreeMap<Ctx, Checksum>> for ChecksumStats {
    fn from(map: BTreeMap<Ctx, Checksum>) -> Self {
        Self(
            map.into_iter()
                .map(|(k, v)| ChecksumPair::new(k, v))
                .collect(),
        )
    }
}

/// Generate stats for an individual file.
#[derive(Serialize, Deserialize, Debug)]
pub struct GenerateFileStats {
    /// The location of the file.
    pub(crate) input: String,
    /// Whether the .sums file was updated. This might be false if `--verify` was used and no
    /// sums needed to be updated.
    pub(crate) updated: bool,
    /// The set of checksums that were generated.
    pub(crate) checksums_generated: ChecksumStats,
}

impl GenerateFileStats {
    /// Create new generate stats.
    pub fn new(input: String, updated: bool, checksums_generated: ChecksumStats) -> Self {
        Self {
            input,
            updated,
            checksums_generated,
        }
    }

    /// Create generate stats from a task.
    pub fn from_task(task: GenerateTask) -> Self {
        let (_, object, updated, checksums_generated) = task.into_inner();

        Self::new(object.location(), updated, checksums_generated.into())
    }
}

/// Represents stats from a `check` operation.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CheckStats {
    /// The time taken in seconds.
    pub(crate) elapsed_seconds: f64,
    /// Whether the check compared for equality of comparability. Equality ensures that there is
    /// at least one checksum with the same value. Comparability only ensures that there is at
    /// least one checksum that is the same type, but not necessarily that they are the same.
    pub(crate) comparison_type: GroupBy,
    /// The set of compared sums.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) compared: Vec<CheckComparison>,
    /// Comparison groups. Files in the same group are considered equal or comparable depending
    /// on the comparison type.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) groups: Vec<Vec<String>>,
    /// The set of sums that were updated if using `--update`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) updated: Vec<String>,
    /// Any generate stats computed if using `--missing`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) generate_stats: Option<GenerateStats>,
    /// The API errors if there was permission issues for object attributes.
    #[serde(skip_serializing_if = "HashSet::is_empty")]
    pub(crate) api_errors: HashSet<ApiError>,
    /// An unrecoverable error occurred, causing the execution to stop.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unrecoverable_error: Option<Error>,
}

impl From<Error> for Box<CheckStats> {
    fn from(err: Error) -> Self {
        Box::new(CheckStats {
            unrecoverable_error: Some(err),
            ..Default::default()
        })
    }
}

impl From<CheckTaskError> for Box<CheckStats> {
    fn from(err: CheckTaskError) -> Self {
        err.error.into()
    }
}

impl CheckStats {
    /// Create new check stats.
    pub fn new(
        elapsed_seconds: f64,
        comparison_type: GroupBy,
        compared: Vec<CheckComparison>,
        groups: Vec<Vec<String>>,
        updated: Vec<String>,
        generate_stats: Option<GenerateStats>,
        api_errors: HashSet<ApiError>,
    ) -> Self {
        Self {
            elapsed_seconds,
            comparison_type,
            compared,
            groups,
            updated,
            generate_stats,
            api_errors,
            unrecoverable_error: None,
        }
    }

    /// Create check stats from a generate task.
    pub fn from_generate_task(
        group_by: GroupBy,
        generate_stats: GenerateStats,
        elapsed: Duration,
    ) -> Self {
        Self::new(
            elapsed.as_secs_f64(),
            group_by,
            vec![],
            vec![],
            vec![],
            Some(generate_stats),
            Default::default(),
        )
    }

    /// Create check stats from a task.
    pub fn from_task(
        group_by: GroupBy,
        task: CheckTask,
        elapsed: Duration,
        generate_stats: Option<GenerateStats>,
    ) -> Self {
        let (objects, compared, updated, api_errors) = task.into_inner();

        Self::new(
            elapsed.as_secs_f64(),
            group_by,
            compared,
            objects.to_groups(),
            updated,
            generate_stats,
            api_errors,
        )
    }
}

/// Represents stats from a `copy` operation.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CopyStats {
    /// Time taken in seconds.
    pub(crate) elapsed_seconds: f64,
    /// The source of the copy.
    pub(crate) source: String,
    /// The destination of the copy.
    pub(crate) destination: String,
    /// The total bytes transferred to the destination.
    pub(crate) bytes_transferred: u64,
    /// Whether the copy was skipped because the destination already has the file with
    /// matching sums.
    pub(crate) skipped: bool,
    /// Whether the copy occurred because the sums at the destination did not match the source sums.
    /// This will be true if the destination file existed but the sums do not match, thus forcing
    /// a re-copy. It will be false if the destination did not exist in the first place.
    pub(crate) sums_mismatch: bool,
    /// The mode of the copy, either server-side or download-upload.
    pub(crate) copy_mode: CopyMode,
    /// The reason a copy was considered successful. This shows the matching checksum that
    /// determines that the copy completed correctly. If the copy was skipped, this shows the
    /// matching checksum.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<ChecksumPair>,
    /// The number of retries if there was permission issues for copying metadata or tags.
    pub(crate) n_retries: u64,
    /// Stats from checking sums to ensure that the copy was successful.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) check_stats: Option<CheckStats>,
    /// The API errors if there was permission issues for copying metadata or tags.
    #[serde(skip_serializing_if = "HashSet::is_empty")]
    pub(crate) api_errors: HashSet<ApiError>,
    /// An unrecoverable error occurred, causing the execution to stop.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unrecoverable_error: Option<Error>,
}

impl From<Error> for Box<CopyStats> {
    fn from(err: Error) -> Self {
        Box::new(CopyStats {
            unrecoverable_error: Some(err),
            ..Default::default()
        })
    }
}

impl From<CopyTaskError> for Box<CopyStats> {
    fn from(err: CopyTaskError) -> Self {
        err.error.into()
    }
}

impl CopyStats {
    /// Create check stats from a generate task.
    pub fn from_check_stats(
        source: String,
        destination: String,
        copy_mode: CopyMode,
        check_stats: CheckStats,
        elapsed: Duration,
        skipped: bool,
        sums_mismatch: bool,
    ) -> Self {
        Self {
            elapsed_seconds: elapsed.as_secs_f64(),
            source,
            destination,
            bytes_transferred: 0,
            skipped,
            sums_mismatch,
            copy_mode,
            reason: Option::<ChecksumPair>::from(&check_stats),
            n_retries: 0,
            api_errors: Default::default(),
            check_stats: Some(check_stats),
            unrecoverable_error: None,
        }
    }

    /// Create copy stats from a task.
    pub fn from_task(
        copy_task: CopyTask,
        check_stats: Option<CheckStats>,
        elapsed: Duration,
        skipped: bool,
        sums_mismatch: bool,
    ) -> Self {
        Self {
            elapsed_seconds: elapsed.as_secs_f64(),
            source: copy_task.source().format(),
            destination: copy_task.destination().format(),
            bytes_transferred: copy_task.bytes_transferred(),
            skipped,
            sums_mismatch,
            copy_mode: copy_task.copy_mode(),
            reason: check_stats.as_ref().and_then(Option::<ChecksumPair>::from),
            n_retries: copy_task.n_retries(),
            api_errors: copy_task.api_errors(),
            check_stats,
            unrecoverable_error: None,
        }
    }
}

/// The specific comparison that a `check` performed.
#[derive(Serialize, Deserialize, Debug)]
pub struct CheckComparison {
    /// The location of files that were affected by this check.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) locations: Vec<String>,
    /// The reason that the check was successful.
    pub(crate) reason: ChecksumPair,
}

impl CheckComparison {
    /// Create a new check comparison.
    pub fn new(locations: Vec<String>, reason: ChecksumPair) -> Self {
        Self { locations, reason }
    }
}
