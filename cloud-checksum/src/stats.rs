//! Structs related to output statistics.
//!

use crate::checksum::file::Checksum;
use crate::checksum::Ctx;
use crate::cli::CopyMode;
use crate::task::check::{CheckTask, GroupBy};
use crate::task::copy::CopyTask;
use crate::task::generate::GenerateTask;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// Stats from running a `generate` command.
#[derive(Serialize, Deserialize, Debug)]
pub struct GenerateStats {
    /// Time taken in seconds.
    pub(crate) elapsed_seconds: f64,
    /// The stats for individual file objects.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) stats: Vec<GenerateFileStats>,
    /// Stats from running `check` for comparability when computing sums with `--missing`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) check_stats: Option<Box<CheckStats>>,
}

impl GenerateStats {
    /// Create new generate stats.
    pub fn new(
        elapsed_seconds: f64,
        stats: Vec<GenerateFileStats>,
        check_stats: Option<CheckStats>,
    ) -> Self {
        Self {
            elapsed_seconds,
            stats: stats
                .into_iter()
                .filter(|stat| !stat.checksums_generated.0.is_empty())
                .collect(),
            check_stats: check_stats.map(Box::new),
        }
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
#[derive(Serialize, Deserialize, Debug)]
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
    ) -> Self {
        Self {
            elapsed_seconds,
            comparison_type,
            compared,
            groups,
            updated,
            generate_stats,
        }
    }

    /// Create check stats from a task.
    pub fn from_task(
        group_by: GroupBy,
        task: CheckTask,
        elapsed: Duration,
        generate_stats: Option<GenerateStats>,
    ) -> Self {
        let (objects, compared, updated) = task.into_inner();

        Self::new(
            elapsed.as_secs_f64(),
            group_by,
            compared,
            objects.to_groups(),
            updated,
            generate_stats,
        )
    }
}

/// An API error that could be returned from storage.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ApiError {
    /// The error kind, e.g. `AccessDenied`.
    pub(crate) error: String,
    /// The error message.
    pub(crate) message: String,
}

impl ApiError {
    /// Create a new error.
    pub fn new(error: String, message: String) -> Self {
        Self { error, message }
    }
}

/// Represents stats from a `copy` operation.
#[derive(Serialize, Deserialize, Debug)]
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
    /// The API errors if there was permission issues for copying metadata or tags.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) api_errors: Vec<ApiError>,
    /// Stats from checking sums to ensure that the copy was successful.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) check_stats: Option<CheckStats>,
}

impl CopyStats {
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
            api_errors: copy_task.api_errors().to_vec(),
            check_stats,
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
