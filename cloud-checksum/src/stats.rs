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

#[derive(Serialize, Deserialize, Debug)]
pub struct GenerateStats {
    pub(crate) elapsed_seconds: f64,
    pub(crate) stats: Vec<GenerateFileStats>,
    pub(crate) check_stats: Option<CheckStats>,
}

impl GenerateStats {
    pub fn new(
        elapsed_seconds: f64,
        stats: Vec<GenerateFileStats>,
        check_stats: Option<CheckStats>,
    ) -> Self {
        Self {
            elapsed_seconds,
            stats,
            check_stats,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ChecksumPair {
    pub(crate) kind: Ctx,
    pub(crate) value: Checksum,
}

impl ChecksumPair {
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

#[derive(Serialize, Deserialize, Debug)]
pub struct GenerateFileStats {
    pub(crate) input: String,
    pub(crate) updated: Vec<String>,
    pub(crate) checksums_generated: ChecksumStats,
}

impl GenerateFileStats {
    pub fn new(input: String, updated: Vec<String>, checksums_generated: ChecksumStats) -> Self {
        Self {
            input,
            updated,
            checksums_generated,
        }
    }

    pub fn from_task(task: GenerateTask) -> Self {
        let (_, object, updated, checksums_generated) = task.into_inner();

        Self::new(object.location(), updated, checksums_generated.into())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CheckStats {
    pub(crate) elapsed_seconds: f64,
    pub(crate) comparison_type: GroupBy,
    pub(crate) compared: Vec<CheckComparison>,
    pub(crate) groups: Vec<Vec<String>>,
    pub(crate) updated: Vec<String>,
}

impl CheckStats {
    pub fn new(
        elapsed_seconds: f64,
        comparison_type: GroupBy,
        compared: Vec<CheckComparison>,
        groups: Vec<Vec<String>>,
        updated: Vec<String>,
    ) -> Self {
        Self {
            elapsed_seconds,
            comparison_type,
            compared,
            groups,
            updated,
        }
    }

    pub fn from_task(group_by: GroupBy, task: CheckTask, elapsed: Duration) -> Self {
        let (objects, compared, updated) = task.into_inner();

        Self::new(
            elapsed.as_secs_f64(),
            group_by,
            compared,
            objects.to_groups(),
            updated,
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ApiError {
    pub(crate) error: String,
    pub(crate) message: String,
}

impl ApiError {
    pub fn new(error: String, message: String) -> Self {
        Self { error, message }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CopyStats {
    pub(crate) elapsed_seconds: f64,
    pub(crate) source: String,
    pub(crate) destination: String,
    pub(crate) bytes_transferred: u64,
    pub(crate) skipped: bool,
    pub(crate) copy_mode: CopyMode,
    pub(crate) reason: Option<ChecksumPair>,
    pub(crate) n_retries: u64,
    pub(crate) api_errors: Vec<ApiError>,
    pub(crate) generate_stats: Option<GenerateStats>,
    pub(crate) check_stats: Option<CheckStats>,
}

impl CopyStats {
    pub fn from_task(
        copy_task: CopyTask,
        check_stats: Option<CheckStats>,
        generate_stats: Option<GenerateStats>,
        elapsed: Duration,
        skipped: bool,
    ) -> Self {
        Self {
            elapsed_seconds: elapsed.as_secs_f64(),
            source: copy_task.source().format(),
            destination: copy_task.destination().format(),
            bytes_transferred: copy_task.bytes_transferred(),
            skipped,
            copy_mode: copy_task.copy_mode(),
            reason: check_stats.as_ref().and_then(Option::<ChecksumPair>::from),
            n_retries: copy_task.n_retries(),
            api_errors: copy_task.api_errors().to_vec(),
            generate_stats,
            check_stats,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CheckComparison {
    pub(crate) locations: Vec<String>,
    pub(crate) reason: ChecksumPair,
}

impl CheckComparison {
    pub fn new(locations: Vec<String>, reason: ChecksumPair) -> Self {
        Self { locations, reason }
    }
}
