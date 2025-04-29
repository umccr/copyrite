//! Structs related to output statistics.
//!

use crate::checksum::file::{Checksum, SumsFile};
use crate::checksum::Ctx;
use crate::cli::CopyMode;
use crate::task::check::{CheckTask, GroupBy};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
pub struct GenerateStats {
    time_taken: Duration,
    stats: GenerateFileStats,
    check_stats: CheckStats,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GenerateFileStats {
    input: String,
    sums_location: String,
    existing_sums: SumsFile,
    output_sums: SumsFile,
    checksums_generated: BTreeMap<Ctx, Checksum>,
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

        CheckStats::new(
            elapsed.as_secs_f64(),
            group_by,
            compared,
            objects.to_groups(),
            updated,
        )
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CopyStats {
    elapsed_seconds: f64,
    source: String,
    destination: String,
    bytes_transferred: u64,
    copy_mode: CopyMode,
    generate_stats: GenerateStats,
    check_stats: CheckStats,
    reason: SumsMatched,
    n_retries: u64,
    api_calls_failed: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CheckComparison {
    left: String,
    right: String,
    reason: Ctx,
}

impl CheckComparison {
    pub fn new(left: String, right: String, reason: Ctx) -> Self {
        Self {
            left,
            right,
            reason,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CopySumsType {
    sums: SumsFile,
    is_metadata_sums: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SumsMatched {
    source_sums: CopySumsType,
    destination_sums: CopySumsType,
    decider: Ctx,
}
