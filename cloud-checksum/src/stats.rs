//! Structs related to output statistics.
//!

use crate::checksum::file::{Checksum, SumsFile};
use crate::checksum::Ctx;
use crate::cli::CopyMode;
use crate::task::check::GroupBy;
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
    time_taken: Duration,
    comparison_type: GroupBy,
    compared: Vec<Vec<String>>,
    groups: Vec<Vec<String>>,
    stats: CheckFileStats,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CheckFileStats {
    input: String,
    sums_location: String,
    existing_sums: SumsFile,
    output_sums: SumsFile,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CopyStats {
    time_taken: Duration,
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
