pub mod error;

use clap::{Parser, ValueEnum};
use std::path::PathBuf;

/// Args for the checksum-cloud CLI.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Checksums to use. Can be specified multiple times or comma-separated.
    /// At least one checksum is required.
    #[arg(value_delimiter = ',', required = true, short, long, env)]
    pub checksums: Vec<Checksum>,

    /// The input file to calculate the checksum for.
    #[arg(short, long, env)]
    pub input: PathBuf,

    /// The output file to write the checksum to.
    #[arg(short, long, env)]
    pub output: Option<PathBuf>,
}

/// The checksum to use.
#[derive(Debug, Clone, ValueEnum, PartialEq, Eq, PartialOrd, Ord)]
pub enum Checksum {
    /// Calculate the MD5 checksum.
    MD5,
    /// Calculate the SHA1 checksum.
    SHA1,
    /// Calculate the SHA256 checksum.
    SHA256,
}
