pub mod error;

use clap::{Parser, Subcommand, ValueEnum};
use humantime::Duration;
use std::path::PathBuf;

/// Args for the checksum-cloud CLI.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Checksums to use. Can be specified multiple times or comma-separated.
    /// At least one checksum is required.
    #[arg(value_delimiter = ',', required = true, short, long, env)]
    pub checksums: Vec<Checksum>,

    /// The amount of time to calculate checksums for. Once this timeout is reached the partial
    /// checksum will be saved to the partial checksum file.
    #[arg(global = true, short, long, env)]
    pub timeout: Option<Duration>,

    /// The subcommands for cloud-checksum.
    #[command(subcommand)]
    pub commands: Commands,
}

/// The subcommands for cloud-checksum.
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Generate a checksum.
    Generate {
        /// The input file to calculate the checksum for. By default, accepts standard input.
        #[arg(short, long, env)]
        input: Option<PathBuf>,

        /// The output file to write the checksum to. By default, writes to standard output.
        #[arg(short, long, env)]
        output: Option<PathBuf>,
    },
    /// Check an existing checksum.
    Check {
        /// The input file to check a checksum. By default, accepts standard input.
        #[arg(short, long, env)]
        input: Option<PathBuf>,
    },
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
    /// Calculate the AWS ETag.
    AWSETag,
    /// Calculate a CRC32.
    CRC32,
    /// Calculate the QuickXor checksum.
    QuickXor,
}
