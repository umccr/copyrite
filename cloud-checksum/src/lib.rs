use error::Result;

pub mod checksum;
pub mod error;
pub mod reader;
pub mod task;

#[doc(hidden)]
pub mod test;

use crate::checksum::ChecksumCtx;
use crate::error::Error;
use crate::error::Error::ParseError;
use clap::{Args, Parser, Subcommand, ValueEnum};
use humantime::Duration;
use std::path::PathBuf;
use std::str::FromStr;

/// Args for the checksum-cloud CLI.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about)]
pub struct Commands {
    /// Checksums to use. Can be specified multiple times or comma-separated.
    /// At least one checksum is required.
    #[arg(value_delimiter = ',', required = true, short, long)]
    pub checksum: Vec<ChecksumCtx>,

    /// The amount of time to calculate checksums for. Once this timeout is reached the partial
    /// checksum will be saved to the partial checksum file.
    #[arg(global = true, short, long, env)]
    pub timeout: Option<Duration>,

    /// The chunk sizes to compute for AWS etags. Specify multiple chunk sizes to compute multiple
    /// etags. The default computes an etag with an 8MiB chunk size.
    #[arg(global = true, value_delimiter = ',', value_parser = parse_size, long, env, default_value = "8mib")]
    pub aws_etag_chunk_sizes: Vec<u64>,

    /// The subcommands for cloud-checksum.
    #[command(subcommand)]
    pub commands: Subcommands,

    /// Commands related to optimizing IO and CPU tasks.
    #[command(flatten)]
    pub optimization: Optimization,
}

fn parse_size(s: &str) -> Result<u64> {
    parse_size::parse_size(s).map_err(|err| ParseError(err.to_string()))
}

/// The subcommands for cloud-checksum.
#[derive(Subcommand, Debug)]
pub enum Subcommands {
    /// Generate a checksum.
    Generate {
        /// The input file to calculate the checksum for. By default, accepts standard input.
        #[arg(short, long)]
        input: Option<PathBuf>,

        /// The output file to write the checksum to. By default, writes to standard output.
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
    /// Confirm a set of files is identical.
    Check {
        /// The input file to check a checksum. Requires at least two files.
        #[arg(value_delimiter = ',', required = true, num_args = 2, short, long)]
        files: Vec<PathBuf>,
    },
}

/// The checksum to use.
#[derive(Debug, Clone, ValueEnum, PartialEq, Eq, PartialOrd, Ord, Copy)]
pub enum Checksum {
    /// Calculate the MD5 checksum.
    MD5,
    /// Calculate the SHA1 checksum.
    SHA1,
    /// Calculate the SHA256 checksum.
    SHA256,
    /// Calculate the AWS ETag.
    #[value(name = "aws-etag")]
    AWSETag,
    /// Calculate a CRC32.
    CRC32,
    /// Calculate a CRC32C.
    CRC32C,
    /// Calculate the QuickXor checksum.
    QuickXor,
}

impl FromStr for Checksum {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        <Checksum as ValueEnum>::from_str(s, true).map_err(ParseError)
    }
}

/// The endianness to use for CRC-based checksums.
#[derive(Debug, Clone, ValueEnum, PartialEq, Eq, PartialOrd, Ord, Copy)]
pub enum Endianness {
    /// Use little-endian representation.
    LittleEndian,
    /// Use big-endian representation.
    BigEndian,
}

/// Commands related to optimizing IO and CPU tasks.
#[derive(Args, Debug)]
#[group(required = false)]
pub struct Optimization {
    /// The capacity of the sender channel for the channel reader. This controls the
    /// number of elements that can be stored in the reader channel for waiting for checksum
    /// processes to catch up.
    #[arg(global = true, short = 'p', long, env, default_value_t = 100)]
    pub channel_capacity: usize,
    /// The chunk size of the channel reader in bytes. This controls how many bytes are read
    /// by the reader before they are passed into the channel.
    #[arg(global = true, short = 's', long, env, default_value_t = 10048576)]
    pub reader_chunk_size: usize,
}
