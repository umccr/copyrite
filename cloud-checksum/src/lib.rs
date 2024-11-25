use error::Result;
use std::fmt::{Display, Formatter};

pub mod checksum;
pub mod error;
pub mod reader;
pub mod task;

#[doc(hidden)]
pub mod test;

use crate::checksum::Checksummer;
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
    #[arg(global = true, value_delimiter = ',', short, long)]
    pub checksum: Vec<Checksummer>,

    /// The amount of time to calculate checksums for. Once this timeout is reached the partial
    /// checksum will be saved to the partial checksum file.
    #[arg(global = true, short, long, env)]
    pub timeout: Option<Duration>,

    /// Overwrite the output file. By default, only checksums that are missing are computed and
    /// added to an existing output file. Any existing checksums are preserved (even if not
    /// specified in --checksums). This option allows overwriting any existing output file. This
    /// will recompute all checksums specified.
    #[arg(global = true, short, long, env, conflicts_with = "verify")]
    pub force_overwrite: bool,

    /// Verify the contents of existing output files when generating checksums. By default,
    /// existing checksum files are assumed to contain checksums that have correct values. This
    /// option allows computing existing output file checksums and updating the file to ensure
    /// that it is correct.
    #[arg(global = true, short, long, env, conflicts_with = "force_overwrite")]
    pub verify: bool,

    /// The subcommands for cloud-checksum.
    #[command(subcommand)]
    pub commands: Subcommands,

    /// Commands related to optimizing IO and CPU tasks.
    #[command(flatten)]
    pub optimization: Optimization,
}

/// The subcommands for cloud-checksum.
#[derive(Subcommand, Debug)]
pub enum Subcommands {
    /// Generate a checksum.
    Generate {
        /// The input file to calculate the checksum for. By default, accepts a file name.
        /// use - to accept input from stdin. If using stdin, the output will be written to stdout.
        #[arg(index = 1, required = true)]
        input: String,
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

impl Display for Endianness {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Endianness::LittleEndian => f.write_str("le"),
            Endianness::BigEndian => f.write_str("be"),
        }
    }
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
    #[arg(global = true, short = 's', long, env, default_value_t = 1048576)]
    pub reader_chunk_size: usize,
}
