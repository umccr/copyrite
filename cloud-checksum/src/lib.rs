use error::Result;
use std::fmt::{Display, Formatter};

pub mod checksum;
pub mod cloud;
pub mod error;
pub mod reader;
pub mod task;

#[doc(hidden)]
pub mod test;

use crate::checksum::Ctx;
use crate::error::Error;
use crate::error::Error::ParseError;
use crate::task::check::GroupBy;
use clap::{Args, Parser, Subcommand, ValueEnum};
use humantime::Duration;
use std::str::FromStr;

/// Args for the checksum-cloud CLI.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about)]
pub struct Commands {
    /// The amount of time to calculate checksums for. Once this timeout is reached the partial
    /// checksum will be saved to the partial checksum file.
    #[arg(global = true, short, long, env)]
    pub timeout: Option<Duration>,

    /// The subcommands for cloud-checksum.
    #[command(subcommand)]
    pub commands: Subcommands,

    /// Commands related to optimizing IO and CPU tasks.
    #[command(flatten)]
    pub optimization: Optimization,
}

impl Commands {
    /// Parse args and set default values.
    pub fn parse_args() -> Result<Self> {
        let mut args = Self::parse();
        if let Subcommands::Generate {
            _is_checksum_defaulted: is_checksum_defaulted,
            checksum,
            ..
        } = &mut args.commands
        {
            // This checks to see if something was passed on the command line for the checksum or
            // if the default value has been used.
            if checksum.is_empty() {
                *is_checksum_defaulted = true;
                checksum.push(Ctx::from_str("md5")?);
            }
        }
        Ok(args)
    }
}

/// The subcommands for cloud-checksum.
#[derive(Subcommand, Debug)]
pub enum Subcommands {
    /// Generate a checksum.
    Generate {
        /// The input file to calculate the checksum for. By default, accepts a file name.
        /// use - to accept input from stdin. If using stdin, the output will be written to stdout.
        /// Multiple files can be specified.
        #[arg(value_delimiter = ',', required = true)]
        input: Vec<String>,
        /// Checksums to use. Can be specified multiple times or comma-separated.
        ///
        /// Use an `aws-<part_size>` suffix to create AWS ETag-style checksums, e.g. `md5-aws-8mib`.
        /// `<part_size>` should contain a size unit, e.g. `mib` or `b`. When the unit is omitted,
        /// this is interpreted as a `<part-number>` where the input file is split evenly into the
        /// number of parts (where the last part can be smaller). For example `md5-aws-10` splits
        /// the file into 10 parts. `<part-number>` is not supported when the file size is not
        /// known, such as when taking input from stdin.
        ///
        /// It is possible to specify different part sizes by appending additional parts separated
        /// by a `-`. In this case, if the file is bigger than the number of parts, the last part
        /// will be repeated until the end. If it is smaller, then some parts may be ignored. For
        /// example, `md5-aws-8mib-16mib` will create one 8 MiB part and the rest will be 16 MiB
        /// parts.
        ///
        /// This option supports file-based objects and objects in S3 by using the
        /// `S3://bucket/object` syntax. Defaults to `md5` for file-based objects if unspecified
        /// and whatever is available in S3 metadata for S3 objects. This means that is no checksums
        /// are specified with S3 objects, the object will not be read to compute the checksum, and
        /// will instead use existing ETags and additional checksums.
        #[arg(value_delimiter = ',', short, long)]
        checksum: Vec<Ctx>,
        /// Generate any missing checksums that would be required to confirm whether two files are
        /// identical using the `check` subcommand. Any additional checksums specified using
        /// `--checksum` will also be generated. If there are no checksums preset, the default
        /// checksum is generated.
        #[arg(short, long, env)]
        missing: bool,
        /// Overwrite the sums file. By default, only checksums that are missing are computed and
        /// added to an existing sums file. Any existing checksums are preserved (even if not
        /// specified in --checksums). This option allows overwriting any existing sums file. This
        /// will recompute all checksums specified. This option will also read objects on S3 to
        /// compute checksums, even if the metadata for that checksum exists.
        #[arg(short, long, env, conflicts_with = "verify")]
        force_overwrite: bool,
        /// Verify the contents of existing sums files when generating checksums. By default,
        /// existing checksum files are assumed to contain checksums that have correct values. This
        /// option allows computing existing sums file checksums and updating the file to ensure
        /// that it is correct. This option will also read objects on S3 to compute checksums, even
        /// if the metadata for that checksum exists.
        #[arg(short, long, env, conflicts_with = "force_overwrite")]
        verify: bool,
        /// Not visible to user, used for default checksum logic.
        #[clap(skip)]
        _is_checksum_defaulted: bool,
    },
    /// Confirm a set of files is identical. This returns sets of files that are identical.
    /// Which means that more than two files can be checked at the same time.
    Check {
        /// The input file to check a checksum. Requires at least two files.
        #[arg(value_delimiter = ',', required = true, num_args = 2)]
        input: Vec<String>,
        /// Update existing sums files when running the `check` subcommand. This will add checksums to
        /// any sums files that are confirmed to be identical through other sums files.
        #[arg(short, long, env)]
        update: bool,
        /// Group outputted checksums by equality or comparability. Equality determines the groups
        /// of sums files that are equal, and comparability determines the groups of sums files
        /// that can be compared, but aren't necessarily equal.
        #[arg(short, long, env, default_value = "equality")]
        group_by: GroupBy,
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
#[derive(Debug, Clone, ValueEnum, PartialEq, Eq, PartialOrd, Ord, Copy, Hash)]
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
