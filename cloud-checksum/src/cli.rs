//! Cli commands and code.
//!

use crate::checksum::file::SumsFile;
use crate::checksum::Ctx;
use crate::error::Error;
use crate::error::Error::ParseError;
use crate::error::Result;
use crate::io::sums::channel::ChannelReader;
use crate::io::sums::ObjectSumsBuilder;
use crate::io::{default_s3_client, Provider};
use crate::stats::{CheckStats, ChecksumPair, CopyStats, GenerateFileStats, GenerateStats};
use crate::task::check::{CheckTask, CheckTaskBuilder, GroupBy};
use crate::task::copy::CopyTaskBuilder;
use crate::task::generate::{GenerateTaskBuilder, SumCtxPairs};
use aws_sdk_s3::Client;
use clap::{Args, Parser, Subcommand, ValueEnum};
use humantime::Duration;
use parse_size::parse_size;
use serde::{Deserialize, Serialize};
use serde_json::{to_string, to_string_pretty};
use std::ffi::OsString;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::stdin;

/// Args for the checksum-cloud CLI.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about)]
pub struct Command {
    /// The amount of time to calculate checksums for. Once this timeout is reached the partial
    /// checksum will be saved to the partial checksum file.
    #[arg(global = true, short, long, env)]
    pub timeout: Option<Duration>,
    /// The subcommands for cloud-checksum.
    #[command(subcommand)]
    pub commands: Subcommands,
    /// Options related to optimizing IO and CPU tasks.
    #[command(flatten)]
    pub optimization: Optimization,
    /// Options related to outputting data from the CLI.
    #[command(flatten)]
    pub output: Output,
}

impl Command {
    /// Parse args and set default values.
    pub fn parse_args() -> Result<Self> {
        let args = Self::parse();
        Self::validate(&args)?;
        Ok(args)
    }

    /// Parse the command from an iterator.
    pub fn parse_from_iter<I, T>(iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let args = Self::parse_from(iter);
        Self::validate(&args)?;
        Ok(args)
    }

    /// Validate commands.
    pub fn validate(args: &Self) -> Result<()> {
        if let Subcommands::Generate(generate) = &args.commands {
            // For S3 objects, passing no checksums is valid as metadata can be used, otherwise
            // it's an error if not verifying the data.
            if generate.checksum.is_empty()
                && !generate.verify
                && !generate.input.iter().all(|input| {
                    Provider::try_from(input.as_str()).is_ok_and(|provider| provider.is_s3())
                })
            {
                return Err(ParseError(
                    "some checksums must be specified if using file based objects and not verify existing sums".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Execute the command from the args.
    pub async fn execute(self) -> Result<()> {
        let client = Arc::new(default_s3_client().await?);

        let pretty_json = self.output.pretty_json;
        match self.commands {
            Subcommands::Generate(generate_args) => {
                let (sums, stats) = generate_args.generate(self.optimization, client).await?;
                Self::print_stats(sums.as_ref(), false)?;
                Self::print_stats(stats.as_ref(), pretty_json)?;
            }
            Subcommands::Check(check_args) => {
                let output = check_args.check(client).await?;
                Self::print_stats(Some(&output), pretty_json)?;
            }
            Subcommands::Copy(copy_args) => {
                let output = copy_args.copy(self.optimization, client).await?;
                Self::print_stats(Some(&output), pretty_json)?;
            }
        }

        Ok(())
    }

    /// Print output statistics
    pub fn print_stats<T>(stats: Option<&T>, pretty_json: bool) -> Result<()>
    where
        T: Serialize,
    {
        let Some(stats) = stats else { return Ok(()) };

        if pretty_json {
            println!("{}", to_string_pretty(stats)?);
        } else {
            println!("{}", to_string(stats)?);
        }

        Ok(())
    }
}

/// The generate subcommand components.provenance: false
#[derive(Debug, Args)]
pub struct Generate {
    /// The input file to calculate the checksum for. By default, accepts a file name.
    /// use - to accept input from stdin. If using stdin, the output will be written to stdout.
    /// Multiple files can be specified.
    #[arg(value_delimiter = ',', required = true)]
    pub input: Vec<String>,
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
    /// `S3://bucket/object` syntax. This option must be specified for file-based objects. It
    /// does not need to be specified for S3 objects as it will use metadata by default. This
    /// means that if no checksums are specified with S3 objects, the object will not be read
    /// to compute the checksum, and will instead use existing ETags and additional checksums.
    #[arg(value_delimiter = ',', short, long)]
    pub checksum: Vec<Ctx>,
    /// Generate any missing checksums that would be required to confirm whether two files are
    /// identical using the `check` subcommand. Any additional checksums specified using
    /// `--checksum` will also be generated. If there are no checksums preset, the default
    /// checksum is generated.
    #[arg(short, long, env)]
    pub missing: bool,
    /// Overwrite the sums file. By default, only checksums that are missing are computed and
    /// added to an existing sums file. Any existing checksums are preserved (even if not
    /// specified in --checksums). This option allows overwriting any existing sums file. This
    /// will recompute all checksums specified.
    #[arg(short, long, env, conflicts_with = "verify")]
    pub force_overwrite: bool,
    /// Verify the contents of existing sums files when generating checksums. By default,
    /// existing checksum files are assumed to contain checksums that have correct values. This
    /// option allows computing existing sums file checksums and updating the file to ensure
    /// that it is correct. This option will also read objects on S3 to compute checksums, even
    /// if the metadata for that checksum exists.
    #[arg(short, long, env, conflicts_with = "force_overwrite")]
    pub verify: bool,
}

impl Generate {
    /// Perform the generate sub command from the args.
    pub async fn generate(
        self,
        optimization: Optimization,
        client: Arc<Client>,
    ) -> Result<(Option<SumsFile>, Option<GenerateStats>)> {
        if self.input[0] == "-" {
            let reader = ChannelReader::new(stdin(), optimization.channel_capacity);

            let output = GenerateTaskBuilder::default()
                .with_overwrite(self.force_overwrite)
                .with_verify(self.verify)
                .with_context(self.checksum)
                .with_reader(reader)
                .with_client(client)
                .build()
                .await?
                .run()
                .await?
                .into_inner()
                .0;

            Ok((Some(output), None))
        } else {
            let now = Instant::now();
            let mut check_stats = None;
            let mut generate_stats = vec![];

            if self.missing {
                let now = Instant::now();
                let (ctxs, group_by) =
                    Check::comparable_check(self.input.clone(), client.clone()).await?;
                let (objects, compared, updated) = ctxs.into_inner();
                check_stats = Some(CheckStats::new(
                    now.elapsed().as_secs_f64(),
                    group_by,
                    compared,
                    objects.to_groups(),
                    updated,
                ));

                let ctxs = SumCtxPairs::from_comparable(objects)?;
                if let Some(ctxs) = ctxs {
                    for ctx in ctxs.into_inner() {
                        let (input, ctx) = ctx.into_inner();
                        let task = GenerateTaskBuilder::default()
                            .with_overwrite(self.force_overwrite)
                            .with_verify(self.verify)
                            .with_input_file_name(input)
                            .with_context(vec![ctx])
                            .with_capacity(optimization.channel_capacity)
                            .with_client(client.clone())
                            .write()
                            .build()
                            .await?
                            .run()
                            .await?;

                        generate_stats.push(GenerateFileStats::from_task(task));
                    }
                }
            };

            for input in self.input {
                let task = GenerateTaskBuilder::default()
                    .with_overwrite(self.force_overwrite)
                    .with_verify(self.verify)
                    .with_input_file_name(input)
                    .with_context(self.checksum.clone())
                    .with_capacity(optimization.channel_capacity)
                    .with_client(client.clone())
                    .write()
                    .build()
                    .await?
                    .run()
                    .await?;
                generate_stats.push(GenerateFileStats::from_task(task));
            }

            Ok((
                None,
                Some(GenerateStats::new(
                    now.elapsed().as_secs_f64(),
                    generate_stats,
                    check_stats,
                )),
            ))
        }
    }
}

/// The check subcommand components.
#[derive(Debug, Args)]
pub struct Check {
    /// The input file to check a checksum. Requires at least two files.
    #[arg(value_delimiter = ',', required = true, num_args = 2..)]
    pub input: Vec<String>,
    /// Update existing sums files when running the `check` subcommand. This will add checksums to
    /// any sums files that are confirmed to be identical through other sums files.
    #[arg(short, long, env)]
    pub update: bool,
    /// Group outputted checksums by equality or comparability. Equality determines the groups
    /// of sums files that are equal, and comparability determines the groups of sums files
    /// that can be compared, but aren't necessarily equal.
    #[arg(short, long, env, default_value = "equality")]
    pub group_by: GroupBy,
}

impl Check {
    /// Perform a check for comparability on the input files.
    pub async fn comparable_check(
        input: Vec<String>,
        client: Arc<Client>,
    ) -> Result<(CheckTask, GroupBy)> {
        Ok((
            CheckTaskBuilder::default()
                .with_input_files(input)
                .with_group_by(GroupBy::Comparability)
                .with_client(client)
                .build()
                .await?
                .run()
                .await?,
            GroupBy::Comparability,
        ))
    }

    /// Perform the check sub command from the args.
    pub async fn check(self, client: Arc<Client>) -> Result<CheckStats> {
        let now = Instant::now();

        let group_by = self.group_by;
        let check = CheckTaskBuilder::default()
            .with_input_files(self.input)
            .with_group_by(group_by)
            .with_update(self.update)
            .with_client(client)
            .build()
            .await?
            .run()
            .await?;

        Ok(CheckStats::from_task(group_by, check, now.elapsed()))
    }
}

/// The tag mode to use when copying files.
#[derive(Debug, Clone, ValueEnum, Copy, Default)]
pub enum MetadataCopy {
    #[default]
    /// Copy all tags or metadata and fail if it could not be copied.
    Copy,
    /// Do not copy any tags or metadata.
    Suppress,
    /// Attempt to copy tags or metadata but do not fail if it could not be copied.
    BestEffort,
}

impl MetadataCopy {
    /// Is this a copy metadata operation.
    pub fn is_copy(&self) -> bool {
        matches!(self, MetadataCopy::Copy)
    }

    /// Is this a best-effort copy metadata operation.
    pub fn is_best_effort(&self) -> bool {
        matches!(self, MetadataCopy::BestEffort)
    }
}

/// Mode to execute copy task in.
#[derive(Debug, Clone, ValueEnum, Copy, Default, Deserialize, Serialize)]
pub enum CopyMode {
    /// Always use server-side copy operations if they are available. This may still download and
    /// upload if it is not possible to server-side copy.
    #[default]
    ServerSide,
    /// Download the object first and then upload it to the destination.
    DownloadUpload,
}

impl CopyMode {
    /// Is this a download-upload copy operation.
    pub fn is_download_upload(&self) -> bool {
        matches!(self, CopyMode::DownloadUpload)
    }
}

/// The copy subcommand components.
#[derive(Debug, Args)]
pub struct Copy {
    /// The source file to copy from. By default, accepts a file name, use - to accept input from
    /// stdin.
    #[arg(required = true)]
    pub source: String,
    /// The destination to copy files to. If the input contains multiple files, then this must
    /// be a directory.
    #[arg(required = true)]
    pub destination: String,
    /// Controls how tags are copied. By default, this will copy all tags and fail if the tags
    /// could not be copied.
    #[arg(long, env, default_value = "copy")]
    pub tag_mode: MetadataCopy,
    /// Controls how metadata is copied. By default, this will copy all metadata and fail if the
    /// metadata could not be copied.
    #[arg(long, env, default_value = "copy")]
    pub metadata_mode: MetadataCopy,
    #[arg(long, env, default_value = "server-side")]
    pub copy_mode: CopyMode,
    /// The threshold at which a file uses multipart uploads when copying to S3. This can be
    /// specified with a size unit, e.g. 8mib. By default, a multipart copy will occur when the
    /// source file was uploaded using multipart, in order to match sums. This can be used to
    /// override that.
    #[arg(short, long, env, value_parser = |s: &str| parse_size(s))]
    pub multipart_threshold: Option<u64>,
    /// The part size to use when copying files using multipart uploads. This can be specified with
    /// a size unit, e.g. 8mib. By default, the part size will be automatically determined based on
    /// how the source was uploaded. This can be used to override that.
    #[arg(short, long, env, value_parser = |s: &str| parse_size(s))]
    pub part_size: Option<u64>,
    /// The number of simultaneous copy tasks to run when using multipart copies. This controls
    /// how many simultaneous connections are made to copy files.
    #[arg(long, env, default_value_t = 10)]
    pub concurrency: usize,
    /// Do not check the checksums of the copied files after copying. By default, all copy
    /// operations will generate checksums for a check and then verify that the copy was correct.
    #[arg(long, env)]
    pub no_check: bool,
    /// Always perform the copy and do not skip if sums match. By default, a copy is performed only
    /// if the file is not at the destination or if the sums of the source and destination do not
    /// match.
    #[arg(long, env)]
    pub no_skip: bool,
}

impl Copy {
    pub async fn copy_check(
        &self,
        client: Arc<Client>,
        optimization: Optimization,
        verify: bool,
    ) -> Result<(CheckStats, Option<GenerateStats>)> {
        let input = vec![self.source.to_string(), self.destination.to_string()];
        let (ctxs, _) = Check::comparable_check(input.clone(), client.clone()).await?;

        // If the inputs have no checksums to begin with, we need to generate something for
        // the check, so pick the default.
        let checksum = if ctxs.compared_directly().is_empty() {
            vec![Ctx::default()]
        } else {
            vec![]
        };

        // First generate missing sums.
        let (_, generate) = Generate {
            input: input.clone(),
            checksum,
            missing: true,
            force_overwrite: false,
            verify,
        }
        .generate(optimization, client.clone())
        .await?;

        // Then perform check.
        let result = Check {
            input,
            update: true,
            group_by: GroupBy::Equality,
        }
        .check(client)
        .await?;

        Ok((result, generate))
    }

    /// Perform the copy sub command from the args.
    pub async fn copy(self, optimization: Optimization, client: Arc<Client>) -> Result<CopyStats> {
        let now = Instant::now();

        let mut exists = false;
        if !self.no_skip {
            // Check if it exists in the first place.
            let file_size = ObjectSumsBuilder::default()
                .set_client(Some(client.clone()))
                .build(self.destination.to_string())
                .await?
                .file_size()
                .await;

            // If it does exist and the check in the following block fails, there must be a
            // sums mismatch.
            exists = file_size.is_ok_and(|file_size| file_size.is_some());

            if exists {
                let (check_stats, generate_stats) = self
                    .copy_check(client.clone(), optimization.clone(), false)
                    .await?;

                if check_stats.groups.len() == 1 {
                    let copy_stats = CopyStats {
                        elapsed_seconds: now.elapsed().as_secs_f64(),
                        source: self.source,
                        destination: self.destination,
                        bytes_transferred: 0,
                        copy_mode: self.copy_mode,
                        reason: Option::<ChecksumPair>::from(&check_stats),
                        skipped: true,
                        sums_mismatch: false,
                        n_retries: 0,
                        api_errors: vec![],
                        generate_stats,
                        check_stats: Some(check_stats),
                    };
                    return Ok(copy_stats);
                }
            }
        }

        let result = CopyTaskBuilder::default()
            .with_source(self.source.to_string())
            .with_destination(self.destination.to_string())
            .with_metadata_mode(self.metadata_mode)
            .with_multipart_threshold(self.multipart_threshold)
            .with_concurrency(self.concurrency)
            .with_part_size(self.part_size)
            .with_copy_mode(self.copy_mode)
            .with_client(client.clone())
            .build()
            .await?
            .run()
            .await?;

        // If the file existed at the start there must be a sums mismatch.
        let sums_mismatch = exists;
        let copy_stats = if !self.no_check {
            let (check_stats, generate_stats) =
                self.copy_check(client, optimization, sums_mismatch).await?;
            CopyStats::from_task(
                result,
                Some(check_stats),
                generate_stats,
                now.elapsed(),
                false,
                sums_mismatch,
            )
        } else {
            CopyStats::from_task(result, None, None, now.elapsed(), false, sums_mismatch)
        };

        Ok(copy_stats)
    }
}

/// The subcommands for cloud-checksum.
#[derive(Subcommand, Debug)]
pub enum Subcommands {
    /// Generate a checksum.
    Generate(#[arg(flatten)] Generate),
    /// Confirm a set of files is identical. This returns sets of files that are identical.
    /// Which means that more than two files can be checked at the same time.
    Check(#[arg(flatten)] Check),
    /// Copy a file to a location. This command can also simultaneously generate checksums, and
    /// supports all options for generate.
    Copy(#[arg(flatten)] Copy),
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
    /// Calculate a CRC64NVME.
    CRC64NVME,
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
#[derive(Args, Debug, Clone)]
#[group(required = false)]
pub struct Optimization {
    /// The capacity of the sender channel for the channel reader. This controls the
    /// number of elements that can be stored in the reader channel for waiting for checksum
    /// processes to catch up.
    #[arg(global = true, long, env, default_value_t = 100)]
    pub channel_capacity: usize,
    /// The chunk size of the channel reader in bytes. This controls how many bytes are read
    /// by the reader before they are passed into the channel.
    #[arg(global = true, long, env, default_value_t = 1048576)]
    pub reader_chunk_size: usize,
}

/// Options related to outputting information from the CLI.
#[derive(Args, Debug)]
#[group(required = false)]
pub struct Output {
    /// Print the output statistics using indented and multi-line json rather than on a single line.
    #[arg(global = true, long, env)]
    pub pretty_json: bool,
}
