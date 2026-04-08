//! Cli commands and code.
//!

use crate::checksum::Ctx;
use crate::error::Error;
use crate::error::Error::{CheckError, GenerateError, ParseError};
use crate::error::Result;
use crate::io::S3Client;
use crate::io::sums::ObjectSumsBuilder;
use crate::io::sums::channel::ChannelReader;
use crate::io::{CredentialOverrides, Provider, create_s3_client, default_s3_client};
use crate::stats;
use crate::stats::{CheckStats, ChecksumPair, CopyStats, GenerateStats};
use crate::task::check::{CheckTask, CheckTaskBuilder, GroupBy};
use crate::task::copy::CopyTaskBuilder;
use crate::task::generate::{GenerateTaskBuilder, SumCtxPairs};
use clap::{Args, Parser, Subcommand, ValueEnum};
use console::style;
use humantime::Duration;
use indicatif::HumanDuration;
use parse_size::parse_size;
use serde::{Deserialize, Serialize};
use serde_json::{to_string, to_string_pretty};
use std::collections::HashSet;
use std::ffi::OsString;
use std::fmt;
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
    #[arg(global = true, short, long, env = "COPYRITE_TIMEOUT")]
    pub timeout: Option<Duration>,
    /// The subcommands for copyrite.
    #[command(subcommand)]
    pub commands: Subcommands,
    /// Options related to optimizing IO and CPU tasks.
    #[command(flatten)]
    pub optimization: Optimization,
    /// Options related to outputting data from the CLI.
    #[command(flatten)]
    pub output: Output,
    /// Options related to credentials.
    #[command(flatten)]
    pub credentials: Credentials,
    /// Options related to S3-compatible storage compatibility.
    #[command(flatten)]
    pub compatibility: Compatibility,
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
            if generate.input[0] == "-" && args.output.ui {
                return Err(ParseError(
                    "cannot use ui mode with an stdout generate command".to_string(),
                ));
            }

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

        if !matches!(args.commands, Subcommands::Copy(_))
            && (args.credentials.has_prefixed_options()
                || args.compatibility.has_prefixed_options())
        {
            return Err(ParseError(
                "source and destination options are only available for the `copy` command, use the unprefixed versions instead (e.g. `--credential-provider`)"
                    .to_string(),
            ));
        }

        Ok(())
    }

    /// Execute the command from the args.
    pub async fn execute(self) -> Result<()> {
        let now = Instant::now();
        let client = self.credentials
            .source_client(&self.compatibility)
            .await?;

        let pretty_json = self.output.pretty_json;
        let write_sums_file = self.output.write_sums_file;
        let ui = self.output.ui;

        match self.commands {
            Subcommands::Generate(generate_args) => {
                let stats = generate_args
                    .generate(
                        self.optimization,
                        vec![client],
                        true,
                    )
                    .await
                    .map_err(|err| Box::new(err.with_elapsed(now.elapsed())))?;
                if let Some(sums) = stats.sums {
                    sums.iter()
                        .try_for_each(|sums| Self::print_stats(&sums, pretty_json, false))?;
                } else {
                    Self::print_stats(&stats, pretty_json, ui)?;
                }
            }
            Subcommands::Check(check_args) => {
                let output = check_args
                    .check(
                        self.optimization,
                        write_sums_file,
                        false,
                        vec![client],
                    )
                    .await
                    .map_err(|err| Box::new(err.with_elapsed(now.elapsed())))?;

                Self::print_stats(&output, pretty_json, ui)?;
            }
            Subcommands::Copy(copy_args) => {
                let destination_client = self.credentials
                    .destination_client(&self.compatibility)
                    .await?;

                let output = copy_args
                    .copy(
                        client,
                        destination_client,
                        self.credentials,
                        self.optimization,
                        write_sums_file,
                        ui,
                    )
                    .await
                    .map_err(|err| Box::new(err.with_elapsed(now.elapsed())))?;

                Self::print_stats(&output, pretty_json, ui)?;
            }
        }

        Ok(())
    }

    /// Print output statistics
    pub fn print_stats<T>(stats: &T, pretty_json: bool, ui: bool) -> Result<()>
    where
        T: Serialize,
    {
        if !ui {
            if pretty_json {
                println!("{}", to_string_pretty(stats)?);
            } else {
                println!("{}", to_string(stats)?);
            }
        }

        Ok(())
    }
}

/// The generate subcommand components.provenance: false
#[derive(Debug, Args)]
pub struct Generate {
    /// The input file to calculate the checksum for.
    ///
    /// By default, accepts a file name. use - to accept input from stdin. If using stdin,
    /// the output will be written to stdout. Multiple files can be specified.
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
    /// identical using the `check` subcommand.
    ///
    /// Any additional checksums specified using `--checksum` will also be generated.
    #[arg(short, long, env = "COPYRITE_MISSING")]
    pub missing: bool,
    /// Overwrite the sums file.
    ///
    /// By default, only checksums that are missing are computed and added to an existing sums
    /// file. Any existing checksums are preserved (even if not specified in --checksums). This
    /// option allows overwriting any existing sums file. This will recompute all checksums
    /// specified.
    #[arg(
        short,
        long,
        env = "COPYRITE_FORCE_OVERWRITE",
        conflicts_with = "verify"
    )]
    pub force_overwrite: bool,
    /// Verify the contents of existing sums files when generating checksums.
    ///
    /// By default, existing checksum files are assumed to contain checksums that have correct
    /// values. This option allows computing existing sums file checksums and updating the file to
    /// ensure that it is correct. This option will also read objects on S3 to compute checksums,
    /// even if the metadata for that checksum exists.
    #[arg(
        short,
        long,
        env = "COPYRITE_VERIFY",
        conflicts_with = "force_overwrite"
    )]
    pub verify: bool,
}

impl Generate {
    /// Perform the generate sub command from the args.
    pub async fn generate(
        self,
        optimization: Optimization,
        mut clients: Vec<S3Client>,
        write_sums_file: bool,
    ) -> stats::Result<GenerateStats> {
        if self.input[0] == "-" {
            let reader = ChannelReader::new(stdin(), optimization.channel_capacity);

            let output = GenerateTaskBuilder::default()
                .with_overwrite(self.force_overwrite)
                .with_verify(self.verify)
                .with_context(self.checksum)
                .with_reader(reader)
                .set_client(clients.first().cloned())
                .build()
                .await?
                .run()
                .await?
                .into_inner()
                .0;

            Ok(GenerateStats::from_sums(vec![(
                self.input[0].to_string(),
                output,
            )]))
        } else {
            let now = Instant::now();
            let mut check_stats = None;
            let mut generate_stats = GenerateStats::default();
            let mut sums_files = vec![];
            let mut errors = HashSet::new();

            if self.missing {
                let now = Instant::now();
                let (ctxs, group_by) = Check::comparable_check(
                    self.input.clone(),
                    clients.clone(),
                )
                .await?;
                let (objects, compared, updated, api_errors) = ctxs.into_inner();
                check_stats = Some(
                    CheckStats::new(
                        group_by,
                        compared,
                        objects.to_groups(),
                        updated,
                        None,
                        api_errors,
                    )
                    .with_elapsed(now.elapsed()),
                );

                if clients.is_empty() {
                    clients = vec![S3Client::new(Arc::new(default_s3_client().await?), false, false)];
                }

                let ctxs = SumCtxPairs::from_comparable(objects)?;
                if let Some(ctxs) = ctxs {
                    for (ctx, client) in ctxs
                        .into_inner()
                        .into_iter()
                        .zip(clients.clone().into_iter().cycle())
                    {
                        let (input, ctx) = ctx.into_inner();
                        let task = GenerateTaskBuilder::default()
                            .with_overwrite(self.force_overwrite)
                            .with_verify(self.verify)
                            .with_input_file_name(input.to_string())
                            .with_context(vec![ctx])
                            .with_capacity(optimization.channel_capacity)
                            .with_client(client)
                            .set_write(write_sums_file)
                            .build()
                            .await?
                            .run()
                            .await;

                        if let Ok(ref task) = task {
                            sums_files.push((input, task.sums_file().clone()));
                            errors.extend(task.api_errors());
                        }
                        generate_stats = generate_stats.add_stats(task)?;
                    }
                }

                if self.checksum.is_empty() {
                    generate_stats.set_check_stats(check_stats);
                    generate_stats.set_recoverable_errors(errors);
                    generate_stats.set_sums_files(sums_files);
                    return Ok(generate_stats);
                }
            };

            for (input, client) in self.input.into_iter().zip(clients.into_iter().cycle()) {
                let task = GenerateTaskBuilder::default()
                    .with_overwrite(self.force_overwrite)
                    .with_verify(self.verify)
                    .with_input_file_name(input.to_string())
                    .with_context(self.checksum.clone())
                    .with_capacity(optimization.channel_capacity)
                    .with_client(client)
                    .set_write(write_sums_file)
                    .build()
                    .await?
                    .run()
                    .await;

                if let Ok(ref task) = task {
                    sums_files.push((input, task.sums_file().clone()));
                    errors.extend(task.api_errors());
                }
                generate_stats = generate_stats.add_stats(task)?;
            }

            generate_stats.set_check_stats(check_stats);
            generate_stats.set_recoverable_errors(errors);
            generate_stats.set_sums_files(sums_files);

            Ok(generate_stats.with_elapsed(now.elapsed()))
        }
    }
}

/// The check subcommand components.
#[derive(Debug, Args)]
pub struct Check {
    /// The input file to check a checksum. Requires at least two files.
    #[arg(value_delimiter = ',', required = true, num_args = 2..)]
    pub input: Vec<String>,
    /// Update existing sums files when running the `check` subcommand.
    ///
    /// This will add checksums to any sums files that are confirmed to be identical through other
    /// sums files.
    #[arg(short, long, env = "COPYRITE_UPDATE")]
    pub update: bool,
    /// Group outputted checksums by equality or comparability.
    ///
    /// Equality determines the groups of sums files that are equal, and comparability determines
    /// the groups of sums files that can be compared, but aren't necessarily equal.
    #[arg(short, long, env = "COPYRITE_GROUP_BY", default_value = "equality")]
    pub group_by: GroupBy,
    /// Generate missing sums for the check.
    ///
    /// This is equivalent to `--missing` on the `generate` command except that it does not write
    /// .sums to the input location unless `--write-sums-file` is also specified.
    #[arg(short, long, env = "COPYRITE_MISSING")]
    pub missing: bool,
}

impl Check {
    /// Perform a check for comparability on the input files.
    pub async fn comparable_check(
        input: Vec<String>,
        clients: Vec<S3Client>,
    ) -> Result<(CheckTask, GroupBy)> {
        Ok((
            CheckTaskBuilder::default()
                .with_input_files(input)
                .with_group_by(GroupBy::Comparability)
                .with_clients(clients)
                .build()
                .await?
                .run()
                .await?,
            GroupBy::Comparability,
        ))
    }

    /// Determine sums to generate based on a comparability check
    fn generate_sums(ctxs: CheckTask) -> Vec<Ctx> {
        if ctxs.is_empty() {
            vec![Ctx::default()]
        } else {
            vec![]
        }
    }

    /// Perform the check sub command from the args.
    pub async fn check(
        self,
        optimization: Optimization,
        write_sums_file: bool,
        verify: bool,
        clients: Vec<S3Client>,
    ) -> stats::Result<CheckStats> {
        let now = Instant::now();
        let group_by = self.group_by;

        let mut builder = CheckTaskBuilder::default()
            .with_group_by(group_by)
            .with_input_files(self.input.clone())
            .with_update(self.update)
            .with_clients(clients.clone());
        let mut generate_stats = None;
        if self.missing {
            let (ctxs, _) = Check::comparable_check(
                self.input.clone(),
                clients.clone(),
            )
            .await?;
            let checksum = Check::generate_sums(ctxs);

            let mut stats = Generate {
                input: self.input.clone(),
                checksum,
                missing: true,
                force_overwrite: false,
                verify,
            }
            .generate(optimization, clients.clone(), write_sums_file)
            .await
            .map_err(|stats| CheckStats::from_generate_task(group_by, *stats))?;
            let sums = stats
                .sums
                .take()
                .ok_or_else(|| GenerateError("missing sums".to_string()))?;
            generate_stats = Some(stats);

            builder = builder.with_sums_files(sums);
        }

        let check = builder.build().await?.run().await?;
        if check.compared_directly().is_empty() {
            return Err(CheckError(
                "nothing to compare in checksums, use `generate` or `--missing` first".to_string(),
            )
            .into());
        }

        Ok(CheckStats::from_task(check, generate_stats).with_elapsed(now.elapsed()))
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

impl Display for CopyMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CopyMode::ServerSide => f.write_str("server-side"),
            CopyMode::DownloadUpload => f.write_str("download-upload"),
        }
    }
}

impl CopyMode {
    /// Is this a download-upload copy operation.
    pub fn is_download_upload(&self) -> bool {
        matches!(self, CopyMode::DownloadUpload)
    }

    /// Is this a server-side copy operation.
    pub fn is_server_side(&self) -> bool {
        matches!(self, CopyMode::ServerSide)
    }
}

/// Details of how to locate credentials or specify no credentials needed
#[derive(Debug, Clone, ValueEnum, Copy, Default, Deserialize, Serialize)]
pub enum CredentialProvider {
    /// Use the default mechanism of the SDK that obtains credentials from the system
    #[default]
    DefaultEnvironment,
    /// Explicitly state that we want to attempt operations using no credentials (no SDK signing)
    NoCredentials,
    /// An AWS profile name.
    AwsProfile,
    /// An AWS Secrets Manager secret containing credentials.
    AwsSecret,
}

impl CredentialProvider {
    /// Is this source of credentials one with no credentials (i.e. anonymous unsigned calls)
    pub fn is_anonymous(&self) -> bool {
        matches!(self, CredentialProvider::NoCredentials)
    }

    /// Is this an aws-profile credential provider.
    pub fn is_aws(&self) -> bool {
        matches!(self, CredentialProvider::AwsProfile)
    }

    /// Is this a default credential provider.
    pub fn is_default(&self) -> bool {
        matches!(self, CredentialProvider::DefaultEnvironment)
    }

    /// Is this an aws-secret credential provider.
    pub fn is_secret(&self) -> bool {
        matches!(self, CredentialProvider::AwsSecret)
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
    /// Controls how tags are copied.
    ///
    /// By default, this will copy all tags and fail if the tags could not be copied.
    #[arg(long, env = "COPYRITE_TAG_MODE", default_value = "copy")]
    pub tag_mode: MetadataCopy,
    /// Controls how metadata is copied.
    ///
    /// By default, this will copy all metadata and fail if the metadata could not be copied.
    #[arg(long, env = "COPYRITE_METADATA_MODE", default_value = "copy")]
    pub metadata_mode: MetadataCopy,
    /// The copy mode.
    ///
    /// By default, this will attempt server-side copy if the source and destination credentials
    /// are the same.
    #[arg(long, env = "COPYRITE_COPY_MODE", default_value = "server-side")]
    pub copy_mode: CopyMode,
    /// The threshold at which a file uses multipart uploads when copying to S3. This can be
    /// specified with a size unit, e.g. 8mib.
    ///
    /// By default, a multipart copy will occur when the source file was uploaded using multipart,
    /// in order to match sums. This can be used to override that.
    #[arg(short, long, env = "COPYRITE_MULTIPART_THRESHOLD", value_parser = |s: &str| parse_size(s))]
    pub multipart_threshold: Option<u64>,
    /// The part size to use when copying files using multipart uploads. This can be specified with
    /// a size unit, e.g. 8mib.
    ///
    /// By default, the part size will be automatically determined based on how the source was
    /// uploaded. This can be used to override that.
    #[arg(short, long, env = "COPYRITE_PART_SIZE", value_parser = |s: &str| parse_size(s))]
    pub part_size: Option<u64>,
    /// The number of simultaneous copy tasks to run when using multipart copies.
    ///
    /// This controls how many simultaneous connections are made to copy files.
    #[arg(long, env = "COPYRITE_CONCURRENCY", default_value_t = 10)]
    pub concurrency: usize,
    /// Do not check the checksums of the copied files after copying.
    ///
    /// By default, all copy operations will generate checksums for a check and then verify that
    /// the copy was correct.
    #[arg(long, env = "COPYRITE_NO_CHECK")]
    pub no_check: bool,
    /// Always perform the copy and do not skip if sums match.
    ///
    /// By default, a copy is performed only if the file is not at the destination or if the sums
    /// of the source and destination do not match.
    #[arg(long, env = "COPYRITE_NO_SKIP")]
    pub no_skip: bool,
}

impl Copy {
    pub async fn copy_check(
        &self,
        source_client: S3Client,
        destination_client: S3Client,
        optimization: Optimization,
        verify: bool,
        write_sums_file: bool,
    ) -> stats::Result<CheckStats> {
        let input = vec![self.source.to_string(), self.destination.to_string()];

        let result = Check {
            input,
            update: write_sums_file,
            group_by: GroupBy::Equality,
            missing: true,
        }
        .check(
            optimization,
            write_sums_file,
            verify,
            vec![source_client, destination_client],
        )
        .await?;

        Ok(result)
    }

    /// Perform the copy sub command from the args.
    pub async fn copy(
        self,
        source_client: S3Client,
        destination_client: S3Client,
        credentials: Credentials,
        optimization: Optimization,
        write_sums_file: bool,
        ui: bool,
    ) -> stats::Result<CopyStats> {
        let now = Instant::now();

        let mut exists = false;
        if !self.no_skip {
            if ui {
                println!("{} Checking before copying...", style("[1/3]").bold().dim(),);
            }

            // Check if it exists in the first place.
            let file_size = ObjectSumsBuilder::default()
                .set_client(Some(destination_client.clone()))
                .build(self.destination.to_string())
                .await?
                .file_size()
                .await;

            // If it does exist and the check in the following block fails, there must be a
            // sums mismatch.
            exists = file_size.is_ok_and(|file_size| file_size.is_some());

            if exists {
                let check_stats = self
                    .copy_check(
                        source_client.clone(),
                        destination_client.clone(),
                        optimization.clone(),
                        false,
                        write_sums_file,
                    )
                    .await
                    .map_err(|err| {
                        CopyStats::from_check_stats(
                            self.source.to_string(),
                            self.destination.to_string(),
                            self.copy_mode,
                            *err,
                            false,
                            false,
                        )
                        .with_elapsed(now.elapsed())
                    })?;

                if check_stats.groups.len() == 1 {
                    let reason = Option::<ChecksumPair>::from(&check_stats);
                    let copy_stats = CopyStats {
                        elapsed_seconds: 0.0,
                        source: self.source,
                        destination: self.destination,
                        bytes_transferred: 0,
                        copy_mode: self.copy_mode,
                        reason: reason.clone(),
                        skipped: true,
                        sums_mismatch: false,
                        n_retries: 0,
                        api_errors: HashSet::new(),
                        check_stats: Some(check_stats),
                        unrecoverable_error: None,
                    };

                    let elapsed = now.elapsed();
                    if ui {
                        if let Some(reason) = reason {
                            println!(
                                "  {} {} sums match, skipping copy!",
                                style("·").bold(),
                                style(reason.kind).green()
                            );
                        }
                        println!("Done in {}", HumanDuration(elapsed));
                    }

                    return Ok(copy_stats.with_elapsed(now.elapsed()));
                }
            }

            if ui {
                if exists {
                    println!(
                        "  {} file exists at source but sums do not match!",
                        style("·").bold(),
                    );
                } else {
                    println!("  {} file does not exist", style("·").bold(),);
                }
            }
        }

        // The copy mode must be download-upload if not using default credential providers.
        let copy_mode = if credentials.is_default() {
            self.copy_mode
        } else {
            CopyMode::DownloadUpload
        };

        let result = CopyTaskBuilder::default()
            .with_source(self.source.to_string())
            .with_destination(self.destination.to_string())
            .with_metadata_mode(self.metadata_mode)
            .with_tag_mode(self.tag_mode)
            .with_multipart_threshold(self.multipart_threshold)
            .with_concurrency(self.concurrency)
            .with_part_size(self.part_size)
            .with_ui(ui)
            .with_copy_mode(copy_mode)
            .with_source_client(source_client.clone())
            .with_destination_client(destination_client.clone())
            .build()
            .await?
            .run()
            .await?;

        // If the file existed at the start there must be a sums mismatch.
        let sums_mismatch = exists;
        let copy_stats = if !self.no_check {
            if ui {
                println!("{} Checking after copying...", style("[3/3]").bold().dim(),);
            }

            let check_stats = self
                .copy_check(
                    source_client,
                    destination_client,
                    optimization,
                    sums_mismatch,
                    write_sums_file,
                )
                .await
                .map_err(|err| {
                    CopyStats::from_check_stats(
                        self.source.to_string(),
                        self.destination.to_string(),
                        self.copy_mode,
                        *err,
                        false,
                        false,
                    )
                    .with_elapsed(now.elapsed())
                })?;

            if ui && let Some(reason) = Option::<ChecksumPair>::from(&check_stats) {
                println!(
                    "  {} {} sums match!",
                    style("·").bold(),
                    style(reason.kind).green()
                );
            }

            CopyStats::from_task(result, Some(check_stats), false, sums_mismatch)
        } else {
            CopyStats::from_task(result, None, false, sums_mismatch)
        };

        let elapsed = now.elapsed();
        if ui {
            println!("Done in {}", HumanDuration(elapsed));
        }

        Ok(copy_stats.with_elapsed(elapsed))
    }
}

/// The subcommands for copyrite.
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
#[command(next_help_heading = "Optimization")]
pub struct Optimization {
    /// The capacity of the sender channel for the channel reader.
    ///
    /// This controls the number of elements that can be stored in the reader channel for waiting
    /// for checksum processes to catch up.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_CHANNEL_CAPACITY",
        default_value_t = 100,
        hide_short_help = true
    )]
    pub channel_capacity: usize,
    /// The chunk size of the channel reader in bytes.
    ///
    /// This controls how many bytes are read by the reader before they are passed into the channel.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_READER_CHUNK_SIZE",
        default_value_t = 1048576,
        hide_short_help = true
    )]
    pub reader_chunk_size: usize,
}

/// Options related to outputting information from the CLI.
#[derive(Args, Debug)]
#[group(required = false)]
#[command(next_help_heading = "Output")]
pub struct Output {
    /// Print the output statistics using indented and multi-line json rather than on a single line.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_PRETTY_JSON",
        hide_short_help = true
    )]
    pub pretty_json: bool,
    /// Print output using a UI-mode for copy operations rather than JSON.
    #[arg(global = true, long, env = "COPYRITE_UI", hide_short_help = true)]
    pub ui: bool,
    /// Write sums files at the location when copying or checking.
    ///
    /// By default, `copy` operations and `check` operations with `--missing` will not write any
    /// .sums files at the source or destination.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_WRITE_SUMS_FILE",
        hide_short_help = true
    )]
    pub write_sums_file: bool,
}

/// Options related to increasing compatibility with S3-compatible storage. For
/// `copy`, options can be prefixed with `source_` or `destination_` to target one side.
/// `generate` and `check` only support the unprefixed version of options. Prefixed
/// options enable additional compatibility for the side, and unprefixed options enable
/// it for both sides.
#[derive(Args, Debug, Clone)]
#[group(required = false)]
#[command(next_help_heading = "Compatibility")]
pub struct Compatibility {
    /// Enable all compatibility options.
    ///
    /// This is a convenience flag that enables `--force-path-style`,
    /// `--no-get-object-attributes`, and `--no-checksum-mode`.
    ///
    /// For the copy command, each compatibility option can also be prefixed with
    /// `source-` or `destination-` to enable additional compatibility per-side (e.g.
    /// `--source-force-path-style`, `--destination-no-checksum-mode`). Unprefixed
    /// options apply to both sides whereas prefixed options enable compatibility for
    /// that side only. Prefixed options are not available for `generate` or `check`.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_S3_COMPATIBLE",
        hide_short_help = true
    )]
    pub s3_compatible: bool,
    /// Use path-style addressing for S3 endpoints.
    ///
    /// By default, the S3 client uses virtual-hosted-style addressing. Some S3-compatible
    /// endpoints such as Ceph require path-style addressing instead.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_FORCE_PATH_STYLE",
        hide_short_help = true
    )]
    pub force_path_style: bool,
    /// Do not use `GetObjectAttributes` calls when determining sums.
    ///
    /// `HeadObject` will be used as a fallback. This is an option because some S3-compatible
    /// endpoints do not support `GetObjectAttributes`. If available, `GetObjectAttributes` is
    /// preferred over `HeadObject` because it only requires a single call rather than a call for
    /// each part.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_NO_GET_OBJECT_ATTRIBUTES",
        hide_short_help = true
    )]
    pub no_get_object_attributes: bool,
    /// Do not use `ChecksumMode::Enabled` on `HeadObject` calls.
    ///
    /// Some S3-compatible endpoints such as Ceph do not support the additional checksums on API
    /// calls like `HeadObject`, which this option disables. This will also force `copyrite` to
    /// only use `ETag`s for verification, and not additional checksums.
    /// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
    #[arg(
        global = true,
        long,
        env = "COPYRITE_NO_CHECKSUM_MODE",
        hide_short_help = true
    )]
    pub no_checksum_mode: bool,
    #[arg(global = true, long, env = "COPYRITE_SOURCE_S3_COMPATIBLE", hide = true)]
    pub source_s3_compatible: bool,
    #[arg(global = true, long, env = "COPYRITE_SOURCE_FORCE_PATH_STYLE", hide = true)]
    pub source_force_path_style: bool,
    #[arg(global = true, long, env = "COPYRITE_SOURCE_NO_GET_OBJECT_ATTRIBUTES", hide = true)]
    pub source_no_get_object_attributes: bool,
    #[arg(global = true, long, env = "COPYRITE_SOURCE_NO_CHECKSUM_MODE", hide = true)]
    pub source_no_checksum_mode: bool,
    #[arg(global = true, long, env = "COPYRITE_DESTINATION_S3_COMPATIBLE", hide = true)]
    pub destination_s3_compatible: bool,
    #[arg(global = true, long, env = "COPYRITE_DESTINATION_FORCE_PATH_STYLE", hide = true)]
    pub destination_force_path_style: bool,
    #[arg(global = true, long, env = "COPYRITE_DESTINATION_NO_GET_OBJECT_ATTRIBUTES", hide = true)]
    pub destination_no_get_object_attributes: bool,
    #[arg(global = true, long, env = "COPYRITE_DESTINATION_NO_CHECKSUM_MODE", hide = true)]
    pub destination_no_checksum_mode: bool,
}

impl Compatibility {
    /// Whether to force path-style addressing.
    pub fn force_path_style(&self) -> bool {
        self.s3_compatible || self.force_path_style
    }

    /// Whether to avoid `GetObjectAttributes` calls.
    pub fn no_get_object_attributes(&self) -> bool {
        self.s3_compatible || self.no_get_object_attributes
    }

    /// Whether to disable checksum mode.
    pub fn no_checksum_mode(&self) -> bool {
        self.s3_compatible || self.no_checksum_mode
    }

    /// Whether to force path-style addressing for the source.
    pub fn source_force_path_style(&self) -> bool {
        self.source_s3_compatible
            || self.source_force_path_style
            || self.force_path_style()
    }

    /// Whether to force path-style addressing for the destination.
    pub fn destination_force_path_style(&self) -> bool {
        self.destination_s3_compatible
            || self.destination_force_path_style
            || self.force_path_style()
    }

    /// Whether to avoid `GetObjectAttributes` calls for the source.
    pub fn source_no_get_object_attributes(&self) -> bool {
        self.source_s3_compatible
            || self.source_no_get_object_attributes
            || self.no_get_object_attributes()
    }

    /// Whether to avoid `GetObjectAttributes` calls for the destination.
    pub fn destination_no_get_object_attributes(&self) -> bool {
        self.destination_s3_compatible
            || self.destination_no_get_object_attributes
            || self.no_get_object_attributes()
    }

    /// Whether to disable checksum mode for the source.
    pub fn source_no_checksum_mode(&self) -> bool {
        self.source_s3_compatible
            || self.source_no_checksum_mode
            || self.no_checksum_mode()
    }

    /// Whether to disable checksum mode for the destination.
    pub fn destination_no_checksum_mode(&self) -> bool {
        self.destination_s3_compatible
            || self.destination_no_checksum_mode
            || self.no_checksum_mode()
    }

    /// Check if any source or destination specific options are set.
    pub fn has_prefixed_options(&self) -> bool {
        self.source_s3_compatible
            || self.source_force_path_style
            || self.source_no_get_object_attributes
            || self.source_no_checksum_mode
            || self.destination_s3_compatible
            || self.destination_force_path_style
            || self.destination_no_get_object_attributes
            || self.destination_no_checksum_mode
    }

}

/// Options related to credentials. Unprefixed options apply to both source and destination. For
/// `copy`, options can be prefixed with `source_` or `destination_` to target one side. `generate`
/// and `check` only support the unprefixed version of options. Prefixed options take precedence
/// over unprefixed options in copies.
#[derive(Args, Debug)]
#[group(required = false)]
#[command(next_help_heading = "Credentials")]
pub struct Credentials {
    /// The credential provider to use. Defaults to `default-environment` if not specified.
    ///
    /// For the copy command, each credential option can also be prefixed with
    /// `source-` or `destination-` to target one side independently (e.g.
    /// `--source-credential-provider`, `--destination-region`). When both prefixed and
    /// unprefixed versions are specified, the prefixed version takes precedence.
    /// Prefixed options are not available for `generate` or `check`.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_CREDENTIAL_PROVIDER",
        requires_if("aws-profile", "profile"),
        requires_if("aws-secret", "secret"),
        hide_short_help = true
    )]
    pub credential_provider: Option<CredentialProvider>,
    /// The profile to use if the credential provider is `aws-profile`.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_PROFILE",

        hide_short_help = true
    )]
    pub profile: Option<String>,
    /// The secret name or ARN to use if the credential provider is `aws-secret`.
    ///
    /// The secret must be a JSON object, with only `access_key_id` and `secret_access_key`
    /// being required:
    ///
    ///   {
    ///     "access_key_id": "...",
    ///     "secret_access_key": "...",
    ///     "session_token": "..."
    ///   }
    ///
    /// The `session_token` is optional.
    ///
    /// The default credential chain is used to authenticate with Secrets Manager.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_SECRET",

        hide_short_help = true,
        verbatim_doc_comment
    )]
    pub secret: Option<String>,
    /// Set the region for the credential provider.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_REGION",

        hide_short_help = true
    )]
    pub region: Option<String>,
    /// Set the endpoint URL for AWS calls. This allows using a different endpoint that has an
    /// S3-compatible storage API.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_ENDPOINT_URL",

        hide_short_help = true
    )]
    pub endpoint_url: Option<String>,
    /// The AWS access key ID. Overrides the value from the selected credential provider.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_ACCESS_KEY_ID",

        hide_short_help = true
    )]
    pub access_key_id: Option<String>,
    /// The AWS secret access key. Overrides the value from the selected credential provider.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_SECRET_ACCESS_KEY",

        hide_short_help = true
    )]
    pub secret_access_key: Option<String>,
    /// The AWS session token. Overrides the value from the selected credential provider.
    #[arg(
        global = true,
        long,
        env = "COPYRITE_SESSION_TOKEN",

        hide_short_help = true
    )]
    pub session_token: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_SOURCE_CREDENTIAL_PROVIDER",
        requires_if("aws-profile", "source_profile"),
        requires_if("aws-secret", "source_secret"),
        hide = true
    )]
    pub source_credential_provider: Option<CredentialProvider>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_SOURCE_PROFILE",
        hide = true
    )]
    pub source_profile: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_SOURCE_SECRET",
        hide = true
    )]
    pub source_secret: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_SOURCE_REGION",
        hide = true
    )]
    pub source_region: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_SOURCE_ENDPOINT_URL",
        hide = true
    )]
    pub source_endpoint_url: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_SOURCE_ACCESS_KEY_ID",
        hide = true
    )]
    pub source_access_key_id: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_SOURCE_SECRET_ACCESS_KEY",
        hide = true
    )]
    pub source_secret_access_key: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_SOURCE_SESSION_TOKEN",
        hide = true
    )]
    pub source_session_token: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_DESTINATION_CREDENTIAL_PROVIDER",
        requires_if("aws-profile", "destination_profile"),
        requires_if("aws-secret", "destination_secret"),
        hide = true
    )]
    pub destination_credential_provider: Option<CredentialProvider>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_DESTINATION_PROFILE",
        hide = true
    )]
    pub destination_profile: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_DESTINATION_SECRET",
        hide = true
    )]
    pub destination_secret: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_DESTINATION_REGION",
        hide = true
    )]
    pub destination_region: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_DESTINATION_ENDPOINT_URL",
        hide = true
    )]
    pub destination_endpoint_url: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_DESTINATION_ACCESS_KEY_ID",
        hide = true
    )]
    pub destination_access_key_id: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_DESTINATION_SECRET_ACCESS_KEY",
        hide = true
    )]
    pub destination_secret_access_key: Option<String>,
    #[arg(
        global = true,
        long,
        env = "COPYRITE_DESTINATION_SESSION_TOKEN",
        hide = true
    )]
    pub destination_session_token: Option<String>,
}

impl Credentials {
    /// Resolve the effective source credential provider.
    fn effective_source_credential_provider(&self) -> CredentialProvider {
        self.source_credential_provider
            .or(self.credential_provider)
            .unwrap_or_default()
    }

    /// Resolve the effective destination credential provider.
    fn effective_destination_credential_provider(&self) -> CredentialProvider {
        self.destination_credential_provider
            .or(self.credential_provider)
            .unwrap_or_default()
    }

    /// Resolve the effective source profile.
    fn effective_source_profile(&self) -> Option<&str> {
        self.source_profile
            .as_deref()
            .or(self.profile.as_deref())
    }

    /// Resolve the effective destination profile.
    fn effective_destination_profile(&self) -> Option<&str> {
        self.destination_profile
            .as_deref()
            .or(self.profile.as_deref())
    }

    /// Resolve the effective source secret.
    fn effective_source_secret(&self) -> Option<&str> {
        self.source_secret
            .as_deref()
            .or(self.secret.as_deref())
    }

    /// Resolve the effective destination secret.
    fn effective_destination_secret(&self) -> Option<&str> {
        self.destination_secret
            .as_deref()
            .or(self.secret.as_deref())
    }

    /// Resolve the effective source region.
    fn effective_source_region(&self) -> Option<&str> {
        self.source_region
            .as_deref()
            .or(self.region.as_deref())
    }

    /// Resolve the effective destination region.
    fn effective_destination_region(&self) -> Option<&str> {
        self.destination_region
            .as_deref()
            .or(self.region.as_deref())
    }

    /// Resolve the effective source endpoint URL.
    fn effective_source_endpoint_url(&self) -> Option<&str> {
        self.source_endpoint_url
            .as_deref()
            .or(self.endpoint_url.as_deref())
    }

    /// Resolve the effective destination endpoint URL.
    fn effective_destination_endpoint_url(&self) -> Option<&str> {
        self.destination_endpoint_url
            .as_deref()
            .or(self.endpoint_url.as_deref())
    }

    /// Construct the source client from the credentials.
    pub async fn source_client(&self, compatibility: &Compatibility) -> Result<S3Client> {
        let client = create_s3_client(
            &self.effective_source_credential_provider(),
            self.effective_source_profile(),
            self.effective_source_region(),
            self.effective_source_endpoint_url(),
            self.effective_source_secret(),
            self.source_overrides(),
            compatibility.source_force_path_style(),
        )
        .await?;
        Ok(S3Client::new(
            Arc::new(client),
            compatibility.source_no_get_object_attributes(),
            compatibility.source_no_checksum_mode(),
        ))
    }

    /// Construct the destination client from the credentials.
    pub async fn destination_client(&self, compatibility: &Compatibility) -> Result<S3Client> {
        let client = create_s3_client(
            &self.effective_destination_credential_provider(),
            self.effective_destination_profile(),
            self.effective_destination_region(),
            self.effective_destination_endpoint_url(),
            self.effective_destination_secret(),
            self.destination_overrides(),
            compatibility.destination_force_path_style(),
        )
        .await?;
        Ok(S3Client::new(
            Arc::new(client),
            compatibility.destination_no_get_object_attributes(),
            compatibility.destination_no_checksum_mode(),
        ))
    }

    /// Check if the default credentials are being used without any overrides.
    pub fn is_default(&self) -> bool {
        self.effective_source_credential_provider().is_default()
            && self.effective_destination_credential_provider().is_default()
            && self.effective_source_endpoint_url().is_none()
            && self.effective_destination_endpoint_url().is_none()
            && !self.source_overrides().any()
            && !self.destination_overrides().any()
    }

    /// Check if any source or destination specific options are set.
    pub fn has_prefixed_options(&self) -> bool {
        self.source_credential_provider.is_some()
            || self.destination_credential_provider.is_some()
            || self.source_profile.is_some()
            || self.destination_profile.is_some()
            || self.source_secret.is_some()
            || self.destination_secret.is_some()
            || self.source_region.is_some()
            || self.destination_region.is_some()
            || self.source_endpoint_url.is_some()
            || self.destination_endpoint_url.is_some()
            || self.source_access_key_id.is_some()
            || self.destination_access_key_id.is_some()
            || self.source_secret_access_key.is_some()
            || self.destination_secret_access_key.is_some()
            || self.source_session_token.is_some()
            || self.destination_session_token.is_some()
    }

    fn source_overrides(&self) -> CredentialOverrides {
        CredentialOverrides::new(
            self.source_access_key_id
                .clone()
                .or(self.access_key_id.clone()),
            self.source_secret_access_key
                .clone()
                .or(self.secret_access_key.clone()),
            self.source_session_token
                .clone()
                .or(self.session_token.clone()),
        )
    }

    fn destination_overrides(&self) -> CredentialOverrides {
        CredentialOverrides::new(
            self.destination_access_key_id
                .clone()
                .or(self.access_key_id.clone()),
            self.destination_secret_access_key
                .clone()
                .or(self.secret_access_key.clone()),
            self.destination_session_token
                .clone()
                .or(self.session_token.clone()),
        )
    }
}
