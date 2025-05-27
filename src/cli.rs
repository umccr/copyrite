//! Cli commands and code.
//!

use crate::checksum::Ctx;
use crate::error::Error;
use crate::error::Error::{CheckError, GenerateError, ParseError};
use crate::error::Result;
use crate::io::sums::channel::ChannelReader;
use crate::io::sums::ObjectSumsBuilder;
use crate::io::{create_s3_client, default_s3_client, Provider};
use crate::stats;
use crate::stats::{CheckStats, ChecksumPair, CopyStats, GenerateStats};
use crate::task::check::{CheckTask, CheckTaskBuilder, GroupBy};
use crate::task::copy::CopyTaskBuilder;
use crate::task::generate::{GenerateTaskBuilder, SumCtxPairs};
use aws_sdk_s3::Client;
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
    #[arg(global = true, short, long, env)]
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

        let credentials = &args.credentials;
        if (credentials.source_credential_provider.is_aws() && credentials.source_profile.is_none())
            || (credentials.destination_credential_provider.is_aws()
                && credentials.destination_profile.is_none())
        {
            return Err(ParseError(
                "a profile must be specified when using the `aws-profile` credential provider"
                    .to_string(),
            ));
        }

        Ok(())
    }

    /// Execute the command from the args.
    pub async fn execute(self) -> Result<()> {
        let now = Instant::now();
        let client = Arc::new(self.credentials.source_client().await?);

        let pretty_json = self.output.pretty_json;
        let write_sums_file = self.output.write_sums_file;
        let ui = self.output.ui;

        match self.commands {
            Subcommands::Generate(generate_args) => {
                let stats = generate_args
                    .generate(self.optimization, &self.credentials, vec![client], true)
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
                        &self.credentials,
                        write_sums_file,
                        false,
                        vec![client],
                    )
                    .await
                    .map_err(|err| Box::new(err.with_elapsed(now.elapsed())))?;

                Self::print_stats(&output, pretty_json, ui)?;
            }
            Subcommands::Copy(copy_args) => {
                let destination_client = Arc::new(self.credentials.destination_client().await?);

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
    /// `--checksum` will also be generated.
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
        credentials: &Credentials,
        mut clients: Vec<Arc<Client>>,
        write_sums_file: bool,
    ) -> stats::Result<GenerateStats> {
        if self.input[0] == "-" {
            let reader = ChannelReader::new(stdin(), optimization.channel_capacity);

            let output = GenerateTaskBuilder::default()
                .with_avoid_get_object_attributes(credentials.avoid_get_object_attributes)
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
                    credentials.avoid_get_object_attributes,
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
                    clients = vec![Arc::new(default_s3_client().await?)];
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
                            .with_avoid_get_object_attributes(
                                credentials.avoid_get_object_attributes,
                            )
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
                    .with_avoid_get_object_attributes(credentials.avoid_get_object_attributes)
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
    /// Update existing sums files when running the `check` subcommand. This will add checksums to
    /// any sums files that are confirmed to be identical through other sums files.
    #[arg(short, long, env)]
    pub update: bool,
    /// Group outputted checksums by equality or comparability. Equality determines the groups
    /// of sums files that are equal, and comparability determines the groups of sums files
    /// that can be compared, but aren't necessarily equal.
    #[arg(short, long, env, default_value = "equality")]
    pub group_by: GroupBy,
    /// Generate missing sums for the check. This is equivalent to `--missing` on the `generate`
    /// command except that it does not write .sums to the input location unless `--write-sums-file`
    /// is also specified.
    #[arg(short, long, env)]
    pub missing: bool,
}

impl Check {
    /// Perform a check for comparability on the input files.
    pub async fn comparable_check(
        input: Vec<String>,
        clients: Vec<Arc<Client>>,
        avoid_get_object_attributes: bool,
    ) -> Result<(CheckTask, GroupBy)> {
        Ok((
            CheckTaskBuilder::default()
                .with_input_files(input)
                .with_group_by(GroupBy::Comparability)
                .with_avoid_get_object_attributes(avoid_get_object_attributes)
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
        credentials: &Credentials,
        write_sums_file: bool,
        verify: bool,
        clients: Vec<Arc<Client>>,
    ) -> stats::Result<CheckStats> {
        let now = Instant::now();
        let group_by = self.group_by;

        let mut builder = CheckTaskBuilder::default()
            .with_group_by(group_by)
            .with_avoid_get_object_attributes(credentials.avoid_get_object_attributes)
            .with_input_files(self.input.clone())
            .with_update(self.update)
            .with_clients(clients.clone());
        let mut generate_stats = None;
        if self.missing {
            let (ctxs, _) = Check::comparable_check(
                self.input.clone(),
                clients.clone(),
                credentials.avoid_get_object_attributes,
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
            .generate(optimization, credentials, clients.clone(), write_sums_file)
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
        source_client: Arc<Client>,
        destination_client: Arc<Client>,
        optimization: Optimization,
        credentials: &Credentials,
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
            credentials,
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
        source_client: Arc<Client>,
        destination_client: Arc<Client>,
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
                .set_client(Some(source_client.clone()))
                .with_avoid_get_object_attributes(credentials.avoid_get_object_attributes)
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
                        &credentials,
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
            .with_avoid_get_object_attributes(credentials.avoid_get_object_attributes)
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
                    &credentials,
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

            if ui {
                if let Some(reason) = Option::<ChecksumPair>::from(&check_stats) {
                    println!(
                        "  {} {} sums match!",
                        style("·").bold(),
                        style(reason.kind).green()
                    );
                }
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
    /// Print output using a UI-mode for copy operations rather than JSON.
    #[arg(global = true, long, env)]
    pub ui: bool,
    /// Write sums files at the location when copying or checking. By default, `copy` operations and
    /// `check` operations with `--missing` will not write any .sums files at the source or
    /// destination.
    #[arg(global = true, long, env)]
    pub write_sums_file: bool,
}

/// Options related to credentials. Options prefixed with `source_` affect `check`, `generate` and
/// the source of a `copy` command. These options also have an alias without the prefix as they are
/// used in all commands. Options prefixed with `destination_` only affect the destination of a
/// `copy` command.
#[derive(Args, Debug)]
#[group(required = false)]
pub struct Credentials {
    /// The credentials source credentials to use. This affects the credentials used for `check`
    /// `generate` and the source of a `copy` operation.
    #[arg(
        global = true,
        long,
        env,
        default_value = "default-environment",
        alias = "credential-provider"
    )]
    pub source_credential_provider: CredentialProvider,
    /// The destination credentials to use. This only affects the credentials used for the
    /// destination of a `copy` operation.
    #[arg(global = true, long, env, default_value = "default-environment")]
    pub destination_credential_provider: CredentialProvider,
    /// The source profile to use if the source credential provider is `aws-profile`.
    /// This must be specified if using `aws-profile`.
    #[arg(global = true, long, env, alias = "profile")]
    pub source_profile: Option<String>,
    /// The destination profile to use if the destination credential provider is `aws-profile`.
    /// This must be specified if using `aws-profile`.
    #[arg(global = true, long, env)]
    pub destination_profile: Option<String>,
    /// Set the region for the source credential provider.
    #[arg(global = true, long, env, alias = "region")]
    pub source_region: Option<String>,
    /// Set the region for the source credential provider.
    #[arg(global = true, long, env)]
    pub destination_region: Option<String>,
    /// Set the source endpoint URL for AWS calls. This allows using a different endpoint that
    /// has an S3-compatible storage API.
    #[arg(global = true, long, env, alias = "endpoint-url")]
    pub source_endpoint_url: Option<String>,
    /// Set the destination endpoint URL for AWS calls. This allows using a different endpoint
    /// that has an S3-compatible storage API.
    #[arg(global = true, long, env)]
    pub destination_endpoint_url: Option<String>,
    /// Avoid `GetObjectAttributes` calls when determining sums. `HeadObject` will be used as a
    /// fallback. `GetObjectAttributes` is preferred over `HeadObject` because it only requires
    /// a single call rather than a call for each part.
    #[arg(global = true, long, env)]
    pub avoid_get_object_attributes: bool,
}

impl Credentials {
    /// Construct the source client from the credentials.
    pub async fn source_client(&self) -> Result<Client> {
        create_s3_client(
            &self.source_credential_provider,
            self.source_profile.as_deref(),
            self.source_region.as_deref(),
            self.source_endpoint_url.as_deref(),
        )
        .await
    }

    /// Construct the destination client from the credentials.
    pub async fn destination_client(&self) -> Result<Client> {
        create_s3_client(
            &self.destination_credential_provider,
            self.destination_profile.as_deref(),
            self.destination_region.as_deref(),
            self.destination_endpoint_url.as_deref(),
        )
        .await
    }

    pub fn is_default(&self) -> bool {
        self.source_credential_provider.is_default()
            && self.destination_credential_provider.is_default()
            && self.source_endpoint_url.is_none()
            && self.destination_endpoint_url.is_none()
    }
}
