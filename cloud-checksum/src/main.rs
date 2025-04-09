use cloud_checksum::checksum::Ctx;
use cloud_checksum::error::{Error, Result};
use cloud_checksum::io::sums::channel::ChannelReader;
use cloud_checksum::task::check::{CheckObjects, CheckOutput, CheckTaskBuilder, GroupBy};
use cloud_checksum::task::copy::{CopyInfo, CopyTaskBuilder};
use cloud_checksum::task::generate::{GenerateTaskBuilder, SumCtxPairs};
use cloud_checksum::{Check, Commands, Copy, Generate, Optimization, Subcommands};
use tokio::io::stdin;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Commands::parse_args()?;

    match args.commands {
        Subcommands::Generate(generate_args) => {
            generate(generate_args, args.optimization).await?;
        }
        Subcommands::Check(check_args) => {
            check(check_args).await?;
        }
        Subcommands::Copy(copy_args) => {
            copy(copy_args, args.optimization).await?;
        }
    };

    Ok(())
}

/// Perform the generate sub command from the args.
async fn generate(generate: Generate, optimization: Optimization) -> Result<()> {
    if generate.input[0] == "-" {
        let reader = ChannelReader::new(stdin(), optimization.channel_capacity);

        let output = GenerateTaskBuilder::default()
            .with_overwrite(generate.force_overwrite)
            .with_verify(generate.verify)
            .with_context(generate.checksum)
            .with_reader(reader)
            .build()
            .await?
            .run()
            .await?
            .to_json_string()?;

        println!("{}", output)
    } else {
        if generate.missing {
            let ctxs = comparable_check(generate.input.clone()).await?;
            let ctxs = SumCtxPairs::from_comparable(ctxs)?;
            if let Some(ctxs) = ctxs {
                for ctx in ctxs.into_inner() {
                    let (input, ctx) = ctx.into_inner();
                    GenerateTaskBuilder::default()
                        .with_overwrite(generate.force_overwrite)
                        .with_verify(generate.verify)
                        .with_input_file_name(input)
                        .with_context(vec![ctx])
                        .with_capacity(optimization.channel_capacity)
                        .write()
                        .build()
                        .await?
                        .run()
                        .await?;
                }
            }
        };

        for input in generate.input {
            GenerateTaskBuilder::default()
                .with_overwrite(generate.force_overwrite)
                .with_verify(generate.verify)
                .with_input_file_name(input)
                .with_context(generate.checksum.clone())
                .with_capacity(optimization.channel_capacity)
                .write()
                .build()
                .await?
                .run()
                .await?;
        }
    }
    Ok(())
}

/// Perform a check for comparability on the input files.
async fn comparable_check(input: Vec<String>) -> Result<CheckObjects> {
    CheckTaskBuilder::default()
        .with_input_files(input)
        .with_group_by(GroupBy::Comparability)
        .build()
        .await?
        .run()
        .await
}

/// Perform the check sub command from the args.
async fn check(check: Check) -> Result<CheckOutput> {
    let files = CheckTaskBuilder::default()
        .with_input_files(check.input)
        .with_group_by(check.group_by)
        .with_update(check.update)
        .build()
        .await?
        .run()
        .await?;
    let output = CheckOutput::from((files, check.group_by));

    println!("{}", output.to_json_string()?);
    Ok(output)
}

/// Perform the copy sub command from the args.
async fn copy(copy: Copy, optimization: Optimization) -> Result<CopyInfo> {
    let result = CopyTaskBuilder::default()
        .with_source(copy.source.to_string())
        .with_destination(copy.destination.to_string())
        .with_metadata_mode(copy.metadata_mode)
        .with_multipart_threshold(copy.multipart_threshold)
        .with_concurrency(copy.concurrency)
        .with_part_size(copy.part_size)
        .with_copy_mode(copy.copy_mode)
        .build()
        .await?
        .run()
        .await?;

    println!("{}", result.to_json_string()?);

    if !copy.no_check {
        let input = vec![copy.source.to_string(), copy.destination.to_string()];
        let ctxs = comparable_check(input.clone()).await?;

        // If the inputs have no checksums to begin with, we need to generate something for
        // the check, so pick the default.
        let checksum = if ctxs.into_inner().is_empty() {
            vec![Ctx::default()]
        } else {
            vec![]
        };

        // First generate missing sums.
        generate(
            Generate {
                input: input.clone(),
                checksum,
                missing: true,
                force_overwrite: false,
                verify: false,
            },
            optimization,
        )
        .await?;

        // Then perform check.
        let result = check(Check {
            input,
            update: true,
            group_by: GroupBy::Equality,
        })
        .await?;

        if result.groups().len() != 1 {
            return Err(Error::CopyError(format!(
                "Copy check failed, the files {} and {} are not identical",
                copy.source, copy.destination
            )));
        }
    }

    Ok(result)
}
