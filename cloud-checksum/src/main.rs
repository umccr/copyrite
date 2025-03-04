use cloud_checksum::checksum::file::SumsFile;
use cloud_checksum::checksum::Ctx;
use cloud_checksum::error::Result;
use cloud_checksum::reader::channel::ChannelReader;
use cloud_checksum::task::check::{CheckOutput, CheckTaskBuilder, GroupBy};
use cloud_checksum::task::generate::{GenerateTaskBuilder, SumCtxPairs};
use cloud_checksum::{Commands, Subcommands};
use tokio::io::stdin;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Commands::parse_args()?;

    match args.commands {
        Subcommands::Generate {
            input,
            checksum,
            missing: generate_missing,
            force_overwrite,
            verify,
            _is_checksum_defaulted,
        } => {
            if input[0] == "-" {
                let reader = ChannelReader::new(stdin(), args.optimization.channel_capacity);

                let output = GenerateTaskBuilder::default()
                    .with_overwrite(force_overwrite)
                    .with_verify(verify)
                    .with_context(checksum)
                    .with_reader(reader)
                    .build()
                    .await?
                    .run()
                    .await?
                    .to_json_string()?;

                println!("{}", output)
            } else {
                if generate_missing {
                    let ctxs = CheckTaskBuilder::default()
                        .with_input_files(input.clone())
                        .with_group_by(GroupBy::Comparability)
                        .build()
                        .await?
                        .run()
                        .await?;

                    let ctxs = SumCtxPairs::from_comparable(ctxs)?;
                    if let Some(ctxs) = ctxs {
                        for ctx in ctxs.into_inner() {
                            let (input, ctx) = ctx.into_inner();
                            generate(
                                args.optimization.channel_capacity,
                                force_overwrite,
                                verify,
                                input,
                                vec![ctx],
                            )
                            .await?
                        }

                        // If there are no additional non-defaulted checksums to generate, return
                        // early.
                        if _is_checksum_defaulted {
                            return Ok(());
                        }
                    }
                };

                for input in input {
                    generate(
                        args.optimization.channel_capacity,
                        force_overwrite,
                        verify,
                        input,
                        checksum.clone(),
                    )
                    .await?;
                }
            }
        }
        Subcommands::Check {
            input,
            update,
            group_by,
        } => {
            let files = check(input, group_by).await?;

            let mut groups = Vec::with_capacity(files.len());
            for file in files {
                if update {
                    file.write().await?;
                }

                groups.push(
                    file.into_state()
                        .into_iter()
                        .map(|state| state.into_inner().0)
                        .collect(),
                );
            }

            println!("{}", CheckOutput::new(groups, group_by).to_json_string()?);
        }
    };

    Ok(())
}

async fn check(input: Vec<String>, group_by: GroupBy) -> Result<Vec<SumsFile>> {
    CheckTaskBuilder::default()
        .with_input_files(input)
        .with_group_by(group_by)
        .build()
        .await?
        .run()
        .await
}

async fn generate(
    capacity: usize,
    force_overwrite: bool,
    verify: bool,
    input: String,
    ctxs: Vec<Ctx>,
) -> Result<()> {
    GenerateTaskBuilder::default()
        .with_overwrite(force_overwrite)
        .with_verify(verify)
        .with_input_file_name(input)
        .with_context(ctxs)
        .with_capacity(capacity)
        .write()
        .build()
        .await?
        .run()
        .await?;
    Ok(())
}
