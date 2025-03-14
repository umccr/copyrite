use cloud_checksum::checksum::Ctx;
use cloud_checksum::error::Result;
use cloud_checksum::reader::channel::ChannelReader;
use cloud_checksum::task::check::{CheckObjects, CheckOutput, CheckTaskBuilder, GroupBy};
use cloud_checksum::task::generate::{GenerateTaskBuilder, SumCtxPairs};
use cloud_checksum::{Check, Commands, Generate, Subcommands};
use tokio::io::stdin;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Commands::parse_args()?;

    match args.commands {
        Subcommands::Generate(Generate {
            input,
            checksum,
            missing: generate_missing,
            force_overwrite,
            verify,
        }) => {
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
        Subcommands::Check(Check {
            input,
            update,
            group_by,
        }) => {
            let files = check(input, group_by, update).await?;
            let output = CheckOutput::from((files, group_by));

            println!("{}", output.to_json_string()?);
        }
        _ => {}
    };

    Ok(())
}

async fn check(input: Vec<String>, group_by: GroupBy, update: bool) -> Result<CheckObjects> {
    CheckTaskBuilder::default()
        .with_input_files(input)
        .with_group_by(group_by)
        .with_update(update)
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
