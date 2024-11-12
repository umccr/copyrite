use clap::Parser;
use cloud_checksum::error::Result;
use cloud_checksum::reader::channel::ChannelReader;
use cloud_checksum::task::generate::GenerateTask;
use cloud_checksum::{Commands, Subcommands};
use tokio::fs::File;
use tokio::io::stdin;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Commands::parse();

    match args.commands {
        Subcommands::Generate {
            input,
            stdin: use_stdin,
            ..
        } => {
            if use_stdin {
                let mut reader = ChannelReader::new(stdin(), args.optimization.channel_capacity);

                GenerateTask::default()
                    .add_generate_tasks(args.checksum, &mut reader)
                    .add_reader_task(reader)?
                    .run()
                    .await?
                    .write()
                    .await?
            } else {
                let mut reader = ChannelReader::new(
                    File::open(&input).await?,
                    args.optimization.channel_capacity,
                );

                GenerateTask::default()
                    .with_input_file_name(input)
                    .add_generate_tasks(args.checksum, &mut reader)
                    .add_reader_task(reader)?
                    .run()
                    .await?
                    .write()
                    .await?
            }
        }
        Subcommands::Check { .. } => todo!(),
    };

    Ok(())
}
