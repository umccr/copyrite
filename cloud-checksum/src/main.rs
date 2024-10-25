use clap::Parser;
use cloud_checksum::error::Result;
use cloud_checksum::reader::channel::{ChannelReader, Message};
use cloud_checksum::{checksum, Args, Commands};
use futures_util::future::join_all;
use hex::encode;
use tokio::fs::File;
use tokio::io::stdin;
use tokio::sync::broadcast::Sender;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Concurrently compute checksums using a channel.
    let tx = Sender::new(1000);
    let rxs = (0..args.checksums.len())
        .map(|_| tx.subscribe())
        .collect::<Vec<_>>();

    // There's two kinds of tasks:
    let mut tasks = vec![];

    match args.commands {
        Commands::Generate { input, .. } => {
            // 1. The read task which reads data into a buffer, and sends it over the channel:
            match input {
                None => {
                    tasks.push(tokio::spawn(async move {
                        ChannelReader::new(stdin(), tx).send_to_end().await
                    }));
                }
                Some(input) => {
                    tasks.push(tokio::spawn(async move {
                        ChannelReader::new(File::open(input).await?, tx)
                            .send_to_end()
                            .await
                    }));
                }
            }
        }
        Commands::Check { .. } => todo!(),
    };

    // 2. the checksum task, which receives data from the channel for each checksum type,
    // and incrementally computes the checksum until there is no more data.
    for (checksum, mut rx) in args.checksums.into_iter().zip(rxs) {
        tasks.push(tokio::spawn(async move {
            let mut ctx = checksum::Checksum::from(checksum);

            // Retrieve the message.
            let mut msg = rx.recv().await?;
            // And compute the checksum until a stop message is received.
            while let Message::Buf(buf) = msg {
                ctx.update(&buf);
                msg = rx.recv().await?;
            }

            let digest = ctx.finalize();
            println!("The {:#?} digest is: {}", checksum, encode(digest));

            Ok(())
        }))
    }

    // Each task is spawned to run concurrently. At the end, wait for all tasks to complete.
    join_all(tasks)
        .await
        .into_iter()
        .map(|val| val?)
        .collect::<Result<Vec<_>>>()?;

    Ok(())
}
