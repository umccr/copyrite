use clap::Parser;
use cloud_checksum::error::Result;
use cloud_checksum::{Args, Checksum, Commands};
use futures_util::future::join_all;
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::Sha256;
use std::fmt::LowerHex;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{stdin, AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};

/// Message type for passing byte data.
#[derive(Debug, Clone)]
pub enum Message {
    Buf(Arc<[u8]>),
    Stop,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Concurrently compute checksums using a channel.
    let (tx, md5rx) = broadcast::channel(1000);
    let sha1rx = tx.subscribe();
    let sha256rx = tx.subscribe();

    // There's two kinds of tasks:
    let mut tasks = vec![];

    match args.commands {
        Commands::Generate { input, .. } => {
            // 1. The read task which reads data into a buffer, and sends it over the channel:
            match input {
                None => {
                    tasks.push(tokio::spawn(async move {
                        read_task(tx, BufReader::new(stdin())).await
                    }));
                }
                Some(input) => {
                    tasks.push(tokio::spawn(async move {
                        read_task(tx, BufReader::new(File::open(input).await?)).await
                    }));
                }
            }
        }
        Commands::Check { .. } => todo!(),
    };

    // 2. the checksum task, which receives data from the channel for each checksum type,
    // and incrementally computes the checksum until there is no more data.
    if args.checksums.contains(&Checksum::MD5) {
        tasks.push(tokio::spawn(async move {
            checksum_task(
                "MD5",
                md5rx,
                md5::Context::new(),
                |ctx, buf| ctx.consume(buf),
                |ctx| ctx.compute(),
            )
            .await
        }));
    }
    if args.checksums.contains(&Checksum::SHA1) {
        tasks.push(tokio::spawn(async move {
            checksum_task(
                "SHA1",
                sha1rx,
                Sha1::new(),
                |ctx, buf| ctx.update(buf),
                |ctx| ctx.finalize(),
            )
            .await
        }));
    }
    if args.checksums.contains(&Checksum::SHA256) {
        tasks.push(tokio::spawn(async move {
            checksum_task(
                "SHA256",
                sha256rx,
                Sha256::new(),
                |ctx, buf| ctx.update(buf),
                |ctx| ctx.finalize(),
            )
            .await
        }));
    }

    // Each task is spawned to run concurrently. At the end, wait for all tasks to complete.
    join_all(tasks)
        .await
        .into_iter()
        .map(|val| val?)
        .collect::<Result<Vec<_>>>()?;

    Ok(())
}

/// Read data from a buffer and send it into the channel.
async fn read_task<T>(tx: Sender<Message>, mut reader: BufReader<T>) -> Result<()>
where
    T: AsyncRead + Unpin,
{
    loop {
        // Read data into a buffer.
        let mut buf = vec![0; 1000];
        let n = reader.read(&mut buf).await?;

        // Send a stop message if there is no more data.
        if n == 0 {
            tx.send(Message::Stop)?;
            break;
        }

        // Send the buffer. An Arc allows sharing the buffer across multiple receivers without
        // copying it.
        tx.send(Message::Buf(Arc::from(buf)))?;
    }

    Ok(())
}

/// Calculate a checksum by retrieving data from the channel.
async fn checksum_task<T, R>(
    fmt: &str,
    mut rx: Receiver<Message>,
    mut ctx: T,
    consume: impl Fn(&mut T, Arc<[u8]>),
    compute: impl FnOnce(T) -> R,
) -> Result<()>
where
    R: LowerHex,
{
    // Retrieve the message.
    let mut msg = rx.recv().await?;
    // And compute the checksum until a stop message is received.
    while let Message::Buf(buf) = msg {
        consume(&mut ctx, buf);
        msg = rx.recv().await?;
    }

    let digest = compute(ctx);
    println!("The {} digest is: {:x}", fmt, digest);

    Ok(())
}
