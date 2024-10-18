use checksum_cloud::error::{Error, Result};
use checksum_cloud::{Args, Checksum};
use clap::Parser;
use futures_util::future::join_all;
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::Sha256;
use std::fmt::LowerHex;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};
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

    let reader = BufReader::new(File::open(args.input).await?);

    let (tx, md5rx) = broadcast::channel(1000);
    let sha1rx = tx.subscribe();
    let sha256rx = tx.subscribe();

    let mut tasks = vec![];
    tasks.push(tokio::spawn(async move { read_task(tx, reader).await }));

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

    join_all(tasks)
        .await
        .into_iter()
        .map(|val| val?)
        .collect::<Result<Vec<_>>>()?;

    Ok(())
}

/// Read data from a buffer and send it into the channel.
async fn read_task(tx: Sender<Message>, mut reader: BufReader<File>) -> Result<()> {
    loop {
        let mut buf = vec![0; 1000];
        let n = reader.read(&mut buf).await?;

        if n == 0 {
            tx.send(Message::Stop)?;
            break;
        }

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
    let mut msg = rx.recv().await?;
    while let Message::Buf(buf) = msg {
        consume(&mut ctx, buf);
        msg = rx.recv().await?;
    }

    let digest = compute(ctx);
    println!("The {} digest is: {:x}", fmt, digest);

    Ok::<_, Error>(())
}
