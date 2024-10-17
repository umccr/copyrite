use checksum_cloud::Args;
use clap::Parser;
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    println!("{:#?}", args);

    Ok(())
}
