use cloud_checksum::cli::Command;
use cloud_checksum::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Command::parse_args()?;

    args.execute().await?;

    Ok(())
}
