use cloud_checksum::cli::execute_args;
use cloud_checksum::error::Result;
use cloud_checksum::Command;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Command::parse_args()?;

    execute_args(args).await?;

    Ok(())
}
