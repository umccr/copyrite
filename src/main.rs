use copyrite::cli::Command;
use copyrite::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Command::parse_args()?;

    args.execute().await?;

    Ok(())
}
