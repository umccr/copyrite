use tracing_subscriber::EnvFilter;
use cloud_checksum::cli::Command;
use cloud_checksum::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // install global collector configured based on RUST_LOG env var.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .try_init();
    
    let args = Command::parse_args()?;

    args.execute().await?;

    Ok(())
}
