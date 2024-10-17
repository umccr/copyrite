use clap::{Parser, ValueEnum};

/// Args for the checksum-cloud CLI.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Checksums to use. Can be specified multiple times or comma-separated.
    /// At least one checksum is required.
    #[arg(value_delimiter = ',', required = true, short, long)]
    checksums: Vec<Checksum>,
}

/// The checksum to use.
#[derive(Debug, Clone, ValueEnum)]
pub enum Checksum {
    /// Calculate the MD5 checksum.
    MD5,
    /// Calculate the SHA1 checksum.
    SHA1,
    /// Calculate the SHA256 checksum.
    SHA256,
}
