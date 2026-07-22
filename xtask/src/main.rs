//! Workspace automation tasks.
//!
//! Run with `cargo xtask <command>`.
//!

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::{env, fs};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use git_cliff::args::Opt;

#[derive(Parser)]
#[command(name = "xtask", about = "Workspace automation tasks")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Regenerate the Debian and RPM packaging changelogs from git history.
    Changelog,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Changelog => changelog(),
    }
}

/// Absolute path to the workspace root.
fn repo_root() -> PathBuf {
    if let Some(root) = env::var_os("XTASK_ROOT") {
        return PathBuf::from(root);
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate should have a parent directory")
        .to_path_buf()
}

/// Regenerate the Debian and RPM packaging changelogs with git-cliff.
fn changelog() -> Result<()> {
    let root = repo_root();
    let version = crate_version(&root.join("copyrite/Cargo.toml"))?;
    let tag = format!("v{version}");

    let debian = run_cliff(&root, &root.join("pkg/cliff-debian.toml"), &tag)?;
    fs::write(root.join("pkg/debian/changelog"), &debian)
        .context("writing pkg/debian/changelog")?;

    let rpm_entries = run_cliff(&root, &root.join("pkg/cliff-rpm.toml"), &tag)?;
    append_rpm_changelog(&root.join("pkg/rpm/copyrite.spec"), &rpm_entries)?;

    Ok(())
}

/// Read the crate version from `copyrite/Cargo.toml`.
fn crate_version(manifest: &Path) -> Result<String> {
    let text =
        fs::read_to_string(manifest).with_context(|| format!("reading {}", manifest.display()))?;
    for line in text.lines() {
        // Match a literal `version = "x.y.z"`, not `rust-version` or `version.workspace`.
        let Some(rest) = line.trim().strip_prefix("version") else {
            continue;
        };
        let Some(value) = rest.trim_start().strip_prefix('=') else {
            continue;
        };
        let version = value.trim().trim_matches('"');
        if !version.is_empty() {
            return Ok(version.to_string());
        }
    }

    bail!("could not find a version in {}", manifest.display())
}

/// Render a changelog with git-cliff, tagging the unreleased commits as `tag`.
fn run_cliff(root: &Path, config_path: &Path, tag: &str) -> Result<String> {
    let opt = Opt::parse_from([
        OsStr::new("git-cliff"),
        OsStr::new("--config"),
        config_path.as_os_str(),
        OsStr::new("--repository"),
        root.as_os_str(),
        OsStr::new("--tag"),
        OsStr::new(tag),
    ]);

    let changelog = git_cliff::run(opt.clone())
        .with_context(|| format!("running git-cliff with {}", config_path.display()))?;

    let mut buf = Vec::new();
    git_cliff::write_changelog(&opt, changelog, &mut buf).context("rendering changelog")?;

    String::from_utf8(buf).context("changelog output not UTF-8")
}

/// Replace everything from the spec's `%changelog` line onwards with `entries`.
fn append_rpm_changelog(spec_path: &Path, entries: &str) -> Result<()> {
    let spec = fs::read_to_string(spec_path)
        .with_context(|| format!("reading {}", spec_path.display()))?;

    const MARKER: &str = "%changelog\n";
    let marker_end = spec
        .find(MARKER)
        .map(|i| i + MARKER.len())
        .context("could not find '%changelog' section in pkg/rpm/copyrite.spec")?;

    let mut out = String::with_capacity(marker_end + entries.len());
    out.push_str(&spec[..marker_end]);
    out.push_str(entries);

    fs::write(spec_path, &out).with_context(|| format!("writing {}", spec_path.display()))?;
    Ok(())
}
