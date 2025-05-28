//! Integration tests for copying files that work on AWS S3 directly. This uses a smaller 10MiB file
//! to increase speeds and requires AWS credentials and a test bucket.
//!

use anyhow::Result;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::types::ChecksumMode;
use aws_sdk_s3::Client;
use copyrite::cli::{Command, CredentialProvider};
use copyrite::io::{create_s3_client, Provider};
use copyrite::test::TestFileBuilder;
use dotenvy::dotenv;
use envy::prefixed;
use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tempfile::TempDir;

/// Configuration for integration tests.
#[derive(Debug, Deserialize)]
struct TestConfig {
    bucket_uri: String,
    endpoint_url: Option<String>,
}

impl TestConfig {
    fn load() -> Result<Self> {
        dotenv()?;

        let mut env: Self = prefixed("COPYRITE_TEST_").from_env()?;

        env.bucket_uri = env
            .bucket_uri
            .strip_suffix("/")
            .unwrap_or(&env.bucket_uri)
            .to_string();

        env.endpoint_url = env
            .endpoint_url
            .map(|url| url.strip_suffix("/").unwrap_or(&url).to_string());

        Ok(env)
    }

    fn format_uri(&self, path: &str) -> String {
        format!("{}/{}", self.bucket_uri, path)
    }

    fn set_endpoint_url(&self, mut commands: Vec<String>) -> Vec<String> {
        if let Some(endpoint_url) = &self.endpoint_url {
            commands.extend([
                "--source-endpoint-url".to_string(),
                endpoint_url.to_string(),
                "--destination-endpoint-url".to_string(),
                endpoint_url.to_string(),
            ]);
        }

        commands
    }
}

#[ignore]
#[tokio::test]
async fn copy_test() -> Result<()> {
    let config = TestConfig::load()?;
    let file = TestFileBuilder::default().generate_bench_defaults()?;
    let client = create_s3_client(
        &CredentialProvider::DefaultEnvironment,
        None,
        None,
        config.endpoint_url.as_deref(),
    )
    .await?;

    // Local to S3.
    local_s3_multipart(file.as_path(), &config, &client).await?;
    local_s3_single_part(file.as_path(), &config, &client).await?;

    // S3 to S3.
    s3_s3_multipart(&config, &client).await?;
    s3_s3_single_part(&config, &client).await?;

    // S3 to local.
    s3_local_multipart(file.as_path(), &config).await?;
    s3_local_single_part(file.as_path(), &config).await?;

    Ok(())
}

/// Test a multipart copy to S3.
async fn local_s3_multipart(file: &Path, config: &TestConfig, client: &Client) -> Result<()> {
    let uri = config.format_uri("multipart");
    let file = file.to_string_lossy();

    execute_multipart(file.as_ref(), uri.as_ref(), config).await;

    let head = get_head_object(client, uri.as_ref()).await?;
    assert_head_multipart(head);

    Ok(())
}

/// Test a single part copy to S3.
async fn local_s3_single_part(file: &Path, config: &TestConfig, client: &Client) -> Result<()> {
    let uri = config.format_uri("single_part");
    let file = file.to_string_lossy();

    execute_single_part(file.as_ref(), uri.as_ref(), config).await;

    let head = get_head_object(client, uri.as_ref()).await?;
    assert_head_single_part(head);

    Ok(())
}

/// Test a multipart copy between S3 objects.
async fn s3_s3_multipart(config: &TestConfig, client: &Client) -> Result<()> {
    let uri = config.format_uri("multipart");
    let destination = config.format_uri("multipart_copy");

    execute_multipart(uri.as_ref(), destination.as_ref(), config).await;

    let head = get_head_object(client, destination.as_ref()).await?;
    assert_head_multipart(head);

    Ok(())
}

/// Test a single part copy between S3 objects.
async fn s3_s3_single_part(config: &TestConfig, client: &Client) -> Result<()> {
    let uri = config.format_uri("single_part");
    let destination = config.format_uri("single_part_copy");

    execute_single_part(uri.as_ref(), destination.as_ref(), config).await;

    let head = get_head_object(client, destination.as_ref()).await?;
    assert_head_single_part(head);

    Ok(())
}

/// Test a multipart copy to a local file.
async fn s3_local_multipart(original: &Path, config: &TestConfig) -> Result<()> {
    let uri = config.format_uri("multipart_copy");
    let tmp = TempDir::new()?;
    let copy_to = tmp.path().join("multipart_copy");

    execute_multipart(uri.as_ref(), copy_to.to_string_lossy().as_ref(), config).await;
    assert_original(
        original.to_str().unwrap(),
        copy_to.to_string_lossy().as_ref(),
    )?;

    Ok(())
}

/// Test a single part copy to a local file.
async fn s3_local_single_part(original: &Path, config: &TestConfig) -> Result<()> {
    let uri = config.format_uri("single_part_copy");
    let tmp = TempDir::new()?;
    let copy_to = tmp.path().join("single_part_copy");

    execute_single_part(uri.as_ref(), copy_to.to_string_lossy().as_ref(), config).await;
    assert_original(
        original.to_str().unwrap(),
        copy_to.to_string_lossy().as_ref(),
    )?;

    Ok(())
}

fn assert_original(original: &str, copy: &str) -> Result<()> {
    let mut original_bytes = vec![];
    File::open(original)?.read_to_end(&mut original_bytes)?;

    let mut copy_bytes = vec![];
    File::open(copy)?.read_to_end(&mut copy_bytes)?;

    assert_eq!(copy_bytes, original_bytes);
    Ok(())
}

async fn get_head_object(client: &Client, url: &str) -> Result<HeadObjectOutput> {
    let (bucket, key) = Provider::try_from(url)?.into_s3()?;
    let head = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await?;
    Ok(head)
}

async fn execute_multipart(from: &str, to: &str, config: &TestConfig) {
    let mut commands = [
        "copyrite",
        "copy",
        from,
        to,
        "--multipart-threshold",
        "5MiB",
        "--part-size",
        "5MiB",
        "--tag-mode",
        "best-effort",
    ]
    .into_iter()
    .map(|s| s.to_string())
    .collect();
    commands = config.set_endpoint_url(commands);

    execute_command(&commands).await;
}

async fn execute_single_part(from: &str, to: &str, config: &TestConfig) {
    let mut commands = [
        "copyrite",
        "copy",
        from,
        to,
        "--part-size",
        "20MiB",
        "--multipart-threshold",
        "20MiB",
        "--tag-mode",
        "best-effort",
    ]
    .into_iter()
    .map(|s| s.to_string())
    .collect();
    commands = config.set_endpoint_url(commands);

    execute_command(&commands).await;
}

fn assert_head_multipart(head: HeadObjectOutput) {
    assert_eq!(
        head.e_tag,
        Some("\"ec1e29805585d04a93eb8cf464b68c43-2\"".to_string())
    );

    if let Some(checksum) = head.checksum_crc64_nvme {
        assert_eq!(checksum, "yM/EwMxFxsE=".to_string());
    }
    if let Some(checksum) = head.checksum_crc32_c {
        assert_eq!(checksum, "4VjD4A==".to_string());
    }
}

fn assert_head_single_part(head: HeadObjectOutput) {
    assert_eq!(
        head.e_tag,
        Some("\"617808065bb1a8be2755f9be0c0ac769\"".to_string())
    );

    if let Some(checksum) = head.checksum_crc64_nvme {
        assert_eq!(checksum, "yM/EwMxFxsE=".to_string());
    }
    if let Some(checksum) = head.checksum_crc32_c {
        assert_eq!(checksum, "4VjD4A==".to_string());
    }
}

async fn execute_command(commands: &[String]) {
    let args = Command::parse_from_iter(commands).unwrap();
    args.execute().await.unwrap();
}
