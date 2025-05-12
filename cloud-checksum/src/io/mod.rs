//! Module that handles all file IO
//!

use crate::cli::CredentialProvider;
use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use aws_config::Region;
use aws_sdk_s3::{config, Client};
use aws_smithy_runtime_api::client::behavior_version::BehaviorVersion;

pub mod copy;
pub mod sums;

/// The type of provider for the object.
#[derive(Debug, Clone)]
pub enum Provider {
    File { file: String },
    S3 { bucket: String, key: String },
}

impl Provider {
    /// Format an S3 url.
    pub fn format_s3(bucket: &str, key: &str) -> String {
        format!("s3://{}/{}", bucket, key)
    }

    /// Format a file url.
    pub fn format_file(file: &str) -> String {
        format!("file://{}", file)
    }

    /// Format the provider into a string.
    pub fn format(&self) -> String {
        match self {
            Provider::File { file } => Self::format_file(file),
            Provider::S3 { bucket, key } => Self::format_s3(bucket, key),
        }
    }

    /// Parse from an S3 url, e.g.`s3://bucket/key`.
    pub fn parse_s3_url(s: &str) -> Result<Self> {
        let Some(s) = s.strip_prefix("s3://") else {
            return Err(ParseError(format!("{} is not an S3 url", s)));
        };

        let split = s.split_once("/");
        let Some((bucket, key)) = split else {
            return Err(ParseError(format!("failed to parse {}", s)));
        };

        if bucket.is_empty() {
            return Err(ParseError(format!("{} is missing a bucket", s)));
        }
        if key.is_empty() {
            return Err(ParseError(format!("{} is missing a key", s)));
        }

        Ok(Self::S3 {
            bucket: bucket.to_string(),
            key: key.to_string(),
        })
    }

    /// Convert the provider into an S3 bucket and key.
    pub fn into_s3(self) -> Result<(String, String)> {
        match self {
            Provider::S3 { bucket, key } => Ok((bucket, key)),
            _ => Err(ParseError("not an S3 provider".to_string())),
        }
    }

    /// Parse from a string a file name which can optionally be prefixed with `file://`
    pub fn parse_file_url(s: &str) -> Self {
        Self::File {
            file: s.strip_prefix("file://").unwrap_or(s).to_string(),
        }
    }

    /// Convert the provider into a file.
    pub fn into_file(self) -> Result<String> {
        match self {
            Provider::File { file } => Ok(file),
            _ => Err(ParseError("not a file provider".to_string())),
        }
    }

    /// Check if the provider is an file provider.
    pub fn is_file(&self) -> bool {
        matches!(self, Provider::File { .. })
    }

    /// Check if the provider is an S3 provider.
    pub fn is_s3(&self) -> bool {
        matches!(self, Provider::S3 { .. })
    }
}

impl TryFrom<&str> for Provider {
    type Error = Error;

    fn try_from(url: &str) -> Result<Self> {
        if url.starts_with("s3://") {
            Self::parse_s3_url(url)
        } else {
            Ok(Self::parse_file_url(url))
        }
    }
}

/// Create an S3 client from the credentials provider, profile, region and endpoint url.
pub async fn create_s3_client(
    provider: &CredentialProvider,
    profile: Option<&str>,
    region: Option<&str>,
    endpoint_url: Option<&str>,
) -> Result<Client> {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());

    if let Some(region) = region {
        loader = loader.region(Region::new(region.to_string()));
    }
    if let Some(endpoint_url) = endpoint_url {
        loader = loader.endpoint_url(endpoint_url);
    }

    let loader = match (provider, profile) {
        (CredentialProvider::DefaultEnvironment, _) => loader,
        (CredentialProvider::NoCredentials, _) => loader.no_credentials(),
        (CredentialProvider::AwsProfile, Some(profile)) => loader.profile_name(profile),
        _ => {
            return Err(ParseError(
                "profile must be specified if using aws-profile credential provider".to_string(),
            ))
        }
    };

    let config = config::Builder::from(&loader.load().await).build();

    Ok(Client::from_conf(config))
}

/// Create the default S3 client.
pub async fn default_s3_client() -> Result<Client> {
    create_s3_client(&CredentialProvider::DefaultEnvironment, None, None, None).await
}

#[cfg(test)]
mod tests {
    use crate::io::Provider;
    use anyhow::Result;

    #[tokio::test]
    pub async fn test_parse_url() -> Result<()> {
        let s3 = provider_s3("s3://bucket/key")?;
        assert_eq!(s3, ("bucket".to_string(), "key".to_string()));

        let s3 = provider_s3("s3://bucket/key/")?;
        assert_eq!(s3, ("bucket".to_string(), "key/".to_string()));

        let file = provider_file("file://file")?;
        assert_eq!(file, "file".to_string());

        let file = provider_file("file")?;
        assert_eq!(file, "file".to_string());

        let s3 = provider_s3("s3://bucket/");
        assert!(s3.is_err());
        let s3 = provider_s3("s3://bucket/");
        assert!(s3.is_err());

        let s3 = provider_s3("s3://");
        assert!(s3.is_err());
        let s3 = provider_s3("s3://");
        assert!(s3.is_err());

        Ok(())
    }

    fn provider_s3(url: &str) -> Result<(String, String)> {
        Ok(Provider::try_from(url)?.into_s3()?)
    }

    fn provider_file(url: &str) -> Result<String> {
        Ok(Provider::try_from(url)?.into_file()?)
    }
}
