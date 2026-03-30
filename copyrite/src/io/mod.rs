//! Module that handles all file IO
//!

use crate::cli::CredentialProvider;
use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use aws_config::Region;
use aws_credential_types::provider::ProvideCredentials;
use aws_credential_types::Credentials;
use aws_sdk_s3::{config, Client};
use aws_smithy_runtime_api::client::behavior_version::BehaviorVersion;
use serde::Deserialize;

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

fn construct_credentials(
    access_key_id: impl Into<String>,
    secret_access_key: impl Into<String>,
    session_token: Option<impl Into<String>>,
) -> Credentials {
    let mut builder = Credentials::builder()
        .access_key_id(access_key_id)
        .secret_access_key(secret_access_key)
        .provider_name("copyrite");

    if let Some(session_token) = session_token {
        builder = builder.session_token(session_token);
    }

    builder.build()
}

/// The expected structure of credentials from Secrets Manager.
#[derive(Deserialize)]
pub struct SecretsManagerCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

impl SecretsManagerCredentials {
    /// Construct credentials by fetching from an AWS Secrets Manager secret. Uses the default
    /// credential chain to authenticate with Secrets Manager, then parses the secret's values.
    pub async fn new(secret_id: &str) -> Result<SecretsManagerCredentials> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let sm_client = aws_sdk_secretsmanager::Client::new(&config);

        let output = sm_client
            .get_secret_value()
            .secret_id(secret_id)
            .send()
            .await
            .map_err(|err| {
                Error::aws_error(format!("failed to fetch secret `{}`: {}", secret_id, err))
            })?;

        let secret_json = if let Some(secret_string) = output.secret_string() {
            secret_string.to_string()
        } else if let Some(secret_binary) = output.secret_binary() {
            String::from_utf8(secret_binary.as_ref().to_vec()).map_err(|err| {
                ParseError(format!(
                    "secret `{}` binary is invalid UTF-8: {}",
                    secret_id, err
                ))
            })?
        } else {
            return Err(ParseError(format!(
                "secret `{}` has no string or binary value",
                secret_id
            )));
        };

        Self::deserialize_from(&secret_json)
    }

    /// Deserialize from a JSON secret.
    pub fn deserialize_from(secret_json: &str) -> Result<SecretsManagerCredentials> {
        serde_json::from_str(secret_json)
            .map_err(|err| ParseError(format!("failed to parse secret: {}", err)))
    }

    /// Convert into AWS config compatible credentials.
    pub fn into_credentials(self) -> Credentials {
        construct_credentials(
            self.access_key_id,
            self.secret_access_key,
            self.session_token,
        )
    }
}

/// Credential overrides from CLI args or environment variables.
pub struct CredentialOverrides {
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    session_token: Option<String>,
}

impl CredentialOverrides {
    /// Create new credential overrides.
    pub fn new(
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
    ) -> Self {
        Self {
            access_key_id,
            secret_access_key,
            session_token,
        }
    }

    /// Returns true if any override is set.
    pub fn any(&self) -> bool {
        self.access_key_id.is_some()
            || self.secret_access_key.is_some()
            || self.session_token.is_some()
    }

    /// Merge overrides with base credentials. Each override takes precedence over the corresponding
    /// field in the base credentials.
    pub fn merge_with(&self, base: Option<&Credentials>) -> Result<Credentials> {
        let access_key_id = self
            .access_key_id
            .as_deref()
            .or_else(|| base.map(|base| base.access_key_id()))
            .ok_or_else(|| {
                ParseError(
                    "access-key-id must be provided as an override or by the credential provider"
                        .to_string(),
                )
            })?;
        let secret_access_key = self
            .secret_access_key
            .as_deref()
            .or_else(|| base.map(|base| base.secret_access_key()))
            .ok_or_else(|| {
                ParseError("secret-access-key must be provided as an override or by the credential provider".to_string())
            })?;
        let session_token = self
            .session_token
            .as_deref()
            .or_else(|| base.and_then(|base| base.session_token()));

        // There's no need to preserve base.expiry() as overrides imply no expiry and control given
        // to user supplied credentials.
        Ok(construct_credentials(
            access_key_id,
            secret_access_key,
            session_token,
        ))
    }
}

/// Create an S3 client from the credentials provider, profile, region and endpoint url.
/// Any fields set in `overrides` take precedence over the resolved credential provider values.
pub async fn create_s3_client(
    provider: &CredentialProvider,
    profile: Option<&str>,
    region: Option<&str>,
    endpoint_url: Option<&str>,
    secret: Option<&str>,
    overrides: CredentialOverrides,
) -> Result<Client> {
    let mut loader = aws_config::defaults(BehaviorVersion::latest());

    if let Some(region) = region {
        loader = loader.region(Region::new(region.to_string()));
    }
    if let Some(endpoint_url) = endpoint_url {
        loader = loader.endpoint_url(endpoint_url);
    }

    let loader = match (provider, profile, secret) {
        (CredentialProvider::DefaultEnvironment, _, _) => loader,
        (CredentialProvider::NoCredentials, _, _) => loader.no_credentials(),
        (CredentialProvider::AwsProfile, Some(profile), _) => loader.profile_name(profile),
        (CredentialProvider::AwsSecret, _, Some(secret)) => {
            let credentials = SecretsManagerCredentials::new(secret)
                .await?
                .into_credentials();
            loader.credentials_provider(credentials)
        }
        (CredentialProvider::AwsProfile, None, _) => {
            return Err(ParseError(
                "profile must be specified if using aws-profile credential provider".to_string(),
            ))
        }
        (CredentialProvider::AwsSecret, _, None) => {
            return Err(ParseError(
                "secret must be specified if using aws-secret credential provider".to_string(),
            ))
        }
    };

    let sdk_config = loader.load().await;

    let s3_config = if overrides.any() {
        // Allow no credentials to be set with only overrides.
        let base = if let Some(creds_provider) = sdk_config.credentials_provider() {
            creds_provider.provide_credentials().await.ok()
        } else {
            None
        };

        let merged = overrides.merge_with(base.as_ref())?;
        config::Builder::from(&sdk_config)
            .credentials_provider(merged)
            .build()
    } else {
        config::Builder::from(&sdk_config).build()
    };

    Ok(Client::from_conf(s3_config))
}

/// Create the default S3 client.
pub async fn default_s3_client() -> Result<Client> {
    let no_overrides = CredentialOverrides {
        access_key_id: None,
        secret_access_key: None,
        session_token: None,
    };
    create_s3_client(
        &CredentialProvider::DefaultEnvironment,
        None,
        None,
        None,
        None,
        no_overrides,
    )
    .await
}

#[cfg(test)]
mod tests {
    use crate::io::{CredentialOverrides, Provider, SecretsManagerCredentials};
    use anyhow::Result;
    use aws_credential_types::Credentials;
    use serde_json::json;
    use std::time::{Duration, SystemTime};

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

    #[test]
    fn merge_with_overrides() {
        let overrides = CredentialOverrides::new(None, None, None);
        let base = base_credentials();
        let merged = overrides.merge_with(Some(&base)).unwrap();

        assert_eq!(merged.access_key_id(), "access_key");
        assert_eq!(merged.secret_access_key(), "secret_access_key");
        assert_eq!(merged.session_token(), Some("session_token"));
        assert!(merged.expiry().is_none());

        let overrides = CredentialOverrides::new(
            Some("override_access_key".to_string()),
            Some("override_secret_key".to_string()),
            Some("override_session_token".to_string()),
        );
        let base = base_credentials();
        let merged = overrides.merge_with(Some(&base)).unwrap();

        assert_eq!(merged.access_key_id(), "override_access_key");
        assert_eq!(merged.secret_access_key(), "override_secret_key");
        assert_eq!(merged.session_token(), Some("override_session_token"));
        assert!(merged.expiry().is_none());

        let overrides =
            CredentialOverrides::new(Some("override_access_key".to_string()), None, None);
        let base = base_credentials();
        let merged = overrides.merge_with(Some(&base)).unwrap();

        assert_eq!(merged.access_key_id(), "override_access_key");
        assert_eq!(merged.secret_access_key(), "secret_access_key");
        assert_eq!(merged.session_token(), Some("session_token"));
        assert!(merged.expiry().is_none());

        let overrides = CredentialOverrides::new(
            Some("override_access_key".to_string()),
            Some("override_secret_key".to_string()),
            None,
        );
        let merged = overrides.merge_with(None).unwrap();

        assert_eq!(merged.access_key_id(), "override_access_key");
        assert_eq!(merged.secret_access_key(), "override_secret_key");
        assert_eq!(merged.session_token(), None);
        assert!(merged.expiry().is_none());

        let result = CredentialOverrides::new(None, Some("override_secret_key".to_string()), None)
            .merge_with(None);
        assert!(result.is_err());

        let result = CredentialOverrides::new(Some("override_access_key".to_string()), None, None)
            .merge_with(None);
        assert!(result.is_err());
    }

    #[test]
    fn secrets_manager_deserialize() {
        let json = json!({
            "access_key_id": "access_key",
            "secret_access_key": "secret_access_key", // pragma: allowlist secret
            "session_token": "session_token"
        });
        let creds = SecretsManagerCredentials::deserialize_from(&json.to_string())
            .unwrap()
            .into_credentials();

        assert_eq!(creds.access_key_id(), "access_key");
        assert_eq!(creds.secret_access_key(), "secret_access_key");
        assert_eq!(creds.session_token(), Some("session_token"));

        let json = json!({
            "access_key_id": "access_key",
            "secret_access_key": "secret_access_key" // pragma: allowlist secret
        });
        let creds = SecretsManagerCredentials::deserialize_from(&json.to_string())
            .unwrap()
            .into_credentials();

        assert_eq!(creds.access_key_id(), "access_key");
        assert_eq!(creds.secret_access_key(), "secret_access_key");
        assert_eq!(creds.session_token(), None);

        assert!(SecretsManagerCredentials::deserialize_from(
            &json!({"secret_access_key": "secret_access_key"}).to_string() // pragma: allowlist secret
        )
        .is_err());
        assert!(SecretsManagerCredentials::deserialize_from(
            &json!({"access_key_id": "access_key"}).to_string()
        )
        .is_err());
        assert!(SecretsManagerCredentials::deserialize_from(&json!({}).to_string()).is_err());
    }

    fn provider_s3(url: &str) -> Result<(String, String)> {
        Ok(Provider::try_from(url)?.into_s3()?)
    }

    fn provider_file(url: &str) -> Result<String> {
        Ok(Provider::try_from(url)?.into_file()?)
    }

    fn base_credentials() -> Credentials {
        Credentials::builder()
            .access_key_id("access_key")
            .secret_access_key("secret_access_key") // pragma: allowlist secret
            .session_token("session_token")
            .expiry(
                SystemTime::now()
                    .checked_add(Duration::from_mins(1))
                    .unwrap(),
            )
            .provider_name("test")
            .build()
    }
}
