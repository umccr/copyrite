//! AWS checksums and functionality.
//!

use crate::checksum::aws_etag::{AWSETagCtx, PartMode};
use crate::checksum::file::SumsFile;
use crate::checksum::standard::StandardCtx;
use crate::checksum::{file, Ctx};
use crate::error::Error::{AwsError, ParseError};
use crate::error::Result;
use crate::Endianness;
use aws_config::{load_defaults, BehaviorVersion};
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::Client;

/// Build an S3 sums object.
#[derive(Debug, Default)]
pub struct S3Builder {
    client: Option<Client>,
    bucket: Option<String>,
    key: Option<String>,
    url: Option<String>,
}

impl S3Builder {
    /// Set the client by loading AWS environment variables.
    pub async fn with_default_client(mut self) -> Self {
        let config = load_defaults(BehaviorVersion::latest()).await;
        self.client = Some(Client::new(&config));
        self
    }

    /// Set the client.
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the key.
    pub fn with_key(mut self, key: String) -> Self {
        self.key = Some(key);
        self
    }

    /// Set the bucket.
    pub fn with_bucket(mut self, bucket: String) -> Self {
        self.bucket = Some(bucket);
        self
    }

    /// Set the bucket and key from an S3 url, e.g.`s3://bucket/key`.
    pub fn parse_from_url(mut self, url: String) -> Self {
        self.url = Some(url);
        self
    }

    /// Build using the client, bucket and key.
    pub fn build(mut self) -> Result<S3> {
        if let Some(url) = self.url {
            let (bucket, key) = Self::parse_url(&url)?;
            self.bucket = Some(bucket);
            self.key = Some(key);
        }

        let error_fn =
            || ParseError("client, bucket and key are required in `S3Builder`".to_string());

        Ok(S3::new(
            self.client.ok_or_else(error_fn)?,
            self.bucket.ok_or_else(error_fn)?,
            self.key.ok_or_else(error_fn)?,
        ))
    }

    /// Parse from an S3 url, e.g.`s3://bucket/key`.
    pub fn parse_url(s: &str) -> Result<(String, String)> {
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

        Ok((bucket.to_string(), key.to_string()))
    }
}

/// An S3 object and AWS-related existing sums.
#[derive(Debug)]
pub struct S3 {
    client: Client,
    bucket: String,
    key: String,
}

impl S3 {
    /// Create a new S3 object.
    pub fn new(client: Client, bucket: String, key: String) -> S3 {
        Self {
            client,
            bucket,
            key,
        }
    }

    /// Get an existing sums file if it exists.
    pub async fn get_existing_sums(&self) -> Result<Option<SumsFile>> {
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(SumsFile::format_sums_file(&self.key))
            .send()
            .await
        {
            Ok(sums) => {
                let data = sums.body.collect().await?.to_vec();
                let sums = SumsFile::read_from_slice(
                    data.as_slice(),
                    SumsFile::format_target_file(&self.key),
                )
                .await?;
                Ok(Some(sums))
            }
            Err(err) if matches!(err.as_service_error(), Some(GetObjectError::NoSuchKey(_))) => {
                Ok(None)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Load a sums file from object metadata.
    pub async fn sums_from_metadata(&self) -> Result<SumsFile> {
        // The target file metadata.
        let key = SumsFile::format_target_file(&self.key);
        let file = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await?;
        let attributes = self
            .client
            .get_object_attributes()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await?;

        let file_size = file.content_length().map(u64::try_from).transpose()?;
        let mut sums_file = SumsFile::default().add_name(key).with_size(file_size);

        // Rules
        // If standard aws e_tag is present, then there are no part checksums presents, only the
        // concatenated e_tag.
        // If any of the additional checksum fields are present, including CRC64NVME, then there
        // are multiple checksum parts, including the concatenated parts.

        if let Some(sum) = attributes.e_tag() {
            let part_size = attributes
                .object_parts()
                .and_then(|parts| parts.total_parts_count)
                .map(u64::try_from)
                .transpose()?;
            let ctx = if let Some(part_size) = part_size {
                Ctx::AWSEtag(AWSETagCtx::new(
                    StandardCtx::MD5(Default::default()),
                    PartMode::PartNumber(part_size),
                    file_size,
                ))
            } else {
                Ctx::Regular(StandardCtx::MD5(Default::default()))
            };

            let checksum = file::Checksum::new(sum.to_string(), part_size, None);

            sums_file = sums_file.add_checksum(ctx, checksum);
        }
        if let Some(sum) = attributes.checksum().and_then(|c| c.checksum_crc32()) {
            let part_size = attributes
                .object_parts()
                .and_then(|parts| parts.total_parts_count)
                .map(u64::try_from)
                .transpose()?;
            let ctx = if let Some(part_size) = part_size {
                Ctx::AWSEtag(AWSETagCtx::new(
                    StandardCtx::CRC32(Default::default(), Endianness::LittleEndian),
                    PartMode::PartNumber(part_size),
                    file_size,
                ))
            } else {
                Ctx::Regular(StandardCtx::CRC32(
                    Default::default(),
                    Endianness::LittleEndian,
                ))
            };

            let parts = attributes
                .object_parts()
                .map(|parts| {
                    parts
                        .parts()
                        .iter()
                        .map(|part| {
                            part.checksum_crc32()
                                .map(|c| c.to_string())
                                .ok_or_else(|| AwsError("mismatched part".to_string()))
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            let checksum = file::Checksum::new(sum.to_string(), part_size, parts);

            sums_file = sums_file.add_checksum(ctx, checksum);
        }
        if let Some(sum) = attributes.checksum().and_then(|c| c.checksum_crc32_c()) {
            let part_size = attributes
                .object_parts()
                .and_then(|parts| parts.total_parts_count)
                .map(u64::try_from)
                .transpose()?;
            let ctx = if let Some(part_size) = part_size {
                Ctx::AWSEtag(AWSETagCtx::new(
                    StandardCtx::CRC32C(Default::default(), Endianness::LittleEndian),
                    PartMode::PartNumber(part_size),
                    file_size,
                ))
            } else {
                Ctx::Regular(StandardCtx::CRC32C(
                    Default::default(),
                    Endianness::LittleEndian,
                ))
            };

            let parts = attributes
                .object_parts()
                .map(|parts| {
                    parts
                        .parts()
                        .iter()
                        .map(|part| {
                            part.checksum_crc32_c()
                                .map(|c| c.to_string())
                                .ok_or_else(|| AwsError("mismatched part".to_string()))
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            let checksum = file::Checksum::new(sum.to_string(), part_size, parts);

            sums_file = sums_file.add_checksum(ctx, checksum);
        }
        if let Some(sum) = attributes.checksum().and_then(|c| c.checksum_sha1()) {
            let part_size = attributes
                .object_parts()
                .and_then(|parts| parts.total_parts_count)
                .map(u64::try_from)
                .transpose()?;
            let ctx = if let Some(part_size) = part_size {
                Ctx::AWSEtag(AWSETagCtx::new(
                    StandardCtx::SHA1(Default::default()),
                    PartMode::PartNumber(part_size),
                    file_size,
                ))
            } else {
                Ctx::Regular(StandardCtx::SHA1(Default::default()))
            };

            let parts = attributes
                .object_parts()
                .map(|parts| {
                    parts
                        .parts()
                        .iter()
                        .map(|part| {
                            part.checksum_sha1()
                                .map(|c| c.to_string())
                                .ok_or_else(|| AwsError("mismatched part".to_string()))
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            let checksum = file::Checksum::new(sum.to_string(), part_size, parts);

            sums_file = sums_file.add_checksum(ctx, checksum);
        }
        if let Some(sum) = attributes.checksum().and_then(|c| c.checksum_sha256()) {
            let part_size = attributes
                .object_parts()
                .and_then(|parts| parts.total_parts_count)
                .map(u64::try_from)
                .transpose()?;
            let ctx = if let Some(part_size) = part_size {
                Ctx::AWSEtag(AWSETagCtx::new(
                    StandardCtx::SHA256(Default::default()),
                    PartMode::PartNumber(part_size),
                    file_size,
                ))
            } else {
                Ctx::Regular(StandardCtx::SHA256(Default::default()))
            };

            let parts = attributes
                .object_parts()
                .map(|parts| {
                    parts
                        .parts()
                        .iter()
                        .map(|part| {
                            part.checksum_sha256()
                                .map(|c| c.to_string())
                                .ok_or_else(|| AwsError("mismatched part".to_string()))
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            let checksum = file::Checksum::new(sum.to_string(), part_size, parts);

            sums_file = sums_file.add_checksum(ctx, checksum);
        }

        Ok(sums_file)
    }

    pub fn parse_part_number(s: &str) -> Result<Option<u64>> {
        s.rsplit_once("-")
            .map(|(_, part_number)| {
                part_number
                    .parse::<u64>()
                    .map_err(|err| ParseError(err.to_string()))
            })
            .transpose()
    }

    /// Get the inner values not including the S3 client.
    pub fn into_inner(self) -> (String, String) {
        (self.bucket, self.key)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::error::Result;

    #[tokio::test]
    pub async fn test_parse_url() -> Result<()> {
        let s3 = expected_s3("s3://bucket/key").await?;
        assert_eq!(s3.into_inner(), ("bucket".to_string(), "key".to_string()));

        let s3 = expected_s3("s3://bucket/key/").await?;
        assert_eq!(s3.into_inner(), ("bucket".to_string(), "key/".to_string()));

        let s3 = expected_s3("file://bucket/key").await;
        assert!(s3.is_err());

        let s3 = expected_s3("s3://bucket/").await;
        assert!(s3.is_err());

        let s3 = expected_s3("s3://").await;
        assert!(s3.is_err());

        Ok(())
    }

    async fn expected_s3(url: &str) -> Result<S3> {
        S3Builder::default()
            .parse_from_url(url.to_string())
            .with_default_client()
            .await
            .build()
    }
}
