//! Checksum calculation and logic.
//!

pub mod aws_etag;
pub mod file;
pub mod standard;

use crate::checksum::aws_etag::AWSETagCtx;
use crate::checksum::standard::{MultipartChecksumType, StandardCtx};
use crate::error::{Error, Result};
use crate::io::Provider;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumType};
use futures_util::{Stream, StreamExt, pin_mut};
use serde::de::Error as SerdeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::result;
use std::str::FromStr;
use std::sync::Arc;

/// The checksum context. This enum also determines the best order of checksums,
/// which is useful for copy operations. AWS etag checksums are preferred over
/// regular checksums.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum Ctx {
    AWSEtag(AWSETagCtx),
    Regular(StandardCtx),
}

impl Default for Ctx {
    fn default() -> Self {
        Self::Regular(Default::default())
    }
}

impl<'de> Deserialize<'de> for Ctx {
    /// Implement deserialize using `FromStr`.
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

impl Serialize for Ctx {
    /// Implement serialize using `ToString`.
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        String::serialize(&self.to_string(), serializer)
    }
}

impl Ctx {
    /// Update a checksum with some data.
    pub fn update(&mut self, data: Arc<[u8]>) -> Result<()> {
        match self {
            Ctx::Regular(ctx) => ctx.update(data),
            Ctx::AWSEtag(ctx) => ctx.update(data),
        }
    }

    /// Finalize the checksum.
    pub fn finalize(&mut self) -> Result<Vec<u8>> {
        match self {
            Ctx::Regular(ctx) => ctx.finalize(),
            Ctx::AWSEtag(ctx) => ctx.finalize(),
        }
    }

    /// Generate a checksum from a stream of bytes.
    pub async fn generate(
        &mut self,
        stream: impl Stream<Item = Result<Arc<[u8]>>>,
    ) -> Result<Vec<u8>> {
        pin_mut!(stream);

        while let Some(chunk) = stream.next().await {
            self.update(chunk?)?;
        }

        self.finalize()
    }

    /// Get the digest output.
    pub fn digest_to_string(&self, digest: &[u8]) -> String {
        match self {
            Ctx::Regular(ctx) => ctx.digest_to_string(digest),
            Ctx::AWSEtag(ctx) => ctx.digest_to_string(digest),
        }
    }

    /// Set the file size if this is an AWS context.
    pub fn set_file_size(&mut self, file_size: Option<u64>) {
        if let Ctx::AWSEtag(ctx) = self {
            ctx.set_file_size(file_size);
        }
    }

    /// Get the encoded part checksums and their part sizes if this is an AWS checksum context.
    pub fn part_checksums(&self) -> Option<Vec<(u64, String)>> {
        match self {
            Ctx::Regular(_) => None,
            Ctx::AWSEtag(ctx) => Some(ctx.part_checksums()),
        }
    }

    /// Does this context represent a valid and preferred multipart checksum. All multipart
    /// checksums are preferred except for those with different sized part sizes. Returns
    /// the preferred part size.
    pub fn is_preferred_multipart(&self, provider: &Provider) -> Option<u64> {
        if let Self::AWSEtag(ctx) = self {
            ctx.is_preferred_multipart(provider)
        } else {
            None
        }
    }

    /// Does this context represent an AWS-compatible single part checksum, i.e. is it a regular
    /// checksum that AWS supports directly or as an additional checksum.
    pub fn is_preferred_single_part(&self, provider: &Provider) -> bool {
        matches!(self, Self::Regular(regular) if regular.is_preferred_cloud_ctx(provider))
    }

    /// Get the underlying standard checksum context.
    pub fn into_standard(self) -> StandardCtx {
        match self {
            Ctx::AWSEtag(ctx) => ctx.ctx(),
            Ctx::Regular(ctx) => ctx,
        }
    }
}

impl TryFrom<Ctx> for ChecksumAlgorithm {
    type Error = Error;

    fn try_from(ctx: Ctx) -> Result<Self> {
        Ok(match ctx.into_standard() {
            StandardCtx::CRC64NVME(_, _) => ChecksumAlgorithm::Crc64Nvme,
            StandardCtx::CRC32C(_, _) => ChecksumAlgorithm::Crc32C,
            StandardCtx::CRC32(_, _) => ChecksumAlgorithm::Crc32,
            StandardCtx::MD5(_) => ChecksumAlgorithm::Md5,
            StandardCtx::SHA1(_) => ChecksumAlgorithm::Sha1,
            StandardCtx::SHA256(_) => ChecksumAlgorithm::Sha256,
            StandardCtx::SHA512(_) => ChecksumAlgorithm::Sha512,
            StandardCtx::XXHash64(_) => ChecksumAlgorithm::Xxhash64,
            StandardCtx::XXHash3(_) => ChecksumAlgorithm::Xxhash3,
            StandardCtx::XXHash128(_) => ChecksumAlgorithm::Xxhash128,
            StandardCtx::QuickXor => {
                return Err(Error::aws_error(
                    "`quickxor` is not implemented".to_string(),
                ));
            }
        })
    }
}

/// The checksum type to declare when creating a multipart upload.
impl From<MultipartChecksumType> for ChecksumType {
    fn from(checksum_type: MultipartChecksumType) -> Self {
        match checksum_type {
            MultipartChecksumType::FullObject => ChecksumType::FullObject,
            MultipartChecksumType::Composite => ChecksumType::Composite,
            // Composite is chosen for algorithms that support both.
            MultipartChecksumType::Either => ChecksumType::Composite,
        }
    }
}

impl Display for Ctx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Ctx::Regular(ctx) => Display::fmt(ctx, f),
            Ctx::AWSEtag(ctx) => Display::fmt(ctx, f),
        }
    }
}

impl FromStr for Ctx {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let aws_etag = AWSETagCtx::from_str(s);
        if aws_etag.is_err() {
            Ok(Self::Regular(StandardCtx::from_str(s)?))
        } else {
            Ok(Self::AWSEtag(aws_etag?))
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::io::sums::SharedReader;
    use crate::io::sums::channel::test::channel_reader;
    use crate::test::{TEST_FILE_SIZE, TestFileBuilder};
    use anyhow::Result;
    use tokio::fs::File;
    use tokio::join;

    #[test]
    fn checksum_algorithm_conversion_is_exhaustive() -> Result<()> {
        let cases = [
            ("md5", ChecksumAlgorithm::Md5),
            ("sha1", ChecksumAlgorithm::Sha1),
            ("sha256", ChecksumAlgorithm::Sha256),
            ("sha512", ChecksumAlgorithm::Sha512),
            ("crc32", ChecksumAlgorithm::Crc32),
            ("crc32c", ChecksumAlgorithm::Crc32C),
            ("crc64nvme", ChecksumAlgorithm::Crc64Nvme),
            ("xxhash64", ChecksumAlgorithm::Xxhash64),
            ("xxhash3", ChecksumAlgorithm::Xxhash3),
            ("xxhash128", ChecksumAlgorithm::Xxhash128),
        ];

        for (name, expected) in cases {
            let ctx = Ctx::from_str(name)?;
            assert_eq!(ChecksumAlgorithm::try_from(ctx)?, expected);
        }

        assert!(ChecksumAlgorithm::try_from(Ctx::Regular(StandardCtx::QuickXor)).is_err(),);

        Ok(())
    }

    pub(crate) async fn test_checksum(checksum: &str, expected: &str) -> Result<()> {
        let test_file = TestFileBuilder::new()?.generate_test_defaults()?;
        let mut reader = channel_reader(File::open(test_file).await?).await;

        let mut checksum = Ctx::from_str(checksum)?;
        checksum.set_file_size(Some(TEST_FILE_SIZE));

        let stream = reader.as_stream();
        let task = tokio::spawn(async move { reader.read_chunks().await });

        let (digest, _) = join!(checksum.generate(stream), task);

        assert_eq!(expected, checksum.digest_to_string(&digest?));

        Ok(())
    }
}
