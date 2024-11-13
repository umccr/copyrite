//! Checksum calculation and logic.
//!

pub mod file;

use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use crate::{Checksum, Endianness};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use crc32c::crc32c_append;
use futures_util::{pin_mut, Stream, StreamExt};
use sha1::Digest;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::mem::discriminant;
use std::str::FromStr;
use std::sync::Arc;

/// The checksum calculator.
#[derive(Debug, Clone)]
pub enum ChecksumCtx {
    // Note, options remove a clone later on, but it might be
    // better Box the state for clarity.
    /// Calculate the MD5 checksum.
    MD5(Option<md5::Md5>),
    /// Calculate the SHA1 checksum.
    SHA1(Option<sha1::Sha1>),
    /// Calculate the SHA256 checksum.
    SHA256(Option<sha2::Sha256>),
    /// Calculate the AWS ETag.
    AWSETag(Option<md5::Md5>),
    /// Calculate a CRC32.
    CRC32(Option<crc32fast::Hasher>, Endianness),
    CRC32C(u32, Endianness),
    /// Calculate the QuickXor checksum.
    QuickXor,
}

impl Eq for ChecksumCtx {}

impl PartialEq for ChecksumCtx {
    fn eq(&self, other: &Self) -> bool {
        discriminant(self) == discriminant(other)
    }
}

impl Hash for ChecksumCtx {
    fn hash<H: Hasher>(&self, state: &mut H) {
        discriminant(self).hash(state)
    }
}

impl FromStr for ChecksumCtx {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let ctx = Self::parse_endianness(s)?;
        if let Some(ctx) = ctx {
            return Ok(ctx);
        }
        let ctx = Self::parse_part_size(s)?;
        if let Some(ctx) = ctx {
            return Ok(ctx);
        }

        let checksum = <Checksum as FromStr>::from_str(s)?;
        let ctx = match checksum {
            Checksum::MD5 => Self::MD5(Some(md5::Md5::new())),
            Checksum::SHA1 => Self::SHA1(Some(sha1::Sha1::new())),
            Checksum::SHA256 => Self::SHA256(Some(sha2::Sha256::new())),
            Checksum::AWSETag => Self::AWSETag(Some(md5::Md5::new())),
            Checksum::CRC32 => Self::CRC32(Some(crc32fast::Hasher::new()), Endianness::BigEndian),
            Checksum::CRC32C => Self::CRC32C(0, Endianness::BigEndian),
            Checksum::QuickXor => todo!(),
        };
        Ok(ctx)
    }
}

impl From<&ChecksumCtx> for Checksum {
    fn from(checksum: &ChecksumCtx) -> Self {
        match checksum {
            ChecksumCtx::MD5(_) => Self::MD5,
            ChecksumCtx::SHA1(_) => Self::SHA1,
            ChecksumCtx::SHA256(_) => Self::SHA256,
            ChecksumCtx::AWSETag(_) => Self::AWSETag,
            ChecksumCtx::CRC32(_, _) => Self::CRC32,
            ChecksumCtx::CRC32C(_, _) => Self::CRC32C,
            ChecksumCtx::QuickXor => Self::QuickXor,
        }
    }
}

impl Display for ChecksumCtx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChecksumCtx::MD5(_) => write!(f, "md5"),
            ChecksumCtx::SHA1(_) => write!(f, "sha1"),
            ChecksumCtx::SHA256(_) => write!(f, "sha256"),
            ChecksumCtx::AWSETag(_) => write!(f, "aws-etag"),
            // Noting big-endian is the default if left unspecified.
            ChecksumCtx::CRC32(_, endianness) => match endianness {
                Endianness::LittleEndian => write!(f, "crc32-{}", endianness),
                Endianness::BigEndian => write!(f, "crc32"),
            },
            ChecksumCtx::CRC32C(_, endianness) => match endianness {
                Endianness::LittleEndian => write!(f, "crc32c-{}", endianness),
                Endianness::BigEndian => write!(f, "crc32c"),
            },
            ChecksumCtx::QuickXor => todo!(),
        }
    }
}

impl ChecksumCtx {
    /// Convert the output digest to a canonical string representation of this checksum.
    pub fn digest_to_string(&self, digest: Vec<u8>) -> String {
        // Todo all the AWS part-size formatting.
        if let ChecksumCtx::AWSETag(_) = self {
            return STANDARD.encode(digest);
        }

        hex::encode(digest)
    }

    /// Parse into a `ChecksumCtx` for values that use endianness. Uses an -le suffix for
    /// little-endian and -be for big-endian.
    pub fn parse_endianness(s: &str) -> Result<Option<Self>> {
        if let Some(s) = s.strip_suffix("-le") {
            let ctx = match <Checksum as FromStr>::from_str(s)? {
                Checksum::CRC32 => {
                    ChecksumCtx::CRC32(Some(crc32fast::Hasher::new()), Endianness::LittleEndian)
                }
                Checksum::CRC32C => ChecksumCtx::CRC32C(0, Endianness::LittleEndian),
                _ => return Err(ParseError("invalid suffix -le for checksum".to_string())),
            };
            Ok(Some(ctx))
        } else if let Some(s) = s.strip_suffix("-be") {
            let ctx = match <Checksum as FromStr>::from_str(s)? {
                Checksum::CRC32 => {
                    ChecksumCtx::CRC32(Some(crc32fast::Hasher::new()), Endianness::BigEndian)
                }
                Checksum::CRC32C => ChecksumCtx::CRC32C(0, Endianness::BigEndian),
                _ => return Err(ParseError("invalid suffix -be for checksum".to_string())),
            };
            Ok(Some(ctx))
        } else {
            Ok(None)
        }
    }

    /// Parse into a `ChecksumCtx` for values that use endianness. Parses an -aws-<n> suffix,
    /// where n represents the part size to calculate.
    pub fn parse_part_size(s: &str) -> Result<Option<Self>> {
        let mut iter = s.rsplitn(2, "-aws-");
        let part_size = iter.next();
        let checksum = iter.next();

        // Todo, part-size is unused, implement it for AWS-style etags.
        if let (Some(checksum), Some(part_size)) = (checksum, part_size) {
            let part_size: u64 = part_size
                .parse()
                .map_err(|err| ParseError(format!("invalid part size: {}", err)))?;
            let ctx = match <Checksum as FromStr>::from_str(checksum)? {
                Checksum::MD5 => ChecksumCtx::MD5(Some(md5::Md5::new())),
                Checksum::SHA1 => ChecksumCtx::SHA1(Some(sha1::Sha1::new())),
                Checksum::SHA256 => ChecksumCtx::SHA256(Some(sha2::Sha256::new())),
                _ => {
                    return Err(ParseError(format!(
                        "invalid suffix -{} for checksum",
                        part_size
                    )))
                }
            };
            Ok(Some(ctx))
        } else {
            Ok(None)
        }
    }

    /// Set the endianness if this is a CRC-based checksum.
    pub fn with_endianness(self, endianness: Endianness) -> Self {
        match self {
            Self::CRC32(ctx, _) => Self::CRC32(ctx, endianness),
            Self::CRC32C(ctx, _) => Self::CRC32C(ctx, endianness),
            checksum => checksum,
        }
    }

    /// Update a checksum with some data.
    pub fn update(&mut self, data: &[u8]) {
        match self {
            ChecksumCtx::MD5(Some(ctx)) => ctx.update(data),
            ChecksumCtx::SHA1(Some(ctx)) => ctx.update(data),
            ChecksumCtx::SHA256(Some(ctx)) => ctx.update(data),
            // Just doing regular MD5 for now, not yet doing parts.
            ChecksumCtx::AWSETag(Some(ctx)) => ctx.update(data),
            ChecksumCtx::CRC32(Some(ctx), _) => ctx.update(data),
            ChecksumCtx::CRC32C(ctx, _) => *ctx = crc32c_append(*ctx, data),
            ChecksumCtx::QuickXor => todo!(),
            _ => panic!("cannot call update with empty context"),
        }
    }

    /// Finalize the checksum.
    pub fn finalize(&mut self) -> Vec<u8> {
        let msg = "cannot call finalize with empty context";
        match self {
            ChecksumCtx::MD5(ctx) => ctx.take().expect(msg).finalize().to_vec(),
            ChecksumCtx::SHA1(ctx) => ctx.take().expect(msg).finalize().to_vec(),
            ChecksumCtx::SHA256(ctx) => ctx.take().expect(msg).finalize().to_vec(),
            ChecksumCtx::AWSETag(ctx) => ctx.take().expect(msg).finalize().to_vec(),
            ChecksumCtx::CRC32(ctx, endianness) => match endianness {
                Endianness::LittleEndian => {
                    ctx.take().expect(msg).finalize().to_le_bytes().to_vec()
                }
                Endianness::BigEndian => ctx.take().expect(msg).finalize().to_be_bytes().to_vec(),
            },
            ChecksumCtx::CRC32C(ctx, endianness) => match endianness {
                Endianness::LittleEndian => ctx.to_le_bytes().to_vec(),
                Endianness::BigEndian => ctx.to_be_bytes().to_vec(),
            },
            ChecksumCtx::QuickXor => todo!(),
        }
    }

    /// Generate a checksum from a stream of bytes.
    pub async fn generate(
        &mut self,
        stream: impl Stream<Item = Result<Arc<[u8]>>>,
    ) -> Result<Vec<u8>> {
        pin_mut!(stream);

        while let Some(chunk) = stream.next().await {
            self.update(&chunk?);
        }

        Ok(self.finalize())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::reader::channel::test::channel_reader;
    use crate::reader::SharedReader;
    use crate::test::TestFileBuilder;
    use anyhow::Result;
    use hex::encode;
    use tokio::fs::File;
    use tokio::join;

    #[tokio::test]
    async fn test_md5() -> Result<()> {
        test_checksum("md5", expected_md5_sum()).await
    }

    #[tokio::test]
    async fn test_sha1() -> Result<()> {
        test_checksum("sha1", expected_sha1_sum()).await
    }

    #[tokio::test]
    async fn test_sha256() -> Result<()> {
        test_checksum("sha256", expected_sha256_sum()).await
    }

    #[tokio::test]
    async fn test_crc32_be() -> Result<()> {
        test_checksum("crc32", expected_crc32_be()).await
    }

    #[tokio::test]
    async fn test_crc32_le() -> Result<()> {
        test_checksum("crc32-le", expected_crc32_le()).await
    }

    #[tokio::test]
    async fn test_crc32c_be() -> Result<()> {
        test_checksum("crc32c", expected_crc32c_be()).await
    }

    #[tokio::test]
    async fn test_crc32c_le() -> Result<()> {
        test_checksum("crc32c-le", expected_crc32c_le()).await
    }

    #[tokio::test]
    async fn test_aws_etag_md5() -> Result<()> {
        test_checksum("md5-aws-1", expected_md5_sum()).await
    }

    #[tokio::test]
    async fn test_aws_etag() -> Result<()> {
        test_checksum("aws-etag", expected_md5_sum()).await
    }

    pub(crate) fn expected_md5_sum() -> &'static str {
        "d93e71879054f205ede90d35c8081ca5"
    }

    pub(crate) fn expected_sha1_sum() -> &'static str {
        "3eafdb6ad3a27167e0db70fccc40d0614307dabf"
    }

    pub(crate) fn expected_sha256_sum() -> &'static str {
        "29ffbd53cbe43179ab2fa62dbd958c0ec30b340ab50ce7c785e8a7a4b4771e39"
    }

    pub(crate) fn expected_crc32_be() -> &'static str {
        "3320f39e"
    }

    pub(crate) fn expected_crc32_le() -> &'static str {
        "9ef32033"
    }

    pub(crate) fn expected_crc32c_be() -> &'static str {
        "4920106a"
    }

    pub(crate) fn expected_crc32c_le() -> &'static str {
        "6a102049"
    }

    async fn test_checksum(checksum: &str, expected: &str) -> Result<()> {
        let test_file = TestFileBuilder::default().generate_test_defaults()?;
        let mut reader = channel_reader(File::open(test_file).await?).await;

        let mut checksum = ChecksumCtx::from_str(checksum)?;

        let stream = reader.as_stream();
        let task = tokio::spawn(async move { reader.read_task().await });

        let (digest, _) = join!(checksum.generate(stream), task);

        assert_eq!(expected, encode(digest?));

        Ok(())
    }
}
