//! Checksum calculation and logic.
//!

pub mod aws_etag;
pub mod file;

use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use crate::{Checksum, Endianness};
use crc32c::crc32c_append;
use futures_util::{pin_mut, Stream, StreamExt};
use sha1::Digest;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::mem::discriminant;
use std::str::FromStr;
use std::sync::Arc;

/// A wrapper around a check-summing algorithm.
#[derive(Debug, Clone)]
pub struct ChecksumAlgorithm<T> {
    checksum: Option<T>,
    aws_etag_part_size: Option<u64>,
}

impl<T> ChecksumAlgorithm<T> {
    /// Create a new wrapper.
    pub fn new(checksum: Option<T>, aws_etag_part_size: Option<u64>) -> Self {
        Self {
            checksum,
            aws_etag_part_size,
        }
    }

    /// Get the inner checksum, panics if not initialized.
    pub fn take(&mut self) -> T {
        self.checksum
            .take()
            .expect("cannot take uninitialized checksum")
    }

    /// Get a reference to the inner checksum, panics if not initialized.
    pub fn get_mut(&mut self) -> &mut T {
        self.checksum
            .as_mut()
            .expect("cannot get reference to uninitialized checksum")
    }

    /// Get the part size.
    pub fn aws_etag_part_size(&self) -> Option<u64> {
        self.aws_etag_part_size
    }
}

/// The checksum calculator.
#[derive(Debug, Clone)]
pub enum ChecksumCtx {
    // Note, options remove a clone later on, but it might be
    // better Box the state for clarity.
    /// Calculate the MD5 checksum.
    MD5(ChecksumAlgorithm<md5::Md5>),
    /// Calculate the SHA1 checksum.
    SHA1(ChecksumAlgorithm<sha1::Sha1>),
    /// Calculate the SHA256 checksum.
    SHA256(ChecksumAlgorithm<sha2::Sha256>),
    /// Calculate a CRC32.
    CRC32(ChecksumAlgorithm<crc32fast::Hasher>, Endianness),
    CRC32C(ChecksumAlgorithm<u32>, Endianness),
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
        let (s, ctx) = Self::parse_endianness(s)?;
        if let Some(ctx) = ctx {
            return Ok(ctx);
        }
        let ctx = Self::parse_part_size(s)?;
        if let Some(ctx) = ctx {
            return Ok(ctx);
        }

        let checksum = <Checksum as FromStr>::from_str(s)?;
        let ctx = match checksum {
            Checksum::MD5 | Checksum::AWSETag => {
                Self::MD5(ChecksumAlgorithm::new(Some(md5::Md5::new()), None))
            }
            Checksum::SHA1 => Self::SHA1(ChecksumAlgorithm::new(Some(sha1::Sha1::new()), None)),
            Checksum::SHA256 => {
                Self::SHA256(ChecksumAlgorithm::new(Some(sha2::Sha256::new()), None))
            }
            Checksum::CRC32 => Self::CRC32(
                ChecksumAlgorithm::new(Some(crc32fast::Hasher::new()), None),
                Endianness::BigEndian,
            ),
            Checksum::CRC32C => {
                Self::CRC32C(ChecksumAlgorithm::new(Some(0), None), Endianness::BigEndian)
            }
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
        // if let ChecksumCtx::AWSETag(_) = self {
        //     return STANDARD.encode(digest);
        // }

        hex::encode(digest)
    }

    /// Parse into a `ChecksumCtx` for values that use endianness. Uses an -le suffix for
    /// little-endian and -be for big-endian.
    pub fn parse_endianness(s: &str) -> Result<(&str, Option<Self>)> {
        if let Some(s) = s.strip_suffix("-le") {
            let ctx = match <Checksum as FromStr>::from_str(s)? {
                Checksum::CRC32 => ChecksumCtx::CRC32(
                    ChecksumAlgorithm::new(Some(crc32fast::Hasher::new()), None),
                    Endianness::LittleEndian,
                ),
                Checksum::CRC32C => ChecksumCtx::CRC32C(
                    ChecksumAlgorithm::new(Some(0), None),
                    Endianness::LittleEndian,
                ),
                _ => return Err(ParseError("invalid suffix -le for checksum".to_string())),
            };
            Ok((s, Some(ctx)))
        } else if let Some(s) = s.strip_suffix("-be") {
            let ctx = match <Checksum as FromStr>::from_str(s)? {
                Checksum::CRC32 => ChecksumCtx::CRC32(
                    ChecksumAlgorithm::new(Some(crc32fast::Hasher::new()), None),
                    Endianness::BigEndian,
                ),
                Checksum::CRC32C => ChecksumCtx::CRC32C(
                    ChecksumAlgorithm::new(Some(0), None),
                    Endianness::BigEndian,
                ),
                _ => return Err(ParseError("invalid suffix -be for checksum".to_string())),
            };
            Ok((s, Some(ctx)))
        } else {
            Ok((s, None))
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
                Checksum::MD5 | Checksum::AWSETag => ChecksumCtx::MD5(ChecksumAlgorithm::new(
                    Some(md5::Md5::new()),
                    Some(part_size),
                )),
                Checksum::SHA1 => ChecksumCtx::SHA1(ChecksumAlgorithm::new(
                    Some(sha1::Sha1::new()),
                    Some(part_size),
                )),
                Checksum::SHA256 => ChecksumCtx::SHA256(ChecksumAlgorithm::new(
                    Some(sha2::Sha256::new()),
                    Some(part_size),
                )),
                Checksum::CRC32 => Self::CRC32(
                    ChecksumAlgorithm::new(Some(crc32fast::Hasher::new()), Some(part_size)),
                    Endianness::BigEndian,
                ),
                Checksum::CRC32C => Self::CRC32C(
                    ChecksumAlgorithm::new(Some(0), Some(part_size)),
                    Endianness::BigEndian,
                ),
                Checksum::QuickXor => todo!(),
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
    pub fn update(&mut self, data: Arc<[u8]>) {
        match self {
            ChecksumCtx::MD5(ctx) => ctx.get_mut().update(&data),
            ChecksumCtx::SHA1(ctx) => ctx.get_mut().update(&data),
            ChecksumCtx::SHA256(ctx) => ctx.get_mut().update(&data),
            ChecksumCtx::CRC32(ctx, _) => ctx.get_mut().update(&data),
            ChecksumCtx::CRC32C(ctx, _) => {
                ctx.checksum = Some(crc32c_append(*ctx.get_mut(), &data))
            }
            ChecksumCtx::QuickXor => todo!(),
        }
    }

    /// Finalize the checksum.
    pub fn finalize(&mut self) -> Vec<u8> {
        match self {
            ChecksumCtx::MD5(ctx) => ctx.take().finalize().to_vec(),
            ChecksumCtx::SHA1(ctx) => ctx.take().finalize().to_vec(),
            ChecksumCtx::SHA256(ctx) => ctx.take().finalize().to_vec(),
            ChecksumCtx::CRC32(ctx, endianness) => match endianness {
                Endianness::LittleEndian => ctx.take().finalize().to_le_bytes().to_vec(),
                Endianness::BigEndian => ctx.take().finalize().to_be_bytes().to_vec(),
            },
            ChecksumCtx::CRC32C(ctx, endianness) => match endianness {
                Endianness::LittleEndian => ctx.take().to_le_bytes().to_vec(),
                Endianness::BigEndian => ctx.take().to_be_bytes().to_vec(),
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
            self.update(chunk?);
        }

        Ok(self.finalize())
    }

    /// Reset the checksum state.
    pub fn reset(&self) -> Self {
        match self {
            ChecksumCtx::MD5(ctx) => ChecksumCtx::MD5(ChecksumAlgorithm::new(
                Some(md5::Md5::new()),
                ctx.aws_etag_part_size,
            )),
            ChecksumCtx::SHA1(ctx) => ChecksumCtx::SHA1(ChecksumAlgorithm::new(
                Some(sha1::Sha1::new()),
                ctx.aws_etag_part_size,
            )),
            ChecksumCtx::SHA256(ctx) => ChecksumCtx::SHA256(ChecksumAlgorithm::new(
                Some(sha2::Sha256::new()),
                ctx.aws_etag_part_size,
            )),
            ChecksumCtx::CRC32(ctx, endianness) => ChecksumCtx::CRC32(
                ChecksumAlgorithm::new(Some(crc32fast::Hasher::new()), ctx.aws_etag_part_size),
                *endianness,
            ),
            ChecksumCtx::CRC32C(ctx, endianness) => ChecksumCtx::CRC32C(
                ChecksumAlgorithm::new(Some(0), ctx.aws_etag_part_size),
                *endianness,
            ),
            ChecksumCtx::QuickXor => todo!(),
        }
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
