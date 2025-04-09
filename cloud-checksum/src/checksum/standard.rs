//! Standard checksum algorithms
//!

use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use crate::io::Provider;
use crate::{Checksum, Endianness};
use crc32c::crc32c_append;
use md5::Digest;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::mem::discriminant;
use std::str::FromStr;
use std::sync::Arc;

/// The checksum calculator. This also defines the ordering of which checksums are preferred
/// for generating/copying data.
#[derive(Debug, Clone)]
pub enum StandardCtx {
    // Note, options remove a clone later on, but it might be
    // better to Box the state for clarity.
    /// Calculate a CRC32C.
    CRC32C(u32, Endianness),
    /// Calculate a CRC32.
    CRC32(Option<crc32fast::Hasher>, Endianness),
    /// Calculate the MD5 checksum.
    MD5(Option<md5::Md5>),
    /// Calculate the SHA1 checksum.
    SHA1(Option<sha1::Sha1>),
    /// Calculate the SHA256 checksum.
    SHA256(Option<sha2::Sha256>),
    /// Calculate the QuickXor checksum.
    QuickXor,
}

impl Ord for StandardCtx {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.to_u8(), self.endianness()).cmp(&(other.to_u8(), self.endianness()))
    }
}

impl PartialOrd for StandardCtx {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for StandardCtx {}

impl PartialEq for StandardCtx {
    fn eq(&self, other: &Self) -> bool {
        discriminant(self) == discriminant(other) && self.endianness() == other.endianness()
    }
}

impl Hash for StandardCtx {
    fn hash<H: Hasher>(&self, state: &mut H) {
        discriminant(self).hash(state);
        self.endianness().hash(state);
    }
}

impl Default for StandardCtx {
    fn default() -> Self {
        Self::CRC32(Default::default(), Endianness::BigEndian)
    }
}

impl FromStr for StandardCtx {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let ctx = Self::parse_endianness(s)?;
        if let Some(ctx) = ctx {
            return Ok(ctx);
        }

        let checksum = Checksum::from_str(s)?;
        let ctx = match checksum {
            Checksum::MD5 => Self::md5(),
            Checksum::SHA1 => Self::sha1(),
            Checksum::SHA256 => Self::sha256(),
            Checksum::CRC32 => Self::crc32(),
            Checksum::CRC32C => Self::crc32c(),
            _ => return Err(ParseError("unsupported checksum algorithm".to_string())),
        };
        Ok(ctx)
    }
}

impl From<&StandardCtx> for Checksum {
    fn from(checksum: &StandardCtx) -> Self {
        match checksum {
            StandardCtx::MD5(_) => Self::MD5,
            StandardCtx::SHA1(_) => Self::SHA1,
            StandardCtx::SHA256(_) => Self::SHA256,
            StandardCtx::CRC32(_, _) => Self::CRC32,
            StandardCtx::CRC32C(_, _) => Self::CRC32C,
            StandardCtx::QuickXor => Self::QuickXor,
        }
    }
}

impl Display for StandardCtx {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            StandardCtx::MD5(_) => write!(f, "md5"),
            StandardCtx::SHA1(_) => write!(f, "sha1"),
            StandardCtx::SHA256(_) => write!(f, "sha256"),
            // Noting big-endian is the default if left unspecified.
            StandardCtx::CRC32(_, endianness) => match endianness {
                Endianness::LittleEndian => write!(f, "crc32-{}", endianness),
                Endianness::BigEndian => write!(f, "crc32"),
            },
            StandardCtx::CRC32C(_, endianness) => match endianness {
                Endianness::LittleEndian => write!(f, "crc32c-{}", endianness),
                Endianness::BigEndian => write!(f, "crc32c"),
            },
            StandardCtx::QuickXor => todo!(),
        }
    }
}

impl StandardCtx {
    /// Create the MD5 variant.
    pub fn md5() -> Self {
        Self::MD5(Some(md5::Md5::new()))
    }

    /// Create the SHA1 variant.
    pub fn sha1() -> Self {
        Self::SHA1(Some(sha1::Sha1::new()))
    }

    /// Create the SHA256 variant.
    pub fn sha256() -> Self {
        Self::SHA256(Some(sha2::Sha256::new()))
    }

    /// Create the CRC32 variant.
    pub fn crc32() -> Self {
        Self::CRC32(Some(crc32fast::Hasher::new()), Endianness::BigEndian)
    }

    /// Create the CRC32C variant.
    pub fn crc32c() -> Self {
        Self::CRC32C(0, Endianness::BigEndian)
    }

    /// Parse into a `ChecksumCtx` for values that use endianness. Uses an -le suffix for
    /// little-endian and -be for big-endian.
    pub fn parse_endianness(s: &str) -> Result<Option<Self>> {
        if let Some(s) = s.strip_suffix("-le") {
            let ctx = match Checksum::from_str(s)? {
                Checksum::CRC32 => Self::crc32().with_endianness(Endianness::LittleEndian),
                Checksum::CRC32C => Self::crc32c().with_endianness(Endianness::LittleEndian),
                _ => return Err(ParseError("invalid suffix -le for checksum".to_string())),
            };
            Ok(Some(ctx))
        } else if let Some(s) = s.strip_suffix("-be") {
            let ctx = match Checksum::from_str(s)? {
                Checksum::CRC32 => Self::crc32(),
                Checksum::CRC32C => Self::crc32c(),
                _ => return Err(ParseError("invalid suffix -be for checksum".to_string())),
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
    pub fn update(&mut self, data: Arc<[u8]>) -> Result<()> {
        match self {
            StandardCtx::MD5(Some(ctx)) => ctx.update(data),
            StandardCtx::SHA1(Some(ctx)) => ctx.update(data),
            StandardCtx::SHA256(Some(ctx)) => ctx.update(data),
            StandardCtx::CRC32(Some(ctx), _) => ctx.update(&data),
            StandardCtx::CRC32C(ctx, _) => *ctx = crc32c_append(*ctx, &data),
            StandardCtx::QuickXor => todo!(),
            _ => panic!("cannot call update with empty context"),
        };

        Ok(())
    }

    /// Finalize the checksum.
    pub fn finalize(&mut self) -> Result<Vec<u8>> {
        let msg = "cannot call finalize with empty context";
        let digest = match self {
            StandardCtx::MD5(ctx) => ctx.take().expect(msg).finalize().to_vec(),
            StandardCtx::SHA1(ctx) => ctx.take().expect(msg).finalize().to_vec(),
            StandardCtx::SHA256(ctx) => ctx.take().expect(msg).finalize().to_vec(),
            StandardCtx::CRC32(ctx, endianness) => match endianness {
                Endianness::LittleEndian => {
                    ctx.take().expect(msg).finalize().to_le_bytes().to_vec()
                }
                Endianness::BigEndian => ctx.take().expect(msg).finalize().to_be_bytes().to_vec(),
            },
            StandardCtx::CRC32C(ctx, endianness) => match endianness {
                Endianness::LittleEndian => ctx.to_le_bytes().to_vec(),
                Endianness::BigEndian => ctx.to_be_bytes().to_vec(),
            },
            StandardCtx::QuickXor => todo!(),
        };

        Ok(digest)
    }

    /// Reset the checksum state.
    pub fn reset(&self) -> Self {
        match self {
            StandardCtx::MD5(_) => Self::md5(),
            StandardCtx::SHA1(_) => Self::sha1(),
            StandardCtx::SHA256(_) => Self::sha256(),
            StandardCtx::CRC32(_, endianness) => Self::crc32().with_endianness(*endianness),
            StandardCtx::CRC32C(_, endianness) => Self::crc32c().with_endianness(*endianness),
            StandardCtx::QuickXor => todo!(),
        }
    }

    /// Get the digest output.
    pub fn digest_to_string(&self, digest: &[u8]) -> String {
        hex::encode(digest)
    }

    /// Extract the endianness if this is a CRC variant.
    pub fn endianness(&self) -> Option<Endianness> {
        match self {
            StandardCtx::CRC32(_, endianness) | StandardCtx::CRC32C(_, endianness) => {
                Some(*endianness)
            }
            _ => None,
        }
    }

    /// Get the numeric value of the enum.
    pub fn to_u8(&self) -> u8 {
        match self {
            StandardCtx::MD5(_) => 0,
            StandardCtx::SHA1(_) => 1,
            StandardCtx::SHA256(_) => 2,
            StandardCtx::CRC32(_, _) => 3,
            StandardCtx::CRC32C(_, _) => 4,
            StandardCtx::QuickXor => 5,
        }
    }

    /// Is this a preferred cloud checksum for copying files.
    pub fn is_preferred_cloud_ctx(&self, provider: &Provider) -> bool {
        if provider.is_s3() {
            self.is_aws_ctx()
        } else {
            true
        }
    }

    /// Is this an AWS-compatible checksum context.
    pub fn is_aws_ctx(&self) -> bool {
        !matches!(self, StandardCtx::QuickXor)
    }

    /// Is this an AWS additional checksum that can be specified.
    pub fn is_aws_additional_ctx(&self) -> bool {
        !matches!(self, StandardCtx::QuickXor | StandardCtx::MD5(_))
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::checksum::test::test_checksum;
    use anyhow::Result;

    pub(crate) const EXPECTED_MD5_SUM: &str = "d93e71879054f205ede90d35c8081ca5"; // pragma: allowlist secret
    pub(crate) const EXPECTED_SHA1_SUM: &str = "3eafdb6ad3a27167e0db70fccc40d0614307dabf"; // pragma: allowlist secret
    pub(crate) const EXPECTED_SHA256_SUM: &str =
        "29ffbd53cbe43179ab2fa62dbd958c0ec30b340ab50ce7c785e8a7a4b4771e39"; // pragma: allowlist secret
    pub(crate) const EXPECTED_CRC32_BE_SUM: &str = "3320f39e";
    pub(crate) const EXPECTED_CRC32_LE_SUM: &str = "9ef32033";
    pub(crate) const EXPECTED_CRC32C_BE_SUM: &str = "4920106a";
    pub(crate) const EXPECTED_CRC32C_LE_SUM: &str = "6a102049";

    #[tokio::test]
    async fn test_md5() -> Result<()> {
        test_checksum("md5", EXPECTED_MD5_SUM).await
    }

    #[tokio::test]
    async fn test_sha1() -> Result<()> {
        test_checksum("sha1", EXPECTED_SHA1_SUM).await
    }

    #[tokio::test]
    async fn test_sha256() -> Result<()> {
        test_checksum("sha256", EXPECTED_SHA256_SUM).await
    }

    #[tokio::test]
    async fn test_crc32_be() -> Result<()> {
        test_checksum("crc32", EXPECTED_CRC32_BE_SUM).await
    }

    #[tokio::test]
    async fn test_crc32_le() -> Result<()> {
        test_checksum("crc32-le", EXPECTED_CRC32_LE_SUM).await
    }

    #[tokio::test]
    async fn test_crc32c_be() -> Result<()> {
        test_checksum("crc32c", EXPECTED_CRC32C_BE_SUM).await
    }

    #[tokio::test]
    async fn test_crc32c_le() -> Result<()> {
        test_checksum("crc32c-le", EXPECTED_CRC32C_LE_SUM).await
    }
}
