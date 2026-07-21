//! Standard checksum algorithms
//!

use crate::cli::{Checksum, Endianness};
use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use crate::io::Provider;
use crc32c::crc32c_append;
use md5::Digest;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::mem::discriminant;
use std::str::FromStr;
use std::sync::Arc;
use xxhash_rust::xxh3::Xxh3Default;
use xxhash_rust::xxh64::Xxh64;

/// The checksum calculator. This also defines the ordering of which checksums are preferred
/// for generating/copying data.
#[derive(Clone)]
pub enum StandardCtx {
    CRC64NVME(Option<crc64fast_nvme::Digest>, Endianness),
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
    /// Calculate the SHA512 checksum.
    SHA512(Option<sha2::Sha512>),
    /// Calculate the XXHash64 checksum.
    XXHash64(Option<Xxh64>),
    /// Calculate the XXHash3 64-bit checksum.
    XXHash3(Option<Xxh3Default>),
    /// Calculate the XXHash128 checksum.
    XXHash128(Option<Xxh3Default>),
    /// Calculate the QuickXor checksum.
    QuickXor,
}

impl Debug for StandardCtx {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
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
        Self::crc64nvme()
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
            Checksum::SHA512 => Self::sha512(),
            Checksum::CRC32 => Self::crc32(),
            Checksum::CRC32C => Self::crc32c(),
            Checksum::CRC64NVME => Self::crc64nvme(),
            Checksum::XXHash64 => Self::xxhash64(),
            Checksum::XXHash3 => Self::xxhash3(),
            Checksum::XXHash128 => Self::xxhash128(),
            _ => return Err(ParseError("unsupported checksum algorithm".to_string())),
        };
        Ok(ctx)
    }
}

impl From<&StandardCtx> for Checksum {
    fn from(checksum: &StandardCtx) -> Self {
        match checksum {
            StandardCtx::CRC64NVME(_, _) => Self::CRC64NVME,
            StandardCtx::MD5(_) => Self::MD5,
            StandardCtx::SHA1(_) => Self::SHA1,
            StandardCtx::SHA256(_) => Self::SHA256,
            StandardCtx::SHA512(_) => Self::SHA512,
            StandardCtx::CRC32(_, _) => Self::CRC32,
            StandardCtx::CRC32C(_, _) => Self::CRC32C,
            StandardCtx::XXHash64(_) => Self::XXHash64,
            StandardCtx::XXHash3(_) => Self::XXHash3,
            StandardCtx::XXHash128(_) => Self::XXHash128,
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
            StandardCtx::SHA512(_) => write!(f, "sha512"),
            // Noting big-endian is the default if left unspecified.
            StandardCtx::CRC32(_, endianness) => match endianness {
                Endianness::LittleEndian => write!(f, "crc32-{}", endianness),
                Endianness::BigEndian => write!(f, "crc32"),
            },
            StandardCtx::CRC32C(_, endianness) => match endianness {
                Endianness::LittleEndian => write!(f, "crc32c-{}", endianness),
                Endianness::BigEndian => write!(f, "crc32c"),
            },
            StandardCtx::CRC64NVME(_, endianness) => match endianness {
                Endianness::LittleEndian => write!(f, "crc64nvme-{}", endianness),
                Endianness::BigEndian => write!(f, "crc64nvme"),
            },
            StandardCtx::XXHash64(_) => write!(f, "xxhash64"),
            StandardCtx::XXHash3(_) => write!(f, "xxhash3"),
            StandardCtx::XXHash128(_) => write!(f, "xxhash128"),
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

    /// Create the SHA512 variant.
    pub fn sha512() -> Self {
        Self::SHA512(Some(sha2::Sha512::new()))
    }

    /// Create the CRC32 variant.
    pub fn crc32() -> Self {
        Self::CRC32(Some(crc32fast::Hasher::new()), Endianness::BigEndian)
    }

    /// Create the CRC32C variant.
    pub fn crc32c() -> Self {
        Self::CRC32C(0, Endianness::BigEndian)
    }

    /// Create the CRC64NVME variant.
    pub fn crc64nvme() -> Self {
        Self::CRC64NVME(Some(crc64fast_nvme::Digest::new()), Endianness::BigEndian)
    }

    /// Create the XXHash64 variant.
    pub fn xxhash64() -> Self {
        Self::XXHash64(Some(Xxh64::new(0)))
    }

    /// Create the XXHash3 64-bit variant.
    pub fn xxhash3() -> Self {
        Self::XXHash3(Some(Xxh3Default::new()))
    }

    /// Create the XXHash128 variant.
    pub fn xxhash128() -> Self {
        Self::XXHash128(Some(Xxh3Default::new()))
    }

    /// Parse into a `ChecksumCtx` for values that use endianness. Uses an -le suffix for
    /// little-endian and -be for big-endian.
    pub fn parse_endianness(s: &str) -> Result<Option<Self>> {
        if let Some(s) = s.strip_suffix("-le") {
            let ctx = match Checksum::from_str(s)? {
                Checksum::CRC32 => Self::crc32().with_endianness(Endianness::LittleEndian),
                Checksum::CRC32C => Self::crc32c().with_endianness(Endianness::LittleEndian),
                Checksum::CRC64NVME => Self::crc64nvme().with_endianness(Endianness::LittleEndian),
                _ => return Err(ParseError("invalid suffix -le for checksum".to_string())),
            };
            Ok(Some(ctx))
        } else if let Some(s) = s.strip_suffix("-be") {
            let ctx = match Checksum::from_str(s)? {
                Checksum::CRC32 => Self::crc32(),
                Checksum::CRC32C => Self::crc32c(),
                Checksum::CRC64NVME => Self::crc64nvme(),
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
            Self::CRC64NVME(ctx, _) => Self::CRC64NVME(ctx, endianness),
            checksum => checksum,
        }
    }

    /// Update a checksum with some data.
    pub fn update(&mut self, data: Arc<[u8]>) -> Result<()> {
        match self {
            StandardCtx::MD5(Some(ctx)) => ctx.update(data),
            StandardCtx::SHA1(Some(ctx)) => ctx.update(data),
            StandardCtx::SHA256(Some(ctx)) => ctx.update(data),
            StandardCtx::SHA512(Some(ctx)) => ctx.update(data),
            StandardCtx::CRC32(Some(ctx), _) => ctx.update(&data),
            StandardCtx::CRC32C(ctx, _) => *ctx = crc32c_append(*ctx, &data),
            StandardCtx::CRC64NVME(Some(ctx), _) => ctx.write(&data),
            StandardCtx::XXHash64(Some(ctx)) => ctx.update(&data),
            StandardCtx::XXHash3(Some(ctx)) => ctx.update(&data),
            StandardCtx::XXHash128(Some(ctx)) => ctx.update(&data),
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
            StandardCtx::SHA512(ctx) => ctx.take().expect(msg).finalize().to_vec(),
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
            StandardCtx::CRC64NVME(ctx, endianness) => match endianness {
                Endianness::LittleEndian => ctx.take().expect(msg).finish().to_le_bytes().to_vec(),
                Endianness::BigEndian => ctx.take().expect(msg).finish().to_be_bytes().to_vec(),
            },
            StandardCtx::XXHash64(ctx) => {
                ctx.take().expect(msg).digest().to_be_bytes().to_vec()
            }
            StandardCtx::XXHash3(ctx) => {
                ctx.take().expect(msg).digest().to_be_bytes().to_vec()
            }
            StandardCtx::XXHash128(ctx) => {
                ctx.take().expect(msg).digest128().to_be_bytes().to_vec()
            }
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
            StandardCtx::SHA512(_) => Self::sha512(),
            StandardCtx::CRC32(_, endianness) => Self::crc32().with_endianness(*endianness),
            StandardCtx::CRC32C(_, endianness) => Self::crc32c().with_endianness(*endianness),
            StandardCtx::CRC64NVME(_, endianness) => Self::crc64nvme().with_endianness(*endianness),
            StandardCtx::XXHash64(_) => Self::xxhash64(),
            StandardCtx::XXHash3(_) => Self::xxhash3(),
            StandardCtx::XXHash128(_) => Self::xxhash128(),
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
            StandardCtx::CRC32(_, endianness)
            | StandardCtx::CRC32C(_, endianness)
            | StandardCtx::CRC64NVME(_, endianness) => Some(*endianness),
            _ => None,
        }
    }

    /// Get the numeric value of the enum.
    pub fn to_u8(&self) -> u8 {
        match self {
            StandardCtx::CRC64NVME(_, _) => 1,
            StandardCtx::CRC32C(_, _) => 2,
            StandardCtx::CRC32(_, _) => 3,
            StandardCtx::MD5(_) => 4,
            StandardCtx::SHA1(_) => 5,
            StandardCtx::SHA256(_) => 6,
            StandardCtx::SHA512(_) => 7,
            StandardCtx::XXHash64(_) => 8,
            StandardCtx::XXHash3(_) => 9,
            StandardCtx::XXHash128(_) => 10,
            StandardCtx::QuickXor => 11,
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
    use super::StandardCtx;
    use crate::checksum::test::test_checksum;
    use anyhow::Result;
    use std::str::FromStr;

    pub(crate) const EXPECTED_MD5_SUM: &str = "d93e71879054f205ede90d35c8081ca5"; // pragma: allowlist secret
    pub(crate) const EXPECTED_SHA1_SUM: &str = "3eafdb6ad3a27167e0db70fccc40d0614307dabf"; // pragma: allowlist secret
    pub(crate) const EXPECTED_SHA256_SUM: &str =
        "29ffbd53cbe43179ab2fa62dbd958c0ec30b340ab50ce7c785e8a7a4b4771e39"; // pragma: allowlist secret
    pub(crate) const EXPECTED_SHA512_SUM: &str =
        "601bda6e0b7f39f8ed92aa4d9125b34c0321b6eb36622dcf0c8ed96847693e55fdd8f083b56746629369752d5ec6566a61eca2d41796245784595b3a6cf52f1e"; // pragma: allowlist secret
    pub(crate) const EXPECTED_CRC32_BE_SUM: &str = "3320f39e";
    pub(crate) const EXPECTED_CRC32_LE_SUM: &str = "9ef32033";
    pub(crate) const EXPECTED_CRC32C_BE_SUM: &str = "4920106a";
    pub(crate) const EXPECTED_CRC32C_LE_SUM: &str = "6a102049";
    pub(crate) const EXPECTED_XXHASH64_SUM: &str = "fde75bc952b2835f";
    pub(crate) const EXPECTED_XXHASH3_SUM: &str = "3e714f0e42a90f5f";
    pub(crate) const EXPECTED_XXHASH128_SUM: &str = "01c124e0c0eaf1903e714f0e42a90f5f";

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

    #[tokio::test]
    async fn test_sha512() -> Result<()> {
        test_checksum("sha512", EXPECTED_SHA512_SUM).await
    }

    #[tokio::test]
    async fn test_xxhash64() -> Result<()> {
        test_checksum("xxhash64", EXPECTED_XXHASH64_SUM).await
    }

    #[tokio::test]
    async fn test_xxhash3() -> Result<()> {
        test_checksum("xxhash3", EXPECTED_XXHASH3_SUM).await
    }

    #[tokio::test]
    async fn test_xxhash128() -> Result<()> {
        test_checksum("xxhash128", EXPECTED_XXHASH128_SUM).await
    }

    #[test]
    fn test_xxhash64_known() -> Result<()> {
        assert_eq!(
            hex::encode(StandardCtx::xxhash64().finalize()?),
            "ef46db3751d8e999"
        );
        Ok(())
    }

    #[test]
    fn test_xxhash3_known() -> Result<()> {
        assert_eq!(
            hex::encode(StandardCtx::xxhash3().finalize()?),
            "2d06800538d394c2"
        );
        Ok(())
    }

    #[test]
    fn test_xxhash128_known() -> Result<()> {
        assert_eq!(
            hex::encode(StandardCtx::xxhash128().finalize()?),
            "99aa06d3014798d86001c324468d497f"
        );
        Ok(())
    }

    #[test]
    fn test_new_checksums_name_round_trip() -> Result<()> {
        for name in ["sha512", "xxhash64", "xxhash3", "xxhash128"] {
            let ctx = StandardCtx::from_str(name)?;
            assert_eq!(ctx.to_string(), name);
        }
        Ok(())
    }
}
