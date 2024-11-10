//! Checksum calculation and logic.
//!

use crate::error::Result;
use crate::{Checksum, Endianness};
use crc32c::crc32c_append;
use futures_util::{pin_mut, Stream, StreamExt};
use sha1::Digest;
use std::sync::Arc;

/// The checksum calculator.
pub enum ChecksumCtx {
    /// Calculate the MD5 checksum.
    MD5(md5::Md5),
    /// Calculate the SHA1 checksum.
    SHA1(sha1::Sha1),
    /// Calculate the SHA256 checksum.
    SHA256(sha2::Sha256),
    /// Calculate the AWS ETag.
    AWSETag,
    /// Calculate a CRC32.
    CRC32(crc32fast::Hasher, Endianness),
    CRC32C(u32, Endianness),
    /// Calculate the QuickXor checksum.
    QuickXor,
}

impl From<Checksum> for ChecksumCtx {
    fn from(checksum: Checksum) -> Self {
        match checksum {
            Checksum::MD5 => Self::MD5(md5::Md5::new()),
            Checksum::SHA1 => Self::SHA1(sha1::Sha1::new()),
            Checksum::SHA256 => Self::SHA256(sha2::Sha256::new()),
            Checksum::AWSETag => todo!(),
            Checksum::CRC32 => Self::CRC32(crc32fast::Hasher::new(), Endianness::BigEndian),
            Checksum::CRC32C => Self::CRC32C(0, Endianness::BigEndian),
            Checksum::QuickXor => todo!(),
        }
    }
}

impl ChecksumCtx {
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
            ChecksumCtx::MD5(ctx) => ctx.update(data),
            ChecksumCtx::SHA1(ctx) => ctx.update(data),
            ChecksumCtx::SHA256(ctx) => ctx.update(data),
            ChecksumCtx::AWSETag => todo!(),
            ChecksumCtx::CRC32(ctx, _) => ctx.update(data),
            ChecksumCtx::CRC32C(ctx, _) => *ctx = crc32c_append(*ctx, data),
            ChecksumCtx::QuickXor => todo!(),
        }
    }

    /// Finalize the checksum.
    pub fn finalize(self) -> Vec<u8> {
        match self {
            ChecksumCtx::MD5(ctx) => ctx.finalize().to_vec(),
            ChecksumCtx::SHA1(ctx) => ctx.finalize().to_vec(),
            ChecksumCtx::SHA256(ctx) => ctx.finalize().to_vec(),
            ChecksumCtx::AWSETag => todo!(),
            ChecksumCtx::CRC32(ctx, endianness) => match endianness {
                Endianness::LittleEndian => ctx.finalize().to_le_bytes().to_vec(),
                Endianness::BigEndian => ctx.finalize().to_be_bytes().to_vec(),
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
        mut self,
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
    use crate::Checksum;
    use anyhow::Result;
    use hex::encode;
    use tokio::fs::File;
    use tokio::join;

    #[tokio::test]
    async fn test_md5() -> Result<()> {
        test_checksum(Checksum::MD5, Endianness::BigEndian, expected_md5_sum()).await
    }

    #[tokio::test]
    async fn test_sha1() -> Result<()> {
        test_checksum(Checksum::SHA1, Endianness::BigEndian, expected_sha1_sum()).await
    }

    #[tokio::test]
    async fn test_sha256() -> Result<()> {
        test_checksum(
            Checksum::SHA256,
            Endianness::BigEndian,
            expected_sha256_sum(),
        )
        .await
    }

    #[tokio::test]
    async fn test_crc32_be() -> Result<()> {
        test_checksum(Checksum::CRC32, Endianness::BigEndian, expected_crc32_be()).await
    }

    #[tokio::test]
    async fn test_crc32_le() -> Result<()> {
        test_checksum(
            Checksum::CRC32,
            Endianness::LittleEndian,
            expected_crc32_le(),
        )
        .await
    }

    #[tokio::test]
    async fn test_crc32c_be() -> Result<()> {
        test_checksum(
            Checksum::CRC32C,
            Endianness::BigEndian,
            expected_crc32c_be(),
        )
        .await
    }

    #[tokio::test]
    async fn test_crc32c_le() -> Result<()> {
        test_checksum(
            Checksum::CRC32C,
            Endianness::LittleEndian,
            expected_crc32c_le(),
        )
        .await
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

    async fn test_checksum(
        checksum: Checksum,
        endianness: Endianness,
        expected: &str,
    ) -> Result<()> {
        let test_file = TestFileBuilder::default().generate_test_defaults()?;
        let mut reader = channel_reader(File::open(test_file).await?).await;

        let checksum = ChecksumCtx::from(checksum).with_endianness(endianness);

        let stream = reader.as_stream();
        let task = tokio::spawn(async move { reader.read_task().await });

        let (digest, _) = join!(checksum.generate(stream), task);

        assert_eq!(expected, encode(digest?));

        Ok(())
    }
}
