//! Checksum calculation and logic.
//!

use crate::error::Result;
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
    CRC32,
    /// Calculate the QuickXor checksum.
    QuickXor,
}

impl From<crate::Checksum> for ChecksumCtx {
    fn from(checksum: crate::Checksum) -> Self {
        match checksum {
            crate::Checksum::MD5 => Self::MD5(md5::Md5::new()),
            crate::Checksum::SHA1 => Self::SHA1(sha1::Sha1::new()),
            crate::Checksum::SHA256 => Self::SHA256(sha2::Sha256::new()),
            crate::Checksum::AWSETag => todo!(),
            crate::Checksum::CRC32 => todo!(),
            crate::Checksum::QuickXor => todo!(),
        }
    }
}

impl ChecksumCtx {
    /// Update a checksum with some data.
    pub fn update(&mut self, data: &[u8]) {
        match self {
            ChecksumCtx::MD5(ctx) => ctx.update(data),
            ChecksumCtx::SHA1(ctx) => ctx.update(data),
            ChecksumCtx::SHA256(ctx) => ctx.update(data),
            ChecksumCtx::AWSETag => todo!(),
            ChecksumCtx::CRC32 => todo!(),
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
            ChecksumCtx::CRC32 => todo!(),
            ChecksumCtx::QuickXor => todo!(),
        }
    }

    /// Generate a checksum from a stream of bytes.
    pub async fn generate(
        mut self,
        stream: impl Stream<Item = Result<Arc<[u8]>>>,
    ) -> Result<Vec<u8>> {
        pin_mut!(stream);

        println!("update chunk");
        while let Some(chunk) = stream.next().await {
            println!("update chunk: {:?}", chunk);
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
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_md5() -> Result<()> {
        let mut ctx = md5::Md5::new();
        let mut data = vec![];
        let mut f = File::open("../data/test_file").await?;
        f.read_to_end(&mut data).await?;

        ctx.update(data);

        let d = ctx.finalize();
        println!("{}", encode(d));

        test_checksum(Checksum::MD5, expected_md5_sum()).await
    }

    #[tokio::test]
    async fn test_sha1() -> Result<()> {
        test_checksum(Checksum::SHA1, expected_sha1_sum()).await
    }

    #[tokio::test]
    async fn test_sha256() -> Result<()> {
        test_checksum(Checksum::SHA256, expected_sha256_sum()).await
    }

    pub(crate) fn expected_md5_sum() -> &'static str {
        "d889d6c2b0bb0efc473ce1c9233a6078"
    }

    pub(crate) fn expected_sha1_sum() -> &'static str {
        "3eafdb6ad3a27167e0db70fccc40d0614307dabf"
    }

    pub(crate) fn expected_sha256_sum() -> &'static str {
        "29ffbd53cbe43179ab2fa62dbd958c0ec30b340ab50ce7c785e8a7a4b4771e39"
    }

    async fn test_checksum(checksum: Checksum, expected: &str) -> Result<()> {
        let test_file = TestFileBuilder::default().generate_test_defaults()?;
        let mut reader = channel_reader(File::open(test_file).await?).await;

        let checksum = ChecksumCtx::from(checksum);

        let stream = reader.to_stream();
        reader.read_task().await?;

        let digest = checksum.generate(stream).await?;

        assert_eq!(expected, encode(digest));

        Ok(())
    }
}
