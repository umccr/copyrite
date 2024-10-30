//! Checksum calculation and logic.
//!

use crate::error::Result;
use futures_util::{pin_mut, Stream, StreamExt};
use sha1::Digest;
use std::sync::Arc;

/// The checksum calculator.
pub enum Checksum {
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

impl From<crate::Checksum> for Checksum {
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

impl Checksum {
    /// Update a checksum with some data.
    pub fn update(&mut self, data: &[u8]) {
        match self {
            Checksum::MD5(ctx) => ctx.update(data),
            Checksum::SHA1(ctx) => ctx.update(data),
            Checksum::SHA256(ctx) => ctx.update(data),
            Checksum::AWSETag => todo!(),
            Checksum::CRC32 => todo!(),
            Checksum::QuickXor => todo!(),
        }
    }

    /// Finalize the checksum.
    pub fn finalize(self) -> Vec<u8> {
        match self {
            Checksum::MD5(ctx) => ctx.finalize().to_vec(),
            Checksum::SHA1(ctx) => ctx.finalize().to_vec(),
            Checksum::SHA256(ctx) => ctx.finalize().to_vec(),
            Checksum::AWSETag => todo!(),
            Checksum::CRC32 => todo!(),
            Checksum::QuickXor => todo!(),
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
