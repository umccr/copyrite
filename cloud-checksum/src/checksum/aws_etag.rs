//! Compute a checksum using an AWS ETag style, i.e. combined checksums
//! of the parts of a file.
//!

use crate::checksum::standard::StandardCtx;
use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::str::FromStr;
use std::sync::Arc;

/// Calculate checksums using an AWS ETag style.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AWSETagCtx {
    part_size: u64,
    current_bytes: u64,
    remainder: Option<Arc<[u8]>>,
    part_checksums: Vec<Vec<u8>>,
    n_checksums: u64,
    ctx: StandardCtx,
}

impl AWSETagCtx {
    /// Create a new checksummer.
    pub fn new(ctx: StandardCtx, part_size: u64) -> Self {
        Self {
            part_size,
            current_bytes: 0,
            remainder: None,
            part_checksums: vec![],
            n_checksums: 0,
            ctx,
        }
    }

    /// Update using data.
    pub fn update(&mut self, data: Arc<[u8]>) -> Result<()> {
        let len = u64::try_from(data.len())?;
        if self.current_bytes + len > self.part_size {
            // If the current byte position is greater than the part size, then split into a new
            // part checksum.
            let (data, remainder) =
                data.split_at(usize::try_from(self.part_size - self.current_bytes)?);

            self.current_bytes = u64::try_from(remainder.len())?;
            self.remainder = Some(Arc::from(remainder));

            self.ctx.update(Arc::from(data))?;

            self.part_checksums.push(self.ctx.finalize()?);

            // Reset the context for next chunk.
            self.ctx = self.ctx.reset();
        } else {
            // Otherwise update as usual, tracking the byte position.
            self.update_with_remainder()?;

            self.current_bytes += len;
            self.ctx.update(data)?;
        }

        Ok(())
    }

    /// Update the checksummer context with remainder bytes.
    fn update_with_remainder(&mut self) -> Result<()> {
        let remainder = self.remainder.take();
        if let Some(remainder) = remainder {
            self.ctx.update(remainder)?;
            self.remainder = None;
        }
        Ok(())
    }

    /// Finalize the checksum.
    pub fn finalize(&mut self) -> Result<Vec<u8>> {
        // Add the last part checksum.
        if self.remainder.is_some() || self.current_bytes != 0 {
            self.update_with_remainder()?;
            self.part_checksums.push(self.ctx.finalize()?);

            // Reset the context for merged chunks.
            self.ctx = self.ctx.reset();
        }

        // Then merge the part checksums and compute a single checksum.
        self.n_checksums = u64::try_from(self.part_checksums.len())?;
        let concat: Vec<u8> = self.part_checksums.iter().flatten().copied().collect();

        self.ctx.update(Arc::from(concat.as_slice()))?;
        self.ctx.finalize()
    }

    /// Parse into a `ChecksumCtx` for values that use endianness. Parses an -aws-<n> suffix,
    /// where n represents the part size to calculate.
    pub fn parse_part_size(s: &str, file_size: Option<u64>) -> Result<(String, u64)> {
        // Support an alias of aws-etag for md5.
        let s = s.replace("aws-etag", "md5-aws");

        let mut iter = s.rsplitn(2, "-aws-");
        let part_size = iter
            .next()
            .ok_or_else(|| ParseError("expected part size".to_string()))?;
        let part_size = part_size.strip_prefix("etag-").unwrap_or(part_size);

        let part_size = if let Ok(part_number) = part_size.parse::<u64>() {
            if let Some(file_size) = file_size {
                if file_size < 1 {
                    return Err(ParseError("cannot use zero part number".to_string()));
                }
                file_size.div_ceil(part_number)
            } else {
                return Err(ParseError(
                    "cannot use part number syntax without file size".to_string(),
                ));
            }
        } else {
            parse_size::parse_size(part_size).map_err(|err| ParseError(err.to_string()))?
        };

        let algorithm = iter
            .next()
            .ok_or_else(|| ParseError("expected checksum algorithm".to_string()))?;

        Ok((algorithm.to_string(), part_size))
    }

    /// Get the digest output.
    pub fn digest_to_string(&self, digest: &[u8]) -> String {
        format!("{}-{}", self.ctx.digest_to_string(digest), self.n_checksums)
    }

    /// Get the part size.
    pub fn part_size(&self) -> u64 {
        self.part_size
    }

    /// Get the encoded part checksums.
    pub fn part_checksums(&self) -> Vec<String> {
        self.part_checksums
            .iter()
            .map(|digest| self.ctx.digest_to_string(digest))
            .collect()
    }
}

impl FromStr for AWSETagCtx {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Self::try_from((s, None))
    }
}

impl TryFrom<(&str, Option<u64>)> for AWSETagCtx {
    type Error = Error;

    fn try_from((s, file_size): (&str, Option<u64>)) -> Result<Self> {
        let (s, size) = Self::parse_part_size(s, file_size)?;
        let ctx = StandardCtx::from_str(&s)?;

        Ok(AWSETagCtx::new(ctx, size))
    }
}

impl Display for AWSETagCtx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ctx, self.n_checksums)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::checksum::test::test_checksum;
    use anyhow::Result;

    pub(crate) fn expected_md5_1gib() -> &'static str {
        "6c434b38867bbd608ba2f06e92ed4e43-1"
    }

    pub(crate) fn expected_md5_100mib() -> &'static str {
        "e5727bb1cb678220f6782ff6cb927569-11"
    }

    pub(crate) fn expected_md5_10() -> &'static str {
        "9a9666a5c313c53fbc3a3ea1d43cc981-10"
    }

    pub(crate) fn expected_sha256_100mib() -> &'static str {
        "a9ed6c4b6aadf887f90a3d483b5c5b79bc08075af2a1718e3e15c63b9904ebf7-11"
    }

    #[tokio::test]
    async fn test_aws_etag_single_part() -> Result<()> {
        test_checksum("md5-aws-1gib", expected_md5_1gib()).await?;
        test_checksum("aws-etag-1gib", expected_md5_1gib()).await?;

        // Larger part sizes should also work.
        test_checksum("md5-aws-2gib", expected_md5_1gib()).await?;
        test_checksum("aws-etag-2gib", expected_md5_1gib()).await
    }

    #[tokio::test]
    async fn test_aws_etag_md5() -> Result<()> {
        test_checksum("md5-aws-100mib", expected_md5_100mib()).await?;
        test_checksum("aws-etag-100mib", expected_md5_100mib()).await
    }

    #[tokio::test]
    async fn test_aws_etag_sha256() -> Result<()> {
        test_checksum("sha256-aws-100mib", expected_sha256_100mib()).await
    }

    #[tokio::test]
    async fn test_aws_etag_part_number() -> Result<()> {
        test_checksum("md5-aws-10", expected_md5_10()).await?;
        test_checksum("aws-etag-10", expected_md5_10()).await
    }
}
