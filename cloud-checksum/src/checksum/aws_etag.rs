//! Compute a checksum using an AWS ETag style, i.e. combined checksums
//! of the parts of a file.
//!

use crate::checksum::standard::StandardCtx;
use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

/// Calculate checksums using an AWS ETag style.
#[derive(Debug, Clone)]
pub struct AWSETagCtx {
    part_mode: PartMode,
    part_size_index: usize,
    current_part_size: u64,
    current_bytes: u64,
    remainder: Option<Arc<[u8]>>,
    part_checksums: Vec<(u64, Vec<u8>)>,
    n_checksums: u64,
    ctx: StandardCtx,
    file_size: Option<u64>,
}

impl Ord for AWSETagCtx {
    fn cmp(&self, other: &Self) -> Ordering {
        (&self.part_mode, &self.ctx).cmp(&(&other.part_mode, &other.ctx))
    }
}

impl PartialOrd for AWSETagCtx {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for AWSETagCtx {}

impl PartialEq for AWSETagCtx {
    fn eq(&self, other: &Self) -> bool {
        self.part_mode == other.part_mode && self.ctx == other.ctx
    }
}

impl Hash for AWSETagCtx {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.part_mode.hash(state);
        self.current_bytes.hash(state);
    }
}

/// The mode to operate aws etags in. Part numbers calculate parts using the total file size.
/// Part sizes can operate without the file size.
#[derive(Debug, Clone, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum PartMode {
    PartNumber(u64),
    PartSizes(Vec<u64>),
}

impl AWSETagCtx {
    /// Create a new checksummer.
    pub fn new(ctx: StandardCtx, part_mode: PartMode, file_size: Option<u64>) -> Result<Self> {
        let mut new = Self {
            part_mode,
            part_size_index: 0,
            current_part_size: 0,
            current_bytes: 0,
            remainder: None,
            part_checksums: vec![],
            n_checksums: 0,
            ctx,
            file_size,
        };

        new.current_part_size = new.next_part_size()?;

        Ok(new)
    }

    /// Update using data.
    pub fn update(&mut self, data: Arc<[u8]>) -> Result<()> {
        let len = u64::try_from(data.len())?;
        if self.current_bytes + len > self.current_part_size {
            // If the current byte position is greater than the part size, then split into a new
            // part checksum.
            let (data, remainder) = data.split_at(usize::try_from(
                self.current_part_size - self.current_bytes,
            )?);

            self.ctx.update(Arc::from(data))?;
            self.part_checksums
                .push((self.current_part_size, self.ctx.finalize()?));

            // Reset the current bytes and any remainder bytes.
            self.current_bytes = u64::try_from(remainder.len())?;
            self.remainder = Some(Arc::from(remainder));

            // Reset the context for next chunk.
            self.ctx = self.ctx.reset();

            // Update the part size.
            self.current_part_size = self.next_part_size()?;
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
            self.part_checksums
                .push((self.current_bytes, self.ctx.finalize()?));

            // Reset the context for merged chunks.
            self.ctx = self.ctx.reset();
        }

        // Then merge the part checksums and compute a single checksum.
        self.n_checksums = u64::try_from(self.part_checksums.len())?;
        let concat: Vec<u8> = self
            .part_checksums
            .iter()
            .flat_map(|(_, sum)| sum)
            .copied()
            .collect();

        self.ctx.update(Arc::from(concat.as_slice()))?;
        self.ctx.finalize()
    }

    /// Parse into a `ChecksumCtx` for values that use endianness. Parses an -aws-<n> suffix,
    /// where n represents the part size to calculate.
    pub fn parse_part_size(s: &str) -> Result<(String, PartMode)> {
        // Support an alias of aws-etag for md5.
        let mut s = s.replace("aws-etag", "md5-aws");

        // If no part size has been specified default to 1.
        if s == "md5-aws" {
            s = "md5-aws-1".to_string();
        }

        let mut iter = s.rsplitn(2, "-aws-");

        let part_sizes = iter
            .next()
            .ok_or_else(|| ParseError("expected part size".to_string()))?;
        let part_sizes = part_sizes.strip_prefix("etag-").unwrap_or(part_sizes);

        // Try a part number first, otherwise use part sizes.
        let part_mode = if let Ok(part_number) = part_sizes.parse::<u64>() {
            if part_number == 0 {
                return Err(ParseError("cannot use zero part number".to_string()));
            }

            PartMode::PartNumber(part_number)
        } else {
            // Allow multiple part sizes to be specified separated with a dash.
            let part_sizes = part_sizes
                .split("-")
                .map(|part| parse_size::parse_size(part).map_err(|err| ParseError(err.to_string())))
                .collect::<Result<Vec<_>>>()?;

            PartMode::PartSizes(part_sizes)
        };

        let algorithm = iter
            .next()
            .ok_or_else(|| ParseError("expected checksum algorithm".to_string()))?;

        Ok((algorithm.to_string(), part_mode))
    }

    /// Get the digest output.
    pub fn digest_to_string(&self, digest: &[u8]) -> String {
        format!("{}-{}", self.ctx.digest_to_string(digest), self.n_checksums)
    }

    /// Get the next part size.
    pub fn next_part_size(&mut self) -> Result<u64> {
        match (&self.part_mode, &self.file_size) {
            (PartMode::PartSizes(part_sizes), _) => {
                // Get the part size based on the index.
                let part_size = part_sizes
                    .get(self.part_size_index)
                    .ok_or_else(|| ParseError("expected part size".to_string()))?;

                // If we reach the end, just return the last value.
                if self.part_size_index != part_sizes.len() - 1 {
                    self.part_size_index += 1;
                }

                Ok(*part_size)
            }
            (PartMode::PartNumber(part_number), Some(file_size)) => {
                Ok(file_size.div_ceil(*part_number))
            }
            _ => Err(ParseError(
                "cannot use part number syntax without file size".to_string(),
            )),
        }
    }

    /// Set the file size.
    pub fn set_file_size(&mut self, file_size: Option<u64>) {
        self.file_size = file_size;
    }

    /// Get the encoded part checksums and their part sizes.
    pub fn part_checksums(&self) -> Vec<(u64, String)> {
        self.part_checksums
            .iter()
            .map(|(part_size, digest)| (*part_size, self.ctx.digest_to_string(digest)))
            .collect()
    }
}

impl FromStr for AWSETagCtx {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (s, part_mode) = Self::parse_part_size(s)?;
        let ctx = StandardCtx::from_str(&s)?;

        AWSETagCtx::new(ctx, part_mode, None)
    }
}

impl Display for AWSETagCtx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let parts = match self.part_mode {
            PartMode::PartNumber(part_number) => part_number.to_string(),
            PartMode::PartSizes(ref part_sizes) => part_sizes
                .iter()
                .map(|part| part.to_string())
                .collect::<Vec<_>>()
                .join("-"),
        };

        write!(f, "{}-aws-{}", self.ctx, parts)
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
