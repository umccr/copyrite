//! Compute a checksum using an AWS ETag style, i.e. combined checksums
//! of the parts of a file.
//!

use crate::checksum::Ctx;
use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::num::ParseIntError;
use std::str::FromStr;
use std::sync::Arc;

/// Calculate checksums using an AWS ETag style.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct AWSEtagCtx {
    part_size: u64,
    current_bytes: u64,
    remainder: Option<Arc<[u8]>>,
    part_checksums: Vec<Vec<u8>>,
    checksummer: Ctx,
}

impl AWSEtagCtx {
    /// Create a new checksummer.
    pub fn new(checksummer: Ctx, part_size: u64) -> Self {
        Self {
            part_size,
            current_bytes: 0,
            remainder: None,
            part_checksums: vec![],
            checksummer,
        }
    }

    /// Update using data.
    pub fn update(&mut self, data: Arc<[u8]>) {
        if self.current_bytes + data.len() as u64 > self.part_size {
            let (data, remainder) = data.split_at((self.part_size - self.current_bytes) as usize);

            self.current_bytes = remainder.len() as u64;
            self.remainder = Some(Arc::from(remainder));

            self.checksummer.update(Arc::from(data));

            self.part_checksums.push(self.checksummer.finalize());

            self.checksummer = self.checksummer.reset();
        } else {
            let remainder = self.remainder.take();
            if let Some(remainder) = remainder {
                self.checksummer.update(remainder);
                self.remainder = None;
            }

            self.checksummer.update(data);
        }
    }

    /// Finalize the checksum.
    pub fn finalize(&mut self) -> Vec<u8> {
        let concat: Vec<_> = self
            .part_checksums
            .drain(0..self.part_checksums.len())
            .flatten()
            .collect();
        self.checksummer.update(Arc::from(concat.as_slice()));
        self.checksummer.finalize()
    }

    /// Parse into a `ChecksumCtx` for values that use endianness. Parses an -aws-<n> suffix,
    /// where n represents the part size to calculate.
    pub fn parse_part_size(s: &str) -> Result<(&str, u64)> {
        let mut iter = s.rsplitn(2, "-aws-");
        let parsed = iter.next().map(|size| {
            (
                iter.next()
                    .ok_or_else(|| ParseError("expected checksum type".to_string())),
                size.parse()
                    .map_err(|err: ParseIntError| ParseError(err.to_string())),
            )
        });
        match parsed {
            Some((s, size)) => Ok((s?, size?)),
            None => Err(ParseError(
                "expected aws etag checksum part size".to_string(),
            )),
        }
    }

    /// Get the digest output.
    pub fn digest_to_string(&self, digest: Vec<u8>) -> String {
        STANDARD.encode(digest)
    }
}

impl FromStr for AWSEtagCtx {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (s, size) = Self::parse_part_size(s)?;
        let ctx = Ctx::from_str(s)?;

        Ok(AWSEtagCtx::new(ctx, size))
    }
}

impl Display for AWSEtagCtx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.checksummer, self.part_size)
    }
}
