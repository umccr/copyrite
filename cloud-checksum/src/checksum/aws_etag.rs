//! Compute a checksum using an AWS ETag style, i.e. combined checksums
//! of the parts of a file.
//!

use crate::checksum::standard::StandardCtx;
use crate::error::Error::ParseError;
use crate::error::{Error, Result};
use crate::io::Provider;
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::sync::Arc;

/// Constant for 1 MiB.
pub const MIB: u64 = 1024 * 1024;
/// Constant for 1 GiB.
pub const GIB: u64 = 1024 * 1024 * 1024;
/// Constant for 1 GB.
pub const GB: u64 = 1000 * 1000 * 1000;

/// Defines the "best" order for part sizes that should be preferenced when copying/generating
/// new checksums. This list takes into account defaults that are likely to show up in the AWS CLI
/// and SDKs.
pub const PREFERRED_PART_SIZES: &[u64] = &[
    // AWS CLI/boto3 uses 8MiB as the default:
    // https://github.com/aws/aws-cli/blob/b9459db122d9f596a4570b6b5ecca44311b48fc2/awscli/customizations/s3/transferconfig.py#L21
    // https://github.com/boto/boto3/blob/0442d32d2d2cbc5efbe158f6336993d1ee89b36b/boto3/s3/transfer.py#L243
    // Java V2 uses 8MiB:
    // https://github.com/aws/aws-sdk-java-v2/blob/5463dd3d403450e167408895c152c62884da65bd/services/s3/src/main/java/software/amazon/awssdk/services/s3/internal/multipart/MultipartConfigurationResolver.java#L28
    8 * MIB,
    // C++ SDK uses 5MiB:
    // https://github.com/aws/aws-sdk-cpp/blob/93f60cc8aad399a3977287485c19c24d15723b78/src/aws-cpp-sdk-transfer/include/aws/transfer/TransferManager.h#L35
    // Go seems to also default to 5MiB:
    // https://github.com/aws/aws-sdk-go-v2/blob/6fa167adb5d1a2618a2f9bbe8f2b885c0d7e2893/feature/s3/transfermanager/options.go#L12-L15
    // So does JavaScript:
    // https://github.com/aws/aws-sdk-js-v3/blob/11baa79dff0d9e8fddce873f5306613286990ded/lib/lib-storage/src/Upload.ts#L42
    // And .NET:
    // https://github.com/aws/aws-sdk-net/blob/0f72ad1c90f471505c0013c92ec5e9e567239527/sdk/src/Services/S3/Custom/Util/S3Constants.cs#L35
    // PHP:
    // https://github.com/aws/aws-sdk-php/blob/713bdb1ff0c2eb519932c111fc450f6b5462b69f/src/S3/MultipartUploader.php#L19
    5 * MIB,
    // Console uses 16mib:
    16 * MIB,
    // Ruby SDK uses 50MiB:
    // https://github.com/aws/aws-sdk-ruby/blob/2c8a0686794e8d03da504fcb25984f7fae93f5b3/gems/aws-sdk-s3/lib/aws-sdk-s3/object_multipart_copier.rb#L31
    50 * MIB,
    // Java V1 SDK uses 100MiB and 16mib:
    // https://github.com/aws/aws-sdk-java/blob/bdca0550fc15769618a51338f5f2f84bc603a1cf/aws-java-sdk-s3/src/main/java/com/amazonaws/services/s3/transfer/TransferManagerConfiguration.java#L32-L46
    // But it also does some calculations based on the total size:
    // https://github.com/aws/aws-sdk-java/blob/bdca0550fc15769618a51338f5f2f84bc603a1cf/aws-java-sdk-s3/src/main/java/com/amazonaws/services/s3/transfer/internal/TransferManagerUtils.java#L119-L125
    100 * MIB,
    // s3cmd uses 15mib:
    // https://github.com/s3tools/s3cmd/blob/8cb9b23992714b5ec22c1e514a50996e25aa333b/S3/Config.py#L196
    15 * MIB,
    // Some other options that a user might enter:
    10 * MIB,
    20 * MIB,
    200 * MIB,
    500 * MIB,
    GIB,
    2 * GIB,
    5 * GIB,
    1000 * MIB,
    2000 * MIB,
    5000 * MIB,
    GB,
    2 * GB,
    5 * GB,
];

/// Find the part size position in `PREFERRED_PART_SIZE`.
pub const fn part_size_position(part_size: u64) -> Option<usize> {
    let position = match part_size {
        s if s == const { PREFERRED_PART_SIZES[0] } => 1,
        s if s == const { PREFERRED_PART_SIZES[1] } => 2,
        s if s == const { PREFERRED_PART_SIZES[2] } => 3,
        s if s == const { PREFERRED_PART_SIZES[3] } => 4,
        s if s == const { PREFERRED_PART_SIZES[4] } => 5,
        s if s == const { PREFERRED_PART_SIZES[5] } => 6,
        s if s == const { PREFERRED_PART_SIZES[6] } => 7,
        s if s == const { PREFERRED_PART_SIZES[7] } => 8,
        s if s == const { PREFERRED_PART_SIZES[8] } => 9,
        s if s == const { PREFERRED_PART_SIZES[9] } => 10,
        s if s == const { PREFERRED_PART_SIZES[10] } => 11,
        s if s == const { PREFERRED_PART_SIZES[11] } => 12,
        s if s == const { PREFERRED_PART_SIZES[12] } => 13,
        s if s == const { PREFERRED_PART_SIZES[13] } => 14,
        s if s == const { PREFERRED_PART_SIZES[14] } => 15,
        s if s == const { PREFERRED_PART_SIZES[15] } => 16,
        s if s == const { PREFERRED_PART_SIZES[16] } => 17,
        s if s == const { PREFERRED_PART_SIZES[17] } => 18,
        s if s == const { PREFERRED_PART_SIZES[18] } => 19,
        _ => return None,
    };

    Some(position)
}

/// Calculate checksums using an AWS ETag style.
#[derive(Debug, Clone)]
pub struct AWSETagCtx {
    part_mode: PartMode,
    part_size_index: usize,
    current_part_size: u64,
    current_bytes: u64,
    total_bytes: u64,
    remainder: Option<Arc<[u8]>>,
    part_checksums: Vec<(u64, Vec<u8>)>,
    n_checksums: u64,
    ctx: StandardCtx,
    file_size: Option<u64>,
}

impl Ord for AWSETagCtx {
    fn cmp(&self, other: &Self) -> Ordering {
        let parts = self.get_part_sizes();
        let other_parts = other.get_part_sizes();

        // Always preference smaller/simpler part sizes over larger part sizes.
        if parts.len() != other_parts.len() {
            return (parts.len(), &self.ctx).cmp(&(other_parts.len(), &other.ctx));
        }

        // If there is only one part size, use the preferred part size ordering
        if parts.len() == 1 && other_parts.len() == 1 {
            let pos = part_size_position(parts[0]);
            let pos_other = part_size_position(other_parts[0]);

            if let (Some(pos), Some(pos_other)) = (pos, pos_other) {
                return (pos, &self.ctx).cmp(&(pos_other, &other.ctx));
            }
        }

        // Otherwise just compare normally using the full part size slice.
        (parts, &self.ctx).cmp(&(other_parts, &other.ctx))
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
        (self.get_part_sizes(), &self.ctx).eq(&(other.get_part_sizes(), &other.ctx))
    }
}

impl Hash for AWSETagCtx {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get_part_sizes().hash(state);
        self.ctx.hash(state);
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
    pub fn new(ctx: StandardCtx, part_mode: PartMode, file_size: Option<u64>) -> Self {
        Self {
            part_mode,
            part_size_index: 0,
            current_part_size: 0,
            current_bytes: 0,
            total_bytes: 0,
            remainder: None,
            part_checksums: vec![],
            n_checksums: 0,
            ctx,
            file_size,
        }
    }

    /// Update the part sizes so that they represent the correct part sizes for the file size.
    /// This takes two steps, first it iterates forward to determine the correct number of part
    /// sizes, and then it removes duplicate part sizes from the back as they are assumed to be
    /// repeated.
    pub fn update_part_sizes(&mut self) {
        let PartMode::PartSizes(part_sizes) = &mut self.part_mode else {
            return;
        };

        Self::iterate_part_sizes(self.file_size.unwrap_or(self.total_bytes), part_sizes);
        Self::remove_duplicates(part_sizes);
    }

    /// Iterate over the part sizes and remove duplicates from the end.
    fn remove_duplicates(part_sizes: &mut Vec<u64>) {
        // Iterate backwards to remove duplicate part sizes from the end only. This only
        // applies if there are at least two elements and the last element is smaller than the
        // second last element.
        let (Some(last), Some(second_last)) = (
            part_sizes.iter().nth_back(0).cloned(),
            part_sizes.iter().nth_back(1).cloned(),
        ) else {
            return;
        };
        if last > second_last {
            return;
        }

        // Ignore the last element as it can be smaller than the previous sizes.
        part_sizes.pop();

        // Only remove the second last element duplicates onwards.
        part_sizes.reverse();
        let mut done = false;
        part_sizes.retain(|part_size| {
            // Only remove one set of duplicates from the back, and then stop.
            if *part_size != second_last {
                done = true;
            }

            done || *part_size != second_last
        });
        part_sizes.reverse();

        // Add the removed elements back.
        part_sizes.push(second_last);
    }

    /// Iterate over the part sizes and correct the parts based on the file size.
    fn iterate_part_sizes(mut file_size: u64, part_sizes: &mut Vec<u64>) {
        // Iterate the part sizes until the end of the file is reached to find the
        // true ending part size.
        let mut remove_from = None;
        for (i, part_size) in part_sizes.iter_mut().enumerate() {
            // If the counter is less than the current part size, stop here, and set
            // the index to remove bytes from.
            if file_size <= *part_size {
                // The ending part size needs to be updated with the remaining bytes.
                *part_size = file_size;
                remove_from = Some(i + 1);
                file_size = file_size.saturating_sub(*part_size);
                break;
            }
            file_size = file_size.saturating_sub(*part_size);
        }

        // Remove the elements after iterating the counter.
        if let Some(remove_from) = remove_from {
            if let Some((keep, _)) = part_sizes.split_at_checked(remove_from) {
                *part_sizes = keep.to_vec();
            }
        }

        // Add back in whatever is left in the counter to ensure that the following code works
        // properly.
        let last = *part_sizes.last().unwrap_or(&0);
        while file_size > 0 {
            if file_size < last {
                part_sizes.push(file_size);
            } else {
                part_sizes.push(last);
            }
            file_size = file_size.saturating_sub(last);
        }
    }

    /// Update using data.
    pub fn update(&mut self, data: Arc<[u8]>) -> Result<()> {
        let len = u64::try_from(data.len())?;

        if self.current_part_size == 0 {
            self.current_part_size = self.next_part_size()?;
        }

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
            self.total_bytes += len;

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

            self.total_bytes += self.current_bytes;

            // Reset the context for merged chunks.
            self.ctx = self.ctx.reset();
        }

        self.update_part_sizes();

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
        format!(
            "{}-{}",
            self.ctx.digest_to_string(digest),
            self.format_parts()
        )
    }

    /// Get the next part size.
    pub fn next_part_size(&mut self) -> Result<u64> {
        match &self.part_mode {
            PartMode::PartSizes(part_sizes) => {
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
            PartMode::PartNumber(part_number) => {
                let file_size = self.file_size.ok_or_else(|| {
                    ParseError("cannot use part number syntax without file size".to_string())
                })?;
                Ok(Self::part_number_to_size(*part_number, file_size))
            }
        }
    }

    /// Format the part size. The canonical form always a has a bytes ending to distinguish it
    /// from part numbers.
    fn format_part_size<T: Display>(part_size: T) -> String {
        format!("{}b", part_size)
    }

    /// Get the part sizes from the part mode.
    pub fn get_part_sizes(&self) -> Vec<u64> {
        match self.part_mode {
            PartMode::PartNumber(part_number) => {
                if self.file_size.is_none() && self.n_checksums == 0 {
                    panic!("cannot format part number without the file size and without finalizing the checksum");
                }

                // Get the file size if it exists or default to the total bytes.
                let file_size = self.file_size.unwrap_or(self.total_bytes);
                let part_size = Self::part_number_to_size(part_number, file_size);

                vec![part_size]
            }
            PartMode::PartSizes(ref part_sizes) => part_sizes.to_vec(),
        }
    }

    /// Format the parts into a string based on the part mode. This will panic if the file size
    /// was not set and `finalize` was not called.
    pub fn format_parts(&self) -> String {
        self.get_part_sizes()
            .iter()
            .map(Self::format_part_size)
            .collect::<Vec<_>>()
            .join("-")
    }

    /// Convert a part number to a part size using the file size.
    pub fn part_number_to_size(part_number: u64, file_size: u64) -> u64 {
        file_size.div_ceil(part_number)
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

    /// Does this context represent a valid and preferred multipart checksum. All multipart
    /// checksums that AWS can use are preferred except for those with different sized part sizes.
    /// Returns the preferred part size.
    pub fn is_preferred_multipart(&self, provider: &Provider) -> Option<u64> {
        let part_sizes = self.get_part_sizes();
        if part_sizes.len() == 1 && self.ctx.is_preferred_cloud_ctx(provider) {
            Some(part_sizes[0])
        } else {
            None
        }
    }

    /// Get the underlying standard context.
    pub fn ctx(self) -> StandardCtx {
        self.ctx
    }
}

impl FromStr for AWSETagCtx {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let (s, part_mode) = Self::parse_part_size(s)?;
        let ctx = StandardCtx::from_str(&s)?;

        Ok(AWSETagCtx::new(ctx, part_mode, None))
    }
}

impl Display for AWSETagCtx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-aws-{}", self.ctx, self.format_parts())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::checksum::aws_etag::{AWSETagCtx, PartMode};
    use crate::checksum::standard::StandardCtx;
    use crate::checksum::test::test_checksum;
    use anyhow::Result;
    use std::str::FromStr;

    pub(crate) fn expected_md5_1gib() -> &'static str {
        "6c434b38867bbd608ba2f06e92ed4e43-1073741824b"
    }

    pub(crate) fn expected_md5_100mib() -> &'static str {
        "e5727bb1cb678220f6782ff6cb927569-104857600b"
    }

    pub(crate) fn expected_md5_10() -> &'static str {
        "9a9666a5c313c53fbc3a3ea1d43cc981-107374183b"
    }

    pub(crate) fn expected_sha256_100mib() -> &'static str {
        "a9ed6c4b6aadf887f90a3d483b5c5b79bc08075af2a1718e3e15c63b9904ebf7-104857600b"
    }

    #[test]
    fn test_ordering() -> Result<()> {
        assert!(AWSETagCtx::from_str("md5-aws-8mib")? < AWSETagCtx::from_str("md5-aws-5mib")?);
        assert!(AWSETagCtx::from_str("sha256-aws-8mib")? < AWSETagCtx::from_str("md5-aws-5mib")?);

        assert!(AWSETagCtx::from_str("md5-aws-1000b")? < AWSETagCtx::from_str("md5-aws-2000b")?);
        assert!(AWSETagCtx::from_str("sha256-aws-1000b")? < AWSETagCtx::from_str("md5-aws-2000b")?);

        assert!(AWSETagCtx::from_str("md5-aws-1000b")? < AWSETagCtx::from_str("sha256-aws-1000b")?);
        assert!(
            AWSETagCtx::from_str("sha256-aws-1000b")? < AWSETagCtx::from_str("md5-aws-100b-100b")?
        );

        Ok(())
    }

    #[test]
    fn test_update_part_sizes() -> Result<()> {
        assert_update_part_sizes(vec![214748365], 1073741824, vec![214748365]);
        assert_update_part_sizes(
            vec![214748365, 214748365, 214748365, 214748365, 214748364],
            1073741824,
            vec![214748365],
        );
        assert_update_part_sizes(
            vec![214748365, 214748365, 214748365, 214748365, 214748365],
            1073741824,
            vec![214748365],
        );
        assert_update_part_sizes(
            vec![214748365, 214748365, 214748365, 214748365, 214748366],
            1073741824,
            vec![214748365],
        );
        assert_update_part_sizes(
            vec![214748365, 214748365, 214748365, 214748365, 214748367],
            1073741826,
            vec![214748365, 214748365, 214748365, 214748365, 214748366],
        );

        assert_update_part_sizes(
            vec![214748365, 214748365, 429496730, 214748364],
            1073741824,
            vec![214748365, 214748365, 429496730],
        );
        assert_update_part_sizes(
            vec![214748365, 214748365, 429496730, 214748366],
            1073741824,
            vec![214748365, 214748365, 429496730],
        );
        assert_update_part_sizes(
            vec![214748365, 214748365, 429496730, 214748365],
            1073741824,
            vec![214748365, 214748365, 429496730],
        );

        assert_update_part_sizes(
            vec![214748365, 214748365, 429496730],
            644245094,
            vec![214748365],
        );

        assert_update_part_sizes(
            vec![214748365, 214748365, 429496730, 214748364],
            1073741825,
            vec![214748365, 214748365, 429496730, 214748364],
        );

        assert_update_part_sizes(
            vec![214748365, 214748365, 429496730, 214748365, 429496730],
            1073741826,
            vec![214748365, 214748365, 429496730, 214748365],
        );

        assert_update_part_sizes(
            vec![214748365, 214748365, 429496730, 214748365, 600000000],
            1288590200,
            vec![214748365, 214748365, 429496730, 214748365, 214848375],
        );

        Ok(())
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

    fn assert_update_part_sizes(part_sizes: Vec<u64>, file_size: u64, expected: Vec<u64>) {
        let mut ctx = AWSETagCtx::new(
            StandardCtx::md5(),
            PartMode::PartSizes(part_sizes),
            Some(file_size),
        );
        ctx.update_part_sizes();
        assert_eq!(ctx.part_mode, PartMode::PartSizes(expected));
    }
}
