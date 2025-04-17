//! AWS checksums and functionality.
//!

use crate::checksum::aws_etag::{AWSETagCtx, PartMode};
use crate::checksum::file::Checksum;
use crate::checksum::file::SumsFile;
use crate::checksum::standard::StandardCtx;
use crate::checksum::Ctx;
use crate::error::Error::{AwsError, ParseError};
use crate::error::{Error, Result};
use crate::io::sums::ObjectSums;
use crate::io::Provider;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::get_object_attributes::GetObjectAttributesOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ChecksumType, ObjectAttributes, ObjectPart,
};
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::ByteStream;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::io::AsyncRead;

/// Build an S3 sums object.
#[derive(Debug, Default)]
pub struct S3Builder {
    client: Option<Arc<Client>>,
    bucket: Option<String>,
    key: Option<String>,
}

impl S3Builder {
    /// Set the client.
    pub fn with_client(mut self, client: Arc<Client>) -> Self {
        self.client = Some(client);
        self
    }

    /// Set the key.
    pub fn with_key(mut self, key: String) -> Self {
        self.key = Some(key);
        self
    }

    /// Set the bucket.
    pub fn with_bucket(mut self, bucket: String) -> Self {
        self.bucket = Some(bucket);
        self
    }

    fn get_components(self) -> Result<(Arc<Client>, String, String)> {
        let error_fn =
            || ParseError("client, bucket and key are required in `S3Builder`".to_string());

        Ok((
            self.client.ok_or_else(error_fn)?,
            self.bucket.ok_or_else(error_fn)?,
            self.key.ok_or_else(error_fn)?,
        ))
    }

    /// Build using the client, bucket and key.
    pub fn build(self) -> Result<S3> {
        Ok(self.get_components()?.into())
    }
}

impl From<(Arc<Client>, String, String)> for S3 {
    fn from((client, bucket, key): (Arc<Client>, String, String)) -> Self {
        Self::new(client, bucket, key)
    }
}

/// An S3 object and AWS-related existing sums.
#[derive(Debug, Clone)]
pub struct S3 {
    client: Arc<Client>,
    bucket: String,
    key: String,
    get_object_attributes: Option<GetObjectAttributesOutput>,
    head_object: HashMap<u64, HeadObjectOutput>,
}

impl S3 {
    /// Create a new S3 object.
    pub fn new(client: Arc<Client>, bucket: String, key: String) -> S3 {
        Self {
            client,
            bucket,
            key,
            get_object_attributes: None,
            head_object: HashMap::new(),
        }
    }

    /// Get an existing sums file if it exists.
    pub async fn get_existing_sums(&self) -> Result<Option<SumsFile>> {
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(SumsFile::format_sums_file(&self.key))
            .send()
            .await
        {
            Ok(sums) => {
                let data = sums.body.collect().await?.to_vec();
                let sums = SumsFile::read_from_slice(data.as_slice()).await?;
                Ok(Some(sums))
            }
            Err(err) if matches!(err.as_service_error(), Some(GetObjectError::NoSuchKey(_))) => {
                Ok(None)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Get the `GetObjectAttributes` output for the target file. This caches the result in
    /// memory so that subsequent calls do not repeat the query.
    pub async fn get_object_attributes(&mut self) -> Result<&GetObjectAttributesOutput> {
        if let Some(ref attributes) = self.get_object_attributes {
            return Ok(attributes);
        }

        let attributes = self
            .client
            .get_object_attributes()
            .bucket(&self.bucket)
            .key(SumsFile::format_target_file(&self.key))
            .object_attributes(ObjectAttributes::Etag)
            .object_attributes(ObjectAttributes::Checksum)
            .object_attributes(ObjectAttributes::ObjectSize)
            .object_attributes(ObjectAttributes::ObjectParts)
            .send()
            .await?;

        Ok(self.get_object_attributes.insert(attributes))
    }

    /// Get the `HeadObjectOutput` output for the target file for a specific part. This caches
    /// the result in memory so that subsequent calls do not repeat the query for the same part.
    pub async fn head_object(&mut self, part_number: u64) -> Result<&HeadObjectOutput> {
        if self.head_object.contains_key(&part_number) {
            return Ok(&self.head_object[&part_number]);
        }

        let head_object = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(SumsFile::format_target_file(&self.key))
            .part_number(i32::try_from(part_number)?)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await?;

        Ok(self.head_object.entry(part_number).or_insert(head_object))
    }

    /// Is this an additional checksum, i.e. not an `ETag`.
    fn is_additional_checksum(ctx: &StandardCtx) -> bool {
        !matches!(ctx, StandardCtx::MD5(_))
    }

    /// Decode the base64 encoded checksum if it is an additional checksum. All additional
    /// checksums (not including the `ETag`) are base64 encoded when returned from the SDK.
    /// The `ETag` is hex encoded.
    fn decode_sum(ctx: &StandardCtx, sum: String) -> Result<Vec<u8>> {
        let sum = sum.split("-").next().unwrap_or_else(|| &sum);

        if Self::is_additional_checksum(ctx) {
            let data = BASE64_STANDARD
                .decode(sum.as_bytes())
                .map_err(|_| ParseError(format!("failed to decode base64 checksum: {}", sum)))?;

            Ok(data)
        } else {
            Ok(hex::decode(sum.as_bytes())
                .map_err(|_| ParseError(format!("failed to decode hex `ETag`: {}", sum)))?)
        }
    }

    /// Get the AWS checksum value from `GetObjectAttributes`.
    pub async fn aws_sums_from_ctx(&mut self, ctx: &StandardCtx) -> Result<Option<String>> {
        let attributes = self.get_object_attributes().await?;
        let sum = match ctx {
            // There are no part checksums for e_tags.
            StandardCtx::MD5(_) => attributes.e_tag(),
            // Every other checksum has part checksums available if uploaded using multipart uploads.
            StandardCtx::SHA1(_) => attributes.checksum().and_then(|c| c.checksum_sha1()),
            StandardCtx::SHA256(_) => attributes.checksum().and_then(|c| c.checksum_sha256()),
            StandardCtx::CRC32(_, _) => attributes.checksum().and_then(|c| c.checksum_crc32()),
            StandardCtx::CRC32C(_, _) => attributes.checksum().and_then(|c| c.checksum_crc32_c()),
            StandardCtx::CRC64NVME(_, _) => {
                attributes.checksum().and_then(|c| c.checksum_crc64_nvme())
            }
            _ => None,
        };

        Ok(sum.map(|sum| sum.to_string()))
    }

    /// Get the AWS checksum part from `ObjectPart`.
    pub fn aws_parts_from_ctx(ctx: &StandardCtx, part: &ObjectPart) -> Option<String> {
        let sum = match ctx {
            // Every checksum other than `ETag` has part checksums available if uploaded using multipart uploads.
            StandardCtx::SHA1(_) => part.checksum_sha1(),
            StandardCtx::SHA256(_) => part.checksum_sha256(),
            StandardCtx::CRC32(_, _) => part.checksum_crc32(),
            StandardCtx::CRC32C(_, _) => part.checksum_crc32_c(),
            StandardCtx::CRC64NVME(_, _) => part.checksum_crc64_nvme(),
            // There are no part checksums for `ETag`s.
            _ => None,
        };

        sum.map(|sum| sum.to_string())
    }

    /// Get the AWS checksum parts from `GetObjectAttributes` parts output.
    pub async fn aws_parts_from_attributes(&mut self) -> Result<Option<Vec<Option<u64>>>> {
        let parts = self
            .get_object_attributes()
            .await?
            .object_parts()
            .map(|parts| {
                let parts = parts
                    .parts()
                    .iter()
                    .map(|part| Ok(part.size().map(u64::try_from).transpose()?))
                    .collect::<Result<Vec<_>>>()?;

                if parts.is_empty() {
                    Ok::<_, Error>(None)
                } else {
                    Ok(Some(parts))
                }
            })
            .transpose()?
            .flatten();

        Ok(parts)
    }

    /// Get the AWS checksum parts from `HeadObjectOutput` using part numbers and the `ContentLength`.
    /// This only fills out the checksum part length, and not the value as that remains unknown. This
    /// is useful to determine how to re-calculate an `ETag`. If the exact part sizes are not known
    /// then it's not possible to know for sure that the `ETag` was calculated with equal part sizes
    /// as they are allowed to be different.
    pub async fn aws_parts_from_head(
        &mut self,
        total_parts: u64,
    ) -> Result<Option<Vec<Option<u64>>>> {
        let mut part_sums = vec![];

        for part_number in 1..=total_parts {
            let head_object = self.head_object(part_number).await?;

            // The content length represents the part size. Return early if any of the content
            // lengths are not present, to avoid having empty part checksums.
            let Some(part_size) = head_object
                .content_length()
                .map(TryInto::try_into)
                .transpose()?
            else {
                return Ok(None);
            };

            part_sums.push(Some(part_size));
        }

        Ok(Some(part_sums))
    }

    /// Add checksums to an existing sums file using AWS metadata.
    async fn add_checksum(&mut self, sums_file: &mut SumsFile, ctx: StandardCtx) -> Result<()> {
        // If there is no sum for this context, return early.
        let Some(sum) = self.aws_sums_from_ctx(&ctx).await? else {
            return Ok(());
        };

        // Get the file size, total part count and checksum type from the attributes. This is in
        // a separate block to avoid mutably borrowing more than once later.
        let (file_size, total_parts, checksum_type) = {
            let attributes = self.get_object_attributes().await?;

            let file_size = attributes.object_size().map(u64::try_from).transpose()?;
            let (total_parts, checksum_type) = Self::parse_parts_and_type(sum.as_str())?;

            (file_size, total_parts, checksum_type)
        };

        // Determine the parts if they exist.
        let parts = self.aws_parts_from_attributes().await?;
        // If there are no parts, try and find them using the total part count and head object.
        // This should only trigger on `ETag`s.
        let parts = match (parts, total_parts) {
            (Some(parts), _) => Some(parts),
            (None, Some(total_parts)) => self.aws_parts_from_head(total_parts).await?,
            _ => None,
        };

        let sum = Self::decode_sum(&ctx, sum)?;

        // Create the AWS context with the available information. This can be a composite checksum
        // with a part size, or a regular context otherwise.
        let ctx = match (total_parts, checksum_type) {
            (Some(total_parts), ChecksumType::Composite) => {
                // Get the part mode from the individual part sizes. This will be used to format
                // the output.
                let part_mode = if let Some(ref parts) = parts {
                    let parts = parts.iter().filter_map(|part| *part).collect::<Vec<u64>>();
                    PartMode::PartSizes(parts)
                } else {
                    PartMode::PartNumber(total_parts)
                };

                let mut ctx = AWSETagCtx::new(ctx, part_mode, file_size);
                ctx.update_part_sizes();

                Ctx::AWSEtag(ctx)
            }
            _ => Ctx::Regular(ctx),
        };

        let checksum = Checksum::new(ctx.digest_to_string(&sum));
        sums_file.add_checksum(ctx, checksum);

        Ok(())
    }

    /// Load a sums file from existing metadata from S3. There's a few sources of information from
    /// AWS for checksums in order of significance:
    ///
    /// 1. `GetObjectAttributes` contains `Checksum`s, `ETag`s and parts:
    ///     - For `ETag`s, there are no parts, however there is a `TotalPartsCount`
    ///     - For the other checksums, parts are included in the response.
    ///     - If other checksums are present, then the `ETag` will have the same part sizes.
    /// 2. `HeadObject` contains the above information, but no part checksums:
    ///     - For `ETag`s, the `ContentLength` header determines the part size of a part if
    ///       queries with `partNumber`. `HeadObject` cannot retrieve the actual part checksums
    ///       for `ETag`s:
    ///       https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax
    ///     - For the other checksums, there is no useful information that isn't already in
    ///       `GetObjectAttributes`.
    ///
    /// This is used to add as much information to the output sums file as possible.
    pub async fn sums_from_metadata(&mut self) -> Result<SumsFile> {
        // The target file metadata.
        let attributes = self.get_object_attributes().await?;
        let file_size = attributes.object_size().map(u64::try_from).transpose()?;
        let mut sums_file = SumsFile::default().with_size(file_size);

        // Add the individual checksums for each type.
        self.add_checksum(&mut sums_file, StandardCtx::md5())
            .await?;
        self.add_checksum(&mut sums_file, StandardCtx::crc32())
            .await?;
        self.add_checksum(&mut sums_file, StandardCtx::crc32c())
            .await?;
        self.add_checksum(&mut sums_file, StandardCtx::sha1())
            .await?;
        self.add_checksum(&mut sums_file, StandardCtx::sha256())
            .await?;
        self.add_checksum(&mut sums_file, StandardCtx::crc64nvme())
            .await?;

        if sums_file.checksums.is_empty() {
            return Err(AwsError(
                "failed to create sums file from metadata".to_string(),
            ));
        }

        Ok(sums_file)
    }

    /// Parse the number of parts and the checksum type from a string.
    pub fn parse_parts_and_type(s: &str) -> Result<(Option<u64>, ChecksumType)> {
        let split = s.rsplit_once("-");
        if let Some((_, parts)) = split {
            let parts = u64::from_str(parts).map_err(|err| {
                ParseError(format!("failed to parse parts from checksum: {}", err))
            })?;
            Ok((Some(parts), ChecksumType::Composite))
        } else {
            Ok((None, ChecksumType::FullObject))
        }
    }

    /// Get the inner values not including the S3 client.
    pub fn into_inner(self) -> (String, String) {
        (self.bucket, self.key)
    }

    /// Get the object and convert it into an `AsyncRead`.
    pub async fn object_reader(&self) -> Result<impl AsyncRead> {
        Ok(Box::new(
            self.client
                .get_object()
                .bucket(&self.bucket)
                .key(SumsFile::format_target_file(&self.key))
                .send()
                .await?
                .body
                .into_async_read(),
        ))
    }

    /// Get the object file size.
    async fn size(&mut self) -> Result<Option<u64>> {
        Ok(self
            .get_object_attributes()
            .await?
            .object_size
            .map(|size| size.try_into())
            .transpose()?)
    }

    /// Write the sums file to the configured location using `PutObject`.
    pub async fn put_sums(&self, sums_file: &SumsFile) -> Result<()> {
        let key = SumsFile::format_sums_file(&self.key);
        self.client
            .put_object()
            .checksum_algorithm(ChecksumAlgorithm::Crc64Nvme)
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(sums_file.to_json_string()?.into_bytes()))
            .send()
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectSums for S3 {
    async fn sums_file(&mut self) -> Result<Option<SumsFile>> {
        let metadata_sums = self.sums_from_metadata().await?;

        match self.get_existing_sums().await? {
            None => Ok(Some(metadata_sums)),
            Some(existing) => Ok(Some(metadata_sums.merge(existing)?)),
        }
    }

    async fn reader(&mut self) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
        Ok(Box::new(self.object_reader().await?))
    }

    async fn file_size(&mut self) -> Result<Option<u64>> {
        self.size().await
    }

    async fn write_sums_file(&self, sums_file: &SumsFile) -> Result<()> {
        self.put_sums(sums_file).await
    }

    fn location(&self) -> String {
        Provider::format_s3(&self.bucket, &self.key)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::checksum::standard::test::EXPECTED_MD5_SUM;
    use crate::task::generate::test::generate_for;
    use crate::test::{TEST_FILE_NAME, TEST_FILE_SIZE};
    use aws_sdk_s3::types;
    use aws_sdk_s3::types::GetObjectAttributesParts;
    use aws_smithy_mocks_experimental::{mock, mock_client, Rule, RuleMode};

    const EXPECTED_SHA256_SUM: &str = "Kf+9U8vkMXmrL6YtvZWMDsMLNAq1DOfHheinpLR3Hjk="; // pragma: allowlist secret

    const EXPECTED_MD5_SUM_5: &str = "0798905b42c575d43e921be42e126a26-5";
    const EXPECTED_MD5_SUM_4: &str = "75652bd9b9c3652b9f43e7663b3f14b6-4";

    const EXPECTED_SHA256_SUM_5: &str = "i+AmvKnN0bTeoChGodtn0v+gJ5Srd1u43mrWaouheo4=-5"; // pragma: allowlist secret
    const EXPECTED_SHA256_SUM_4: &str = "Wb7wV/0P9hRl2hTZ7Ee8eD7SlDUBwxJywUDIPV0W8Gw=-4"; // pragma: allowlist secret

    const EXPECTED_SHA256_PART_1: &str = "qGw2Bcs0UvgbO0gUoljNQFAWen5xWqwi2RNIEvHfDRc="; // pragma: allowlist secret
    const EXPECTED_SHA256_PART_2: &str = "XLJehuPqO2ZOF80bcsOwMfRUp1Sy8Pue4FNQB+BaDpU="; // pragma: allowlist secret
    const EXPECTED_SHA256_PART_3: &str = "BQn5YX5CBUx6XYhY9T7RnVTIsR8o/lKnSKgRRUs6B7U="; // pragma: allowlist secret
    const EXPECTED_SHA256_PART_4: &str = "Wt2RpJkRAlmYPk0/BfBS5XMvlvhtSRRsU4MhbJTm/RQ="; // pragma: allowlist secret
    const EXPECTED_SHA256_PART_5: &str = "laScT3WEixthSDryDZwNEA+U5URMQ1Q8EXOO48F4v78="; // pragma: allowlist secret

    const EXPECTED_SHA256_PART_3_4_CONCAT: &str = "pWWT3JcI0KGHFujswlkNCTl1JfsSRpbmHyMcYIbjBQA="; // pragma: allowlist secret

    #[tokio::test]
    pub async fn test_multi_part_with_sha256_different_part_sizes() -> anyhow::Result<()> {
        let mut s3 = S3Builder::default()
            .with_client(Arc::new(mock_multi_part_with_sha256_different_part_sizes()))
            .with_bucket("bucket".to_string())
            .with_key("key".to_string())
            .build()?;

        let sums = s3.sums_from_metadata().await?.split();
        let expected = generate_for(
            "key",
            vec![
                "md5-aws-214748365b-214748365b-429496730b",
                "sha256-aws-214748365b-214748365b-429496730b",
            ],
            true,
            false,
        )
        .await?
        .split();

        assert_all_same(sums, expected);

        Ok(())
    }

    #[tokio::test]
    pub async fn test_multi_part_etag_only_different_part_sizes() -> anyhow::Result<()> {
        let mut s3 = S3Builder::default()
            .with_client(Arc::new(mock_multi_part_etag_only_different_part_sizes()))
            .with_bucket("bucket".to_string())
            .with_key("key".to_string())
            .build()?;

        let sums = s3.sums_from_metadata().await?.split();
        let expected = generate_for(
            "key",
            vec!["md5-aws-214748365b-214748365b-429496730b"],
            true,
            false,
        )
        .await?
        .split();

        assert_all_same(sums, expected);

        Ok(())
    }

    #[tokio::test]
    pub async fn test_multi_part_with_sha256() -> anyhow::Result<()> {
        let mut s3 = S3Builder::default()
            .with_client(Arc::new(mock_multi_part_with_sha256()))
            .with_bucket("bucket".to_string())
            .with_key("key".to_string())
            .build()?;

        let sums = s3.sums_from_metadata().await?.split();
        let expected = generate_for("key", vec!["md5-aws-5", "sha256-aws-5"], true, false)
            .await?
            .split();

        assert_all_same(sums, expected);

        Ok(())
    }

    fn assert_all_same(result: Vec<SumsFile>, expected: Vec<SumsFile>) {
        println!("{}", serde_json::to_string_pretty(&result).unwrap());
        println!("{}", serde_json::to_string_pretty(&expected).unwrap());

        assert!(result.into_iter().zip(expected).all(|(a, b)| a.is_same(&b)));
    }

    #[tokio::test]
    pub async fn test_multi_part_etag_only() -> anyhow::Result<()> {
        let mut s3 = S3Builder::default()
            .with_client(Arc::new(mock_multi_part_etag_only()))
            .with_bucket("bucket".to_string())
            .with_key("key".to_string())
            .build()?;

        let sums = s3.sums_from_metadata().await?.split();
        let expected = generate_for(TEST_FILE_NAME, vec!["md5-aws-5"], true, false)
            .await?
            .split();

        assert_all_same(sums, expected);

        Ok(())
    }

    #[tokio::test]
    pub async fn test_single_part_with_sha256() -> anyhow::Result<()> {
        let mut s3 = S3Builder::default()
            .with_client(Arc::new(mock_single_part_with_sha256()))
            .with_bucket("bucket".to_string())
            .with_key("key".to_string())
            .build()?;

        let sums = s3.sums_from_metadata().await?.split();
        let expected = generate_for(TEST_FILE_NAME, vec!["md5", "sha256"], true, false)
            .await?
            .split();

        assert_all_same(sums, expected);

        Ok(())
    }

    #[tokio::test]
    pub async fn test_single_part_etag_only() -> anyhow::Result<()> {
        let mut s3 = S3Builder::default()
            .with_client(Arc::new(mock_single_part_etag_only()))
            .with_bucket("bucket".to_string())
            .with_key("key".to_string())
            .build()?;

        let sums = s3.sums_from_metadata().await?.split();
        let expected = generate_for(TEST_FILE_NAME, vec!["md5"], true, false)
            .await?
            .split();

        assert_all_same(sums, expected);

        Ok(())
    }

    fn head_object_rule(content_length: i64) -> Rule {
        mock!(Client::head_object)
            .match_requests(|req| req.bucket() == Some("bucket") && req.key() == Some("key"))
            .then_output(move || {
                HeadObjectOutput::builder()
                    .content_length(content_length)
                    .build()
            })
    }

    fn mock_multi_part_with_sha256_different_part_sizes() -> Client {
        let get_object_attributes = mock!(Client::get_object_attributes)
            .match_requests(|req| req.bucket() == Some("bucket") && req.key() == Some("key"))
            .then_output(|| {
                GetObjectAttributesOutput::builder()
                    .e_tag(EXPECTED_MD5_SUM_4)
                    .checksum(
                        types::Checksum::builder()
                            .checksum_sha256(EXPECTED_SHA256_SUM_4)
                            .checksum_type(ChecksumType::Composite)
                            .build(),
                    )
                    .object_parts(
                        GetObjectAttributesParts::builder()
                            .total_parts_count(4)
                            .parts(
                                ObjectPart::builder()
                                    .part_number(1)
                                    .size(214748365)
                                    .checksum_sha256(EXPECTED_SHA256_PART_1.to_string())
                                    .build(),
                            )
                            .parts(
                                ObjectPart::builder()
                                    .part_number(2)
                                    .size(214748365)
                                    .checksum_sha256(EXPECTED_SHA256_PART_2.to_string())
                                    .build(),
                            )
                            .parts(
                                ObjectPart::builder()
                                    .part_number(3)
                                    .size(429496730)
                                    .checksum_sha256(EXPECTED_SHA256_PART_3_4_CONCAT.to_string())
                                    .build(),
                            )
                            .parts(
                                ObjectPart::builder()
                                    .part_number(4)
                                    .size(214748364)
                                    .checksum_sha256(EXPECTED_SHA256_PART_5.to_string())
                                    .build(),
                            )
                            .build(),
                    )
                    .object_size(TEST_FILE_SIZE as i64)
                    .build()
            });

        // If an additional checksum is present, then there is no need to call head object as the
        // parts are always in the get object attributes response.
        mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&get_object_attributes,])
    }

    fn mock_multi_part_with_sha256() -> Client {
        let get_object_attributes = mock!(Client::get_object_attributes)
            .match_requests(|req| req.bucket() == Some("bucket") && req.key() == Some("key"))
            .then_output(|| {
                GetObjectAttributesOutput::builder()
                    .e_tag(EXPECTED_MD5_SUM_5)
                    .checksum(
                        types::Checksum::builder()
                            .checksum_sha256(EXPECTED_SHA256_SUM_5)
                            .checksum_type(ChecksumType::Composite)
                            .build(),
                    )
                    .object_parts(
                        GetObjectAttributesParts::builder()
                            .total_parts_count(5)
                            .parts(
                                ObjectPart::builder()
                                    .part_number(1)
                                    .size(214748365)
                                    .checksum_sha256(EXPECTED_SHA256_PART_1.to_string())
                                    .build(),
                            )
                            .parts(
                                ObjectPart::builder()
                                    .part_number(2)
                                    .size(214748365)
                                    .checksum_sha256(EXPECTED_SHA256_PART_2.to_string())
                                    .build(),
                            )
                            .parts(
                                ObjectPart::builder()
                                    .part_number(3)
                                    .size(214748365)
                                    .checksum_sha256(EXPECTED_SHA256_PART_3.to_string())
                                    .build(),
                            )
                            .parts(
                                ObjectPart::builder()
                                    .part_number(4)
                                    .size(214748365)
                                    .checksum_sha256(EXPECTED_SHA256_PART_4.to_string())
                                    .build(),
                            )
                            .parts(
                                ObjectPart::builder()
                                    .part_number(5)
                                    .size(214748364)
                                    .checksum_sha256(EXPECTED_SHA256_PART_5.to_string())
                                    .build(),
                            )
                            .build(),
                    )
                    .object_size(TEST_FILE_SIZE as i64)
                    .build()
            });

        // If an additional checksum is present, then there is no need to call head object as the
        // parts are always in the get object attributes response.
        mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&get_object_attributes,])
    }

    fn mock_multi_part_etag_only_different_part_sizes() -> Client {
        let get_object_attributes = mock!(Client::get_object_attributes)
            .match_requests(|req| req.bucket() == Some("bucket") && req.key() == Some("key"))
            .then_output(|| {
                GetObjectAttributesOutput::builder()
                    .e_tag(EXPECTED_MD5_SUM_4)
                    .object_parts(
                        GetObjectAttributesParts::builder()
                            .total_parts_count(4)
                            .build(),
                    )
                    .object_size(TEST_FILE_SIZE as i64)
                    .build()
            });

        mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            &[
                &get_object_attributes,
                &head_object_rule(214748365),
                &head_object_rule(214748365),
                &head_object_rule(429496730),
                &head_object_rule(214748364),
            ]
        )
    }

    fn mock_multi_part_etag_only() -> Client {
        let get_object_attributes = mock_multi_part_etag_only_rule();

        mock_client!(
            aws_sdk_s3,
            RuleMode::Sequential,
            get_object_attributes.as_slice()
        )
    }

    pub(crate) fn mock_multi_part_etag_only_rule() -> Vec<Rule> {
        let get_object_attributes = mock!(Client::get_object_attributes)
            .match_requests(|req| req.bucket() == Some("bucket") && req.key() == Some("key"))
            .then_output(|| {
                GetObjectAttributesOutput::builder()
                    .e_tag(EXPECTED_MD5_SUM_5)
                    .object_parts(
                        GetObjectAttributesParts::builder()
                            .total_parts_count(5)
                            .build(),
                    )
                    .object_size(TEST_FILE_SIZE as i64)
                    .build()
            });

        vec![
            get_object_attributes,
            head_object_rule(214748365),
            head_object_rule(214748365),
            head_object_rule(214748365),
            head_object_rule(214748365),
            head_object_rule(214748364),
        ]
    }

    fn mock_single_part_with_sha256() -> Client {
        let get_object_attributes = mock!(Client::get_object_attributes)
            .match_requests(|req| req.bucket() == Some("bucket") && req.key() == Some("key"))
            .then_output(|| {
                GetObjectAttributesOutput::builder()
                    .e_tag(EXPECTED_MD5_SUM)
                    .checksum(
                        types::Checksum::builder()
                            .checksum_sha256(EXPECTED_SHA256_SUM)
                            .build(),
                    )
                    .object_size(TEST_FILE_SIZE as i64)
                    .build()
            });

        mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&get_object_attributes])
    }

    fn mock_single_part_etag_only() -> Client {
        let get_object_attributes = mock_single_part_etag_only_rule();

        mock_client!(aws_sdk_s3, RuleMode::Sequential, &[&get_object_attributes])
    }

    pub(crate) fn mock_single_part_etag_only_rule() -> Rule {
        mock!(Client::get_object_attributes)
            .match_requests(move |req| req.bucket() == Some("bucket") && req.key() == Some("key"))
            .then_output(|| {
                GetObjectAttributesOutput::builder()
                    .e_tag(EXPECTED_MD5_SUM)
                    .object_size(TEST_FILE_SIZE as i64)
                    .build()
            })
    }
}
