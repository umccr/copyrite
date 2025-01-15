//! Generate checksums for files.
//!

use crate::checksum::file::{Checksum, SumsFile};
use crate::checksum::Ctx;
use crate::error::Error::GenerateError;
use crate::error::{Error, Result};
use crate::reader::SharedReader;
use crate::task::generate::Task::{ChecksumTask, ReadTask};
use futures_util::future::join_all;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use tokio::fs::File;
use tokio::task::JoinHandle;

/// Define the kind of task that is running.
#[derive(Debug)]
pub enum Task {
    ReadTask(u64),
    ChecksumTask(Box<(Ctx, Vec<u8>)>),
}

/// Build a generate task.
#[derive(Debug, Default)]
pub struct GenerateTaskBuilder {
    input_file_name: String,
    overwrite: bool,
    verify: bool,
}

impl GenerateTaskBuilder {
    /// Set the input file name.
    pub fn with_input_file_name(mut self, input_file_name: String) -> Self {
        self.input_file_name = input_file_name;
        self
    }

    /// Set whether to overwrite existing files.
    pub fn with_overwrite(mut self, overwrite: bool) -> Self {
        self.overwrite = overwrite;
        self
    }

    /// Set whether to overwrite existing files.
    pub fn with_verify(mut self, verify: bool) -> Self {
        self.verify = verify;
        self
    }

    /// Build a generate task.
    pub async fn build(self) -> Result<GenerateTask> {
        let existing_output = if !self.input_file_name.is_empty() {
            SumsFile::read_from(self.input_file_name.to_string())
                .await
                .ok()
        } else {
            None
        };

        if self.overwrite && self.verify {
            return Err(GenerateError(
                "cannot verify and overwrite checksums".to_string(),
            ));
        }

        let mode = if self.overwrite {
            OverwriteMode::Overwrite
        } else if self.verify {
            OverwriteMode::Verify
        } else {
            OverwriteMode::None
        };

        Ok(GenerateTask {
            input_file_name: self.input_file_name,
            overwrite: mode,
            existing_output,
            ..Default::default()
        })
    }
}

#[derive(Debug, Default)]
enum OverwriteMode {
    #[default]
    None,
    Verify,
    Overwrite,
}

/// Execute the generate checksums tasks.
#[derive(Debug, Default)]
pub struct GenerateTask {
    tasks: Vec<JoinHandle<Result<Task>>>,
    input_file_name: String,
    overwrite: OverwriteMode,
    existing_output: Option<SumsFile>,
}

impl GenerateTask {
    /// Spawns a task which reads from the buffered reader.
    pub fn add_reader_task(mut self, mut reader: impl SharedReader + 'static) -> Result<Self> {
        self.tasks.push(tokio::spawn(async move {
            Ok(ReadTask(reader.read_task().await?))
        }));
        Ok(self)
    }

    /// Spawns a task which generates checksums.
    pub fn add_generate_task(mut self, mut ctx: Ctx, reader: &mut impl SharedReader) -> Self {
        let stream = reader.as_stream();
        self.tasks.push(tokio::spawn(async move {
            let stream = ctx.generate(stream);

            let digest = stream.await?;

            Ok(ChecksumTask(Box::new((ctx, digest))))
        }));

        self
    }

    fn add_generate_tasks_direct(
        mut self,
        checksums: HashSet<Ctx>,
        reader: &mut impl SharedReader,
    ) -> Self {
        for checksum in checksums {
            self = self.add_generate_task(checksum, reader);
        }
        self
    }

    /// Spawns tasks for a series of checksums.
    pub fn add_generate_tasks(
        mut self,
        mut checksums: HashSet<Ctx>,
        reader: &mut impl SharedReader,
    ) -> Result<Self> {
        let existing = self.existing_output.as_ref();

        match self.overwrite {
            OverwriteMode::Verify => {
                existing
                    .map(|file| {
                        for name in file.checksums.keys() {
                            checksums.insert(name.clone());
                        }
                        Ok::<_, Error>(())
                    })
                    .transpose()?;

                self = self.add_generate_tasks_direct(checksums, reader);
            }
            OverwriteMode::Overwrite | OverwriteMode::None => {
                self = self.add_generate_tasks_direct(checksums, reader);
            }
        }

        Ok(self)
    }

    /// Runs the generate task, returning an output file.
    pub async fn run(self) -> Result<SumsFile> {
        let mut file_size = 0;
        let checksums = join_all(self.tasks)
            .await
            .into_iter()
            .map(|val| {
                let task = val??;
                match task {
                    ReadTask(size) => {
                        file_size = size;
                        Ok(None)
                    }
                    ChecksumTask(ctx) => {
                        let (ctx, digest) = *ctx;

                        let part_size = ctx.part_size();
                        let part_checksums = ctx.part_checksums();
                        let checksum = ctx.digest_to_string(&digest);
                        Ok(Some((
                            ctx,
                            Checksum::new(checksum, part_size, part_checksums),
                        )))
                    }
                }
            })
            .collect::<Result<Vec<Option<(Ctx, Checksum)>>>>()?
            .into_iter()
            .flatten();

        let checksums = BTreeMap::from_iter(checksums);
        let new_file = SumsFile::new(
            BTreeSet::from_iter(vec![self.input_file_name]),
            Some(file_size),
            checksums,
        );

        let output = match self.existing_output {
            Some(file) if !matches!(self.overwrite, OverwriteMode::Overwrite) => {
                file.merge(new_file)?
            }
            _ => new_file,
        };

        Ok(output)
    }
}

/// Holds a file name and checksum context.
#[derive(Debug, PartialEq, Eq)]
pub struct SumCtxPair {
    file: String,
    ctx: Ctx,
}

impl SumCtxPair {
    /// Create a new additional context checksum.
    pub fn new(file: String, ctx: Ctx) -> Self {
        SumCtxPair { file, ctx }
    }

    /// Get the inner values.
    pub fn into_inner(self) -> (String, Ctx) {
        (self.file, self.ctx)
    }
}

/// A list of context pairs.
#[derive(Debug, PartialEq, Eq)]
pub struct SumCtxPairs(Vec<SumCtxPair>);

impl SumCtxPairs {
    /// Create the additional checksums.
    pub fn new(ctxs: Vec<SumCtxPair>) -> Self {
        SumCtxPairs(ctxs)
    }

    /// Get the inner value.
    pub fn into_inner(self) -> Vec<SumCtxPair> {
        self.0
    }

    /// Get the additional checksums required from a group of comparables sums files.
    pub fn from_comparable(files: Vec<SumsFile>) -> Result<Option<Self>> {
        // Get the checksum which contains the most amount of occurrences across groups of sums files.
        let file_ctx = files
            .iter()
            .flat_map(|file| file.checksums.keys().cloned())
            .fold(BTreeMap::new(), |mut map, val| {
                // Count occurrences
                map.entry(val).and_modify(|count| *count += 1).or_insert(1);
                map
            })
            .into_iter()
            .max_by(|(_, a), (_, b)| a.cmp(b))
            .map(|(k, _)| k);

        if let Some(mut file_ctx) = file_ctx {
            // Use the checksum for one of the elements in the group.
            let ctxs = files
                .into_iter()
                .flat_map(|file| {
                    // If the sums group already contains this checksum, skip.
                    if file.checksums.contains_key(&file_ctx) {
                        return None;
                    }
                    file_ctx.set_file_size(file.size);

                    file.names
                        .first()
                        .map(|name| SumCtxPair::new(name.to_string(), file_ctx.clone()))
                })
                .collect();

            Ok(Some(SumCtxPairs::new(ctxs)))
        } else {
            Ok(None)
        }
    }
}

impl From<Vec<SumCtxPair>> for SumCtxPairs {
    fn from(ctxs: Vec<SumCtxPair>) -> Self {
        SumCtxPairs(ctxs)
    }
}

/// Get the file size if available.
pub async fn file_size<P: AsRef<Path>>(path: P) -> Option<u64> {
    match File::open(path).await {
        Ok(file) => file.metadata().await.map(|metadata| metadata.len()).ok(),
        Err(_) => None,
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::checksum::aws_etag::test::expected_md5_1gib;
    use crate::checksum::standard::test::{
        expected_crc32_be, expected_crc32c_be, expected_md5_sum, expected_sha1_sum,
        expected_sha256_sum,
    };
    use crate::checksum::standard::StandardCtx;
    use crate::reader::channel::test::channel_reader;
    use crate::task::check::test::write_test_files_not_comparable;
    use crate::task::check::{CheckTaskBuilder, GroupBy};
    use crate::test::{TestFileBuilder, TEST_FILE_SIZE};
    use crate::Endianness;
    use anyhow::Result;
    use std::collections::BTreeSet;
    use tempfile::{tempdir, TempDir};
    use tokio::fs::File;

    #[tokio::test]
    async fn test_sum_ctx_pairs() -> Result<()> {
        let tmp = tempdir()?;
        let files = write_test_files_not_comparable(tmp).await?;

        let check = CheckTaskBuilder::default()
            .with_input_files(files.clone())
            .with_group_by(GroupBy::Comparability)
            .build()
            .await?;
        let check = check.run().await?;

        let result = SumCtxPairs::from_comparable(check)?.unwrap();

        assert_eq!(
            result,
            vec![SumCtxPair::new(
                files.first().unwrap().to_string(),
                Ctx::Regular(StandardCtx::CRC32C(0, Endianness::BigEndian))
            )]
            .into()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_generate_overwrite() -> Result<()> {
        let tmp = tempdir()?;
        let name = write_test_files(tmp).await?;

        test_generate(
            name,
            true,
            false,
            vec!["sha1", "sha256", "md5", "aws-etag-1gib", "crc32", "crc32c"],
            expected_md5_sum(),
        )
        .await
    }

    #[tokio::test]
    async fn test_generate_verify() -> Result<()> {
        let tmp = tempdir()?;
        let name = write_test_files(tmp).await?;

        test_generate(
            name,
            false,
            true,
            vec!["sha1", "sha256", "aws-etag-1gib", "crc32", "crc32c"],
            expected_md5_sum(),
        )
        .await
    }

    #[tokio::test]
    async fn test_generate_no_verify() -> Result<()> {
        let tmp = tempdir()?;
        let name = write_test_files(tmp).await?;

        test_generate(
            name,
            false,
            false,
            vec!["sha1", "sha256", "aws-etag-1gib", "crc32", "crc32c"],
            "123",
        )
        .await
    }

    async fn test_generate(
        name: String,
        overwrite: bool,
        verify: bool,
        tasks: Vec<&str>,
        md5: &str,
    ) -> Result<()> {
        let test_file = TestFileBuilder::default().generate_test_defaults()?;
        let file = File::open(test_file).await?;
        let mut reader = channel_reader(file).await;

        let tasks = tasks
            .into_iter()
            .map(|task| Ok(task.parse()?))
            .collect::<Result<Vec<_>>>()?;
        let file = GenerateTaskBuilder::default()
            .with_input_file_name(name.to_string())
            .with_overwrite(overwrite)
            .with_verify(verify)
            .build()
            .await?
            .add_generate_tasks(HashSet::from_iter(tasks), &mut reader)?
            .add_reader_task(reader)?
            .run()
            .await?;

        assert_eq!(file.names, BTreeSet::from_iter(vec![name]));
        assert_eq!(file.size, Some(TEST_FILE_SIZE));
        assert_eq!(
            file.checksums[&"md5".parse()?],
            Checksum::new(md5.to_string(), None, None)
        );
        assert_eq!(
            file.checksums[&"sha1".parse()?],
            Checksum::new(expected_sha1_sum().to_string(), None, None)
        );
        assert_eq!(
            file.checksums[&"sha256".parse()?],
            Checksum::new(expected_sha256_sum().to_string(), None, None)
        );
        assert_eq!(
            file.checksums[&"md5-aws-1073741824b".parse()?],
            Checksum::new(
                expected_md5_1gib().to_string(),
                Some(1073741824),
                Some(vec!["d93e71879054f205ede90d35c8081ca5".to_string()])
            )
        );
        assert_eq!(
            file.checksums[&"crc32".parse()?],
            Checksum::new(expected_crc32_be().to_string(), None, None)
        );
        assert_eq!(
            file.checksums[&"crc32c".parse()?],
            Checksum::new(expected_crc32c_be().to_string(), None, None)
        );

        Ok(())
    }

    async fn write_test_files(tmp: TempDir) -> Result<String, Error> {
        let name = tmp.path().to_string_lossy().to_string() + "name";
        let existing = SumsFile::new(
            BTreeSet::from_iter(vec![name.to_string()]),
            Some(TEST_FILE_SIZE),
            BTreeMap::from_iter(vec![(
                "md5".parse()?,
                Checksum::new("123".to_string(), None, None),
            )]),
        );
        existing.write().await?;
        Ok(name)
    }
}
