//! Generate checksums for files.
//!

use crate::checksum::file::{Checksum, PartChecksum, PartChecksums, State, SumsFile};
use crate::checksum::Ctx;
use crate::cloud::ObjectSumsBuilder;
use crate::error::Error::GenerateError;
use crate::error::{Error, Result};
use crate::reader::channel::ChannelReader;
use crate::reader::SharedReader;
use crate::task::generate::Task::{ChecksumTask, ReadTask};
use futures_util::future::join_all;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use tokio::task::JoinHandle;

/// Define the kind of task that is running.
#[derive(Debug)]
pub enum Task {
    ReadTask(u64),
    ChecksumTask(Box<(Ctx, Vec<u8>)>),
}

/// Build a generate task.
#[derive(Default)]
pub struct GenerateTaskBuilder {
    input_file_name: String,
    overwrite: bool,
    verify: bool,
    ctxs: Vec<Ctx>,
    reader: Option<Box<dyn SharedReader + Send>>,
    capacity: usize,
    write: bool,
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

    /// Set the generate contexts.
    pub fn with_context(mut self, ctxs: Vec<Ctx>) -> Self {
        self.ctxs = ctxs;
        self
    }

    /// Set the reader directly.
    pub fn with_reader(mut self, reader: impl SharedReader + Send + 'static) -> Self {
        self.reader = Some(Box::new(reader));
        self
    }

    /// Set the reader capacity.
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity;
        self
    }

    /// Write the file to the specified location one computed.
    pub fn write(mut self) -> Self {
        self.write = true;
        self
    }

    /// Build a generate task.
    pub async fn build(mut self) -> Result<GenerateTask> {
        let mut object_sums = ObjectSumsBuilder
            .build(self.input_file_name.to_string())
            .await?;

        let existing_output = if !self.input_file_name.is_empty() {
            object_sums.sums_file().await?
        } else {
            None
        };

        if self.overwrite && self.verify {
            return Err(GenerateError(
                "cannot verify and overwrite checksums".to_string(),
            ));
        }

        if self.ctxs.is_empty() {
            return Err(GenerateError("checksums not specified".to_string()));
        }

        let mode = if self.overwrite {
            OverwriteMode::Overwrite
        } else if self.verify {
            OverwriteMode::Verify
        } else {
            OverwriteMode::None
        };

        let reader: Box<dyn SharedReader + Send> = if let Some(reader) = self.reader {
            reader
        } else {
            let mut object_sums = ObjectSumsBuilder
                .build(self.input_file_name.to_string())
                .await?;
            let file_size = object_sums.file_size().await?;
            self.ctxs
                .iter_mut()
                .for_each(|ctx| ctx.set_file_size(Some(file_size)));
            let reader = object_sums.reader().await?;

            let reader = ChannelReader::new(reader, self.capacity);
            Box::new(reader)
        };

        let task = GenerateTask {
            tasks: Default::default(),
            input_file_name: self.input_file_name,
            overwrite: mode,
            existing_output,
            reader: Some(reader),
            write: self.write,
        };

        let task = task
            .add_generate_tasks(HashSet::from_iter(self.ctxs))?
            .add_reader_task()?;
        Ok(task)
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
pub struct GenerateTask {
    tasks: Vec<JoinHandle<Result<Task>>>,
    input_file_name: String,
    overwrite: OverwriteMode,
    existing_output: Option<SumsFile>,
    reader: Option<Box<dyn SharedReader + Send>>,
    write: bool,
}

impl GenerateTask {
    /// Spawns a task which reads from the buffered reader.
    pub fn add_reader_task(mut self) -> Result<Self> {
        let mut reader = self.reader.take().expect("reader already taken");
        self.tasks.push(tokio::spawn(async move {
            Ok(ReadTask(reader.read_task().await?))
        }));
        Ok(self)
    }

    /// Spawns a task which generates checksums.
    pub fn add_generate_task(mut self, mut ctx: Ctx) -> Self {
        let stream = self
            .reader
            .as_mut()
            .map(|reader| reader.as_stream())
            .expect("missing reader");
        self.tasks.push(tokio::spawn(async move {
            let stream = ctx.generate(stream);

            let digest = stream.await?;

            Ok(ChecksumTask(Box::new((ctx, digest))))
        }));

        self
    }

    fn add_generate_tasks_direct(mut self, checksums: HashSet<Ctx>) -> Self {
        for checksum in checksums {
            self = self.add_generate_task(checksum);
        }
        self
    }

    /// Spawns tasks for a series of checksums.
    pub fn add_generate_tasks(mut self, mut checksums: HashSet<Ctx>) -> Result<Self> {
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

                self = self.add_generate_tasks_direct(checksums);
            }
            OverwriteMode::Overwrite | OverwriteMode::None => {
                self = self.add_generate_tasks_direct(checksums);
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

                        let part_checksums = ctx.part_checksums().map(|sums| {
                            PartChecksums::new(
                                sums.into_iter()
                                    .map(|(part_size, sum)| {
                                        PartChecksum::new(Some(part_size), Some(sum))
                                    })
                                    .collect::<Vec<_>>(),
                            )
                        });

                        let checksum = ctx.digest_to_string(&digest);
                        Ok(Some((ctx, Checksum::new(checksum, part_checksums))))
                    }
                }
            })
            .collect::<Result<Vec<Option<(Ctx, Checksum)>>>>()?
            .into_iter()
            .flatten();

        let checksums = BTreeMap::from_iter(checksums);
        let new_file = SumsFile::new(
            BTreeSet::from_iter(vec![State::try_from(self.input_file_name).await?]),
            Some(file_size),
            checksums,
        );

        let output = match self.existing_output {
            Some(file) if !matches!(self.overwrite, OverwriteMode::Overwrite) => {
                file.merge(new_file)?
            }
            _ => new_file,
        };

        if self.write {
            output.write().await?;
        }

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
                .flat_map(|mut file| {
                    // If the sums group already contains this checksum, skip.
                    if file.checksums.contains_key(&file_ctx) {
                        return None;
                    }
                    file_ctx.set_file_size(file.size);

                    let first = file.state.pop_first();
                    first.map(|state| SumCtxPair::new(state.name, file_ctx.clone()))
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

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::checksum::aws_etag::test::expected_md5_1gib;
    use crate::checksum::standard::test::{
        EXPECTED_CRC32C_BE_SUM, EXPECTED_CRC32_BE_SUM, EXPECTED_MD5_SUM, EXPECTED_SHA1_SUM,
        EXPECTED_SHA256_SUM,
    };
    use crate::checksum::standard::StandardCtx;
    use crate::task::check::test::write_test_files_not_comparable;
    use crate::task::check::{CheckTaskBuilder, GroupBy};
    use crate::test::{TestFileBuilder, TEST_FILE_SIZE};
    use crate::Endianness;
    use anyhow::Result;
    use std::collections::BTreeSet;
    use tempfile::{tempdir, TempDir};

    #[tokio::test]
    async fn test_sum_ctx_pairs() -> Result<()> {
        let tmp = tempdir()?;
        let files = write_test_files_not_comparable(tmp).await?;

        let check = CheckTaskBuilder::default()
            .with_input_files(files.iter().map(|state| state.name.to_string()).collect())
            .with_group_by(GroupBy::Comparability)
            .build()
            .await?;
        let check = check.run().await?;

        let result = SumCtxPairs::from_comparable(check)?.unwrap();

        assert_eq!(
            result,
            vec![SumCtxPair::new(
                files.first().unwrap().clone().name,
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
            EXPECTED_MD5_SUM,
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
            EXPECTED_MD5_SUM,
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

    pub(crate) async fn generate_for(
        state: State,
        tasks: Vec<&str>,
        overwrite: bool,
        verify: bool,
    ) -> Result<SumsFile> {
        TestFileBuilder::default().generate_test_defaults()?;

        let tasks: Vec<Ctx> = tasks
            .into_iter()
            .map(|task| Ok(task.parse()?))
            .collect::<Result<Vec<_>>>()?;

        Ok(GenerateTaskBuilder::default()
            .with_input_file_name(state.name)
            .with_overwrite(overwrite)
            .with_verify(verify)
            .with_context(tasks)
            .build()
            .await?
            .run()
            .await?)
    }

    async fn test_generate(
        state: State,
        overwrite: bool,
        verify: bool,
        tasks: Vec<&str>,
        md5: &str,
    ) -> Result<()> {
        let file = generate_for(state.clone(), tasks, overwrite, verify).await?;

        assert_eq!(file.state, BTreeSet::from_iter(vec![state]));
        assert_eq!(file.size, Some(TEST_FILE_SIZE));
        assert_eq!(
            file.checksums[&"md5".parse()?],
            Checksum::new(md5.to_string(), None)
        );
        assert_eq!(
            file.checksums[&"sha1".parse()?],
            Checksum::new(EXPECTED_SHA1_SUM.to_string(), None)
        );
        assert_eq!(
            file.checksums[&"sha256".parse()?],
            Checksum::new(EXPECTED_SHA256_SUM.to_string(), None)
        );
        assert_eq!(
            file.checksums[&"md5-aws-1073741824b".parse()?],
            Checksum::new(
                expected_md5_1gib().to_string(),
                Some(
                    vec![(
                        Some(1073741824),
                        Some("d93e71879054f205ede90d35c8081ca5".to_string())
                    )]
                    .into()
                )
            )
        );
        assert_eq!(
            file.checksums[&"crc32".parse()?],
            Checksum::new(EXPECTED_CRC32_BE_SUM.to_string(), None)
        );
        assert_eq!(
            file.checksums[&"crc32c".parse()?],
            Checksum::new(EXPECTED_CRC32C_BE_SUM.to_string(), None)
        );

        Ok(())
    }

    async fn write_test_files(tmp: TempDir) -> Result<State, Error> {
        let name = tmp.path().to_string_lossy().to_string() + "name";
        let name = State::try_from(name).await?;
        let existing = SumsFile::new(
            BTreeSet::from_iter(vec![name.clone()]),
            Some(TEST_FILE_SIZE),
            BTreeMap::from_iter(vec![(
                "md5".parse()?,
                Checksum::new("123".to_string(), None),
            )]),
        );
        existing.write().await?;

        Ok(name)
    }
}
