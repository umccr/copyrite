//! Generate checksums for files.
//!

use crate::checksum::file::{Checksum, SumsFile};
use crate::checksum::Ctx;
use crate::error::Error::GenerateError;
use crate::error::{ApiError, Error, Result};
use crate::io::sums::channel::ChannelReader;
use crate::io::sums::{ObjectSums, ObjectSumsBuilder, SharedReader};
use crate::task::check::{CheckObjects, SumsKey};
use crate::task::generate::Task::{ChecksumTask, ReadTask};
use aws_sdk_s3::Client;
use futures_util::future::join_all;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::result;
use std::sync::Arc;
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
    client: Option<Arc<Client>>,
    avoid_get_object_attributes: bool,
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

    /// Set the S3 client to use.
    pub fn with_client(self, client: Arc<Client>) -> Self {
        self.set_client(Some(client))
    }

    /// Set the S3 client to use.
    pub fn set_client(mut self, client: Option<Arc<Client>>) -> Self {
        self.client = client;
        self
    }

    /// Write the file to the specified location one computed.
    pub fn write(self) -> Self {
        self.set_write(true)
    }

    /// Set the write flag.
    pub fn set_write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Avoid `GetObjectAttributes` calls.
    pub fn with_avoid_get_object_attributes(mut self, avoid_get_object_attributes: bool) -> Self {
        self.avoid_get_object_attributes = avoid_get_object_attributes;
        self
    }

    /// Build a generate task.
    pub async fn build(mut self) -> Result<GenerateTask> {
        let mut sums = ObjectSumsBuilder::default()
            .set_client(self.client)
            .with_avoid_get_object_attributes(self.avoid_get_object_attributes)
            .build(self.input_file_name.to_string())
            .await?;

        let existing_output = if !self.input_file_name.is_empty() {
            sums.sums_file().await?
        } else {
            None
        };

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
            let file_size = sums.file_size().await?;
            self.ctxs
                .iter_mut()
                .for_each(|ctx| ctx.set_file_size(file_size));
            let reader = sums.reader().await?;

            let reader = ChannelReader::new(reader, self.capacity);
            Box::new(reader)
        };

        let task = GenerateTask {
            tasks: Default::default(),
            overwrite: mode,
            existing_output,
            reader: Some(reader),
            write: self.write,
            object_sums: sums,
            updated: false,
            output: Default::default(),
            checksums_generated: Default::default(),
        };

        let task = task.add_tasks(HashSet::from_iter(self.ctxs))?;
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
    overwrite: OverwriteMode,
    existing_output: Option<SumsFile>,
    reader: Option<Box<dyn SharedReader + Send>>,
    write: bool,
    object_sums: Box<dyn ObjectSums + Send>,
    updated: bool,
    output: SumsFile,
    checksums_generated: BTreeMap<Ctx, Checksum>,
}

/// The generate error with the task information when the error occurred.
pub struct GenerateTaskError {
    pub task: GenerateTask,
    pub error: Error,
}

impl Debug for GenerateTaskError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.error)
    }
}

impl From<(GenerateTask, Error)> for GenerateTaskError {
    fn from((task, error): (GenerateTask, Error)) -> Self {
        Self { task, error }
    }
}

impl From<GenerateTaskError> for Error {
    fn from(error: GenerateTaskError) -> Self {
        error.error
    }
}

/// The generate task result type.
pub type GenerateTaskResult = result::Result<GenerateTask, GenerateTaskError>;

impl GenerateTask {
    /// Spawns a task which reads from the buffered reader.
    pub fn add_reader_task(mut self) -> Result<Self> {
        let mut reader = self.reader.take().expect("reader already taken");
        self.tasks.push(tokio::spawn(async move {
            Ok(ReadTask(reader.read_chunks().await?))
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

    fn add_generate_tasks(mut self, checksums: HashSet<Ctx>) -> Self {
        for checksum in checksums {
            self = self.add_generate_task(checksum);
        }
        self
    }

    /// Spawns tasks for a series of checksums.
    pub fn add_tasks(mut self, mut checksums: HashSet<Ctx>) -> Result<Self> {
        let existing = self.existing_output.as_ref();

        match self.overwrite {
            // If verifying, add existing checksums into the set that needs to be generated.
            OverwriteMode::Verify => {
                existing
                    .map(|file| {
                        for name in file.checksums.keys() {
                            checksums.insert(name.clone());
                        }
                        Ok::<_, Error>(())
                    })
                    .transpose()?;
            }
            // Otherwise, if unspecified, remove existing checksums to not re-compute them.
            OverwriteMode::None => {
                existing
                    .map(|file| {
                        for name in file.checksums.keys() {
                            checksums.remove(name);
                        }
                        Ok::<_, Error>(())
                    })
                    .transpose()?;
            }
            // If it's overwriting, just use the checksums as specified on the command line.
            _ => {}
        }

        // Only perform generate tasks if there is something to do.
        if !checksums.is_empty() {
            self = self.add_generate_tasks(checksums).add_reader_task()?;
        }

        Ok(self)
    }

    async fn do_generate(&mut self) -> Result<()> {
        let mut file_size = 0;
        let tasks: Vec<_> = self.tasks.drain(..).collect();
        let checksums = join_all(tasks)
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

                        let checksum = ctx.digest_to_string(&digest);
                        Ok(Some((ctx, Checksum::new(checksum))))
                    }
                }
            })
            .collect::<Result<Vec<Option<(Ctx, Checksum)>>>>()?
            .into_iter()
            .flatten();

        self.checksums_generated = BTreeMap::from_iter(checksums);
        let new_file = SumsFile::new(Some(file_size), self.checksums_generated.clone());

        let output = match self.existing_output.clone() {
            Some(file) if !matches!(self.overwrite, OverwriteMode::Overwrite) => {
                file.merge(new_file)?
            }
            _ => new_file,
        };

        if output.checksums.is_empty() {
            return Err(GenerateError(
                "no checksums were generated because they may not have been specified".to_string(),
            ));
        }

        if self.write {
            let current = self.object_sums.sums_file().await?;

            if current.as_ref() != Some(&output) {
                self.object_sums.write_sums_file(&output).await?;
                self.updated = true;
            }
        }

        self.output = output;

        Ok(())
    }

    /// Runs the generate task, returning an output file.
    pub async fn run(mut self) -> GenerateTaskResult {
        match self.do_generate().await {
            Ok(_) => Ok(self),
            Err(err) => Err((self, err).into()),
        }
    }

    /// Get the inner values.
    pub fn into_inner(
        self,
    ) -> (
        SumsFile,
        Box<dyn ObjectSums + Send>,
        bool,
        BTreeMap<Ctx, Checksum>,
    ) {
        (
            self.output,
            self.object_sums,
            self.updated,
            self.checksums_generated,
        )
    }

    /// Get the api errors.
    pub fn api_errors(&self) -> HashSet<ApiError> {
        self.object_sums.api_errors()
    }

    /// Return the computed sums file.
    pub fn sums_file(&self) -> &SumsFile {
        &self.output
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
    pub fn from_comparable(files: CheckObjects) -> Result<Option<Self>> {
        // Get the checksum which contains the most amount of occurrences across groups of sums files.
        let file_ctx = files
            .0
            .iter()
            .flat_map(|(file, _)| file.0 .0.checksums.keys().cloned())
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
                .0
                .into_iter()
                .flat_map(|(SumsKey((file, _)), state)| {
                    // If the sums group already contains this checksum, skip.
                    if file.checksums.contains_key(&file_ctx) {
                        return None;
                    }
                    file_ctx.set_file_size(file.size);

                    let first = state.into_iter().next();
                    first.map(|state| SumCtxPair::new(state.location(), file_ctx.clone()))
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
    use crate::io::sums::channel::test::channel_reader;
    use crate::io::sums::file::FileBuilder;
    use crate::task::check::test::write_test_files_not_comparable;
    use crate::task::check::{CheckTaskBuilder, GroupBy};
    use crate::test::{TestFileBuilder, TEST_FILE_SIZE};
    use anyhow::Result;
    use std::path::Path;
    use tempfile::tempdir;
    use tokio::fs::File;

    #[tokio::test]
    async fn test_sum_ctx_pairs() -> Result<()> {
        let tmp = tempdir()?;
        let files = write_test_files_not_comparable(tmp).await?;

        let check = CheckTaskBuilder::default()
            .with_input_files(files.iter().map(|name| name.to_string()).collect())
            .with_group_by(GroupBy::Comparability)
            .build()
            .await?;
        let (objects, _, _, _) = check.run().await.unwrap().into_inner();

        let result = SumCtxPairs::from_comparable(objects)?.unwrap();

        assert_eq!(
            result,
            vec![SumCtxPair::new(
                files[2].to_string(),
                Ctx::Regular(StandardCtx::sha256())
            )]
            .into()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_generate_overwrite() -> Result<()> {
        let tmp = tempdir()?;
        let name = write_test_files(tmp.path()).await?;

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
        let name = write_test_files(tmp.path()).await?;

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
        let name = write_test_files(tmp.path()).await?;

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
        name: &str,
        tasks: Vec<&str>,
        overwrite: bool,
        verify: bool,
    ) -> Result<SumsFile> {
        let test_file = TestFileBuilder::default().generate_test_defaults()?;

        let file = File::open(test_file).await?;
        let reader = channel_reader(file).await;

        let mut tasks: Vec<Ctx> = tasks
            .into_iter()
            .map(|task| Ok(task.parse()?))
            .collect::<Result<Vec<_>>>()?;

        tasks
            .iter_mut()
            .for_each(|task| task.set_file_size(Some(TEST_FILE_SIZE)));

        Ok(GenerateTaskBuilder::default()
            .with_input_file_name(name.to_string())
            .with_overwrite(overwrite)
            .with_verify(verify)
            .with_reader(reader)
            .with_context(tasks)
            .build()
            .await?
            .run()
            .await
            .unwrap()
            .into_inner()
            .0)
    }

    async fn test_generate(
        name: String,
        overwrite: bool,
        verify: bool,
        tasks: Vec<&str>,
        md5: &str,
    ) -> Result<()> {
        let file = generate_for(&name, tasks, overwrite, verify).await?;

        assert_eq!(file.size, Some(TEST_FILE_SIZE));
        assert_eq!(
            file.checksums[&"md5".parse()?],
            Checksum::new(md5.to_string())
        );
        assert_eq!(
            file.checksums[&"sha1".parse()?],
            Checksum::new(EXPECTED_SHA1_SUM.to_string())
        );
        assert_eq!(
            file.checksums[&"sha256".parse()?],
            Checksum::new(EXPECTED_SHA256_SUM.to_string())
        );
        assert_eq!(
            file.checksums[&"md5-aws-1073741824b".parse()?],
            Checksum::new(expected_md5_1gib().to_string())
        );
        assert_eq!(
            file.checksums[&"crc32".parse()?],
            Checksum::new(EXPECTED_CRC32_BE_SUM.to_string())
        );
        assert_eq!(
            file.checksums[&"crc32c".parse()?],
            Checksum::new(EXPECTED_CRC32C_BE_SUM.to_string())
        );

        Ok(())
    }

    async fn write_test_files(tmp: &Path) -> Result<String, Error> {
        let name = tmp.join("name").to_string_lossy().to_string();
        let existing = SumsFile::new(
            Some(TEST_FILE_SIZE),
            BTreeMap::from_iter(vec![("md5".parse()?, Checksum::new("123".to_string()))]),
        );
        FileBuilder::default()
            .with_file(name.to_string())
            .build()?
            .write_sums(&existing)
            .await?;

        Ok(name)
    }
}
