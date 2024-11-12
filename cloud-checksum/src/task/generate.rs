//! Generate checksums for files.
//!

use crate::checksum::file::{OutputChecksum, OutputFile};
use crate::checksum::ChecksumCtx;
use crate::error::Result;
use crate::reader::SharedReader;
use crate::task::generate::Task::{ChecksumTask, ReadTask};
use futures_util::future::join_all;
use std::collections::HashMap;
use tokio::task::JoinHandle;

/// Define the kind of task that is running.
#[derive(Debug)]
pub enum Task {
    ReadTask(u64),
    ChecksumTask((ChecksumCtx, Vec<u8>)),
}

/// Build a generate task.
#[derive(Debug, Default)]
pub struct GenerateTaskBuilder {
    input_file_name: String,
    overwrite: bool,
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

    /// Build a generate task.
    pub async fn build(self) -> Result<GenerateTask> {
        let existing_output = OutputFile::read_from(&self.input_file_name).await;
        Ok(GenerateTask {
            input_file_name: self.input_file_name,
            overwrite: self.overwrite,
            existing_output: existing_output.ok(),
            ..Default::default()
        })
    }
}

/// Execute the generate checksums tasks.
#[derive(Debug, Default)]
pub struct GenerateTask {
    tasks: Vec<JoinHandle<Result<Task>>>,
    input_file_name: String,
    overwrite: bool,
    existing_output: Option<OutputFile>,
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
    pub fn add_generate_task(
        mut self,
        mut ctx: ChecksumCtx,
        reader: &mut impl SharedReader,
    ) -> Self {
        let stream = reader.as_stream();
        self.tasks.push(tokio::spawn(async move {
            let stream = ctx.generate(stream);

            let digest = stream.await?;

            Ok(ChecksumTask((ctx, digest)))
        }));

        self
    }

    /// Spawns tasks for a series of checksums.
    pub fn add_generate_tasks(
        mut self,
        checksums: Vec<ChecksumCtx>,
        reader: &mut impl SharedReader,
    ) -> Self {
        for checksum in checksums {
            if self.overwrite
                || !self
                    .existing_output
                    .as_ref()
                    .is_some_and(|file| file.checksums.contains_key(&checksum.to_string()))
            {
                self = self.add_generate_task(checksum, reader);
            }
        }
        self
    }

    /// Runs the generate task, returning an output file.
    pub async fn run(self) -> Result<OutputFile> {
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
                    ChecksumTask((ctx, digest)) => {
                        // Todo part-size outputs.
                        let checksum = ctx.digest_to_string(digest);
                        Ok(Some((
                            ctx.to_string(),
                            OutputChecksum::new(checksum, None, None),
                        )))
                    }
                }
            })
            .collect::<Result<Vec<Option<(String, OutputChecksum)>>>>()?
            .into_iter()
            .flatten();

        let checksums = HashMap::from_iter(checksums);

        let new_file = OutputFile::new(self.input_file_name, file_size, checksums);
        let output = match self.existing_output {
            Some(file) if !self.overwrite => file.merge(new_file),
            _ => new_file,
        };

        Ok(output)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::checksum::test::{
        expected_crc32_be, expected_crc32c_be, expected_md5_sum, expected_sha1_sum,
        expected_sha256_sum,
    };
    use crate::reader::channel::test::channel_reader;
    use crate::test::{TestFileBuilder, TEST_FILE_SIZE};
    use anyhow::Result;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use hex::decode;
    use tokio::fs::File;

    #[tokio::test]
    async fn test_generate() -> Result<()> {
        let test_file = TestFileBuilder::default().generate_test_defaults()?;
        let mut reader = channel_reader(File::open(test_file).await?).await;

        let file = GenerateTaskBuilder::default()
            .with_input_file_name("name".to_string())
            .build()
            .await?
            .add_generate_tasks(
                vec![
                    "sha1".parse()?,
                    "sha256".parse()?,
                    "md5".parse()?,
                    "aws-etag".parse()?,
                    "crc32".parse()?,
                    "crc32c".parse()?,
                ],
                &mut reader,
            )
            .add_reader_task(reader)?
            .run()
            .await?;

        assert_eq!(file.name, "name");
        assert_eq!(file.size, TEST_FILE_SIZE);
        assert_eq!(
            file.checksums["md5"],
            OutputChecksum::new(expected_md5_sum().to_string(), None, None)
        );
        assert_eq!(
            file.checksums["sha1"],
            OutputChecksum::new(expected_sha1_sum().to_string(), None, None)
        );
        assert_eq!(
            file.checksums["sha256"],
            OutputChecksum::new(expected_sha256_sum().to_string(), None, None)
        );
        assert_eq!(
            file.checksums["aws-etag"],
            OutputChecksum::new(STANDARD.encode(decode(expected_md5_sum())?), None, None)
        );
        assert_eq!(
            file.checksums["crc32"],
            OutputChecksum::new(expected_crc32_be().to_string(), None, None)
        );
        assert_eq!(
            file.checksums["crc32c"],
            OutputChecksum::new(expected_crc32c_be().to_string(), None, None)
        );

        Ok(())
    }
}
