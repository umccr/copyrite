//! Generate checksums for files.
//!

use crate::error::Result;
use crate::reader::SharedReader;
use crate::{checksum, Checksum, Endianness};
use futures_util::future::join_all;
use tokio::task::JoinHandle;

/// Execute the generate checksums tasks.
#[derive(Debug, Default)]
pub struct GenerateTask {
    tasks: Vec<JoinHandle<Result<()>>>,
}

impl GenerateTask {
    pub fn add_reader_task(mut self, mut reader: impl SharedReader + 'static) -> Result<Self> {
        self.tasks
            .push(tokio::spawn(async move { reader.read_task().await }));
        Ok(self)
    }

    pub fn add_generate_task<F>(
        mut self,
        checksum: Checksum,
        endianness: Endianness,
        reader: &mut impl SharedReader,
        on_digest: F,
    ) -> Self
    where
        F: FnOnce(Vec<u8>, Checksum) + Send + 'static,
    {
        let ctx = checksum::ChecksumCtx::from(checksum).with_endianness(endianness);
        let stream = reader.as_stream();
        self.tasks.push(tokio::spawn(async move {
            let stream = ctx.generate(stream);

            let digest = stream.await?;

            on_digest(digest, checksum);
            Ok(())
        }));

        self
    }

    pub fn add_generate_tasks<F>(
        mut self,
        checksums: Vec<Checksum>,
        endianness: Endianness,
        reader: &mut impl SharedReader,
        on_digest: F,
    ) -> Self
    where
        F: FnOnce(Vec<u8>, Checksum) + Clone + Send + 'static,
    {
        for checksum in checksums {
            self = self.add_generate_task(checksum, endianness, reader, on_digest.clone());
        }
        self
    }

    pub async fn run(self) -> Result<Vec<()>> {
        join_all(self.tasks)
            .await
            .into_iter()
            .map(|val| val?)
            .collect::<Result<Vec<_>>>()
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::checksum::test::{
        expected_crc32_be, expected_md5_sum, expected_sha1_sum, expected_sha256_sum,
    };
    use crate::reader::channel::test::channel_reader;
    use crate::test::TestFileBuilder;
    use anyhow::Result;
    use hex::encode;
    use tokio::fs::File;

    #[tokio::test]
    async fn test_generate() -> Result<()> {
        let test_file = TestFileBuilder::default().generate_test_defaults()?;
        let mut reader = channel_reader(File::open(test_file).await?).await;

        GenerateTask::default()
            .add_generate_tasks(
                vec![Checksum::SHA1, Checksum::MD5, Checksum::SHA256],
                Endianness::BigEndian,
                &mut reader,
                |digest, checksum| match checksum {
                    Checksum::MD5 => assert_eq!(encode(digest), expected_md5_sum()),
                    Checksum::SHA1 => assert_eq!(encode(digest), expected_sha1_sum()),
                    Checksum::SHA256 => assert_eq!(encode(digest), expected_sha256_sum()),
                    Checksum::CRC32 => assert_eq!(encode(digest), expected_crc32_be()),
                    _ => {}
                },
            )
            .add_reader_task(reader)?
            .run()
            .await?;

        Ok(())
    }
}
