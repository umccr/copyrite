//! Generate checksums for files.
//!

use crate::error::Result;
use crate::reader::SharedReader;
use crate::{checksum, Checksum};
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
        reader: &impl SharedReader,
        on_digest: F,
    ) -> Self
    where
        F: FnOnce(Vec<u8>, Checksum) + Send + 'static,
    {
        let ctx = checksum::Checksum::from(checksum);
        let stream = reader.to_stream();
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
        reader: &impl SharedReader,
        on_digest: F,
    ) -> Self
    where
        F: FnOnce(Vec<u8>, Checksum) + Clone + Send + 'static,
    {
        for checksum in checksums {
            self = self.add_generate_task(checksum, reader, on_digest.clone());
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
