//! Generate checksums for files.
//!

use crate::error::Result;
use crate::reader::SharedReader;
use crate::{checksum, Checksum};
use futures_util::future::join_all;
use hex::encode;
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

    pub fn add_generate_task(mut self, checksum: Checksum, reader: &impl SharedReader) -> Self {
        let ctx = checksum::Checksum::from(checksum);
        let stream = reader.to_stream();
        self.tasks.push(tokio::spawn(async move {
            let stream = ctx.generate(stream);

            let digest = stream.await?;

            println!("The {:#?} digest is: {}", checksum, encode(digest));

            Ok(())
        }));

        self
    }

    pub fn add_generate_tasks(
        mut self,
        checksums: Vec<Checksum>,
        reader: &impl SharedReader,
    ) -> Self {
        for checksum in checksums {
            self = self.add_generate_task(checksum, reader);
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
