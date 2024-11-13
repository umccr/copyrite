//! A shared reader implementation which makes use on channels.
//!

use crate::error::Error::OverflowError;
use crate::error::Result;
use crate::reader::SharedReader;
use async_stream::stream;
use futures_util::Stream;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::mpsc;

/// The shared reader implementation using channels.
#[derive(Debug)]
pub struct ChannelReader<R> {
    inner: BufReader<R>,
    txs: Vec<mpsc::Sender<Arc<[u8]>>>,
    capacity: usize,
}

impl<R> ChannelReader<R>
where
    R: AsyncRead + Unpin,
{
    /// Create a new shared reader.
    pub fn new(inner: R, capacity: usize) -> Self {
        Self {
            inner: BufReader::new(inner),
            txs: vec![],
            capacity,
        }
    }

    /// Get the inner buffered reader.
    pub fn into_inner(self) -> BufReader<R> {
        self.inner
    }

    /// Subscribe to the channel returning a stream of elements polled from the sender channel
    pub fn subscribe_stream(&mut self) -> impl Stream<Item = Result<Arc<[u8]>>> {
        let (tx, mut rx) = mpsc::channel(self.capacity);
        self.txs.push(tx);

        stream! {
            let mut msg = rx.recv().await;
            // Poll the channel until the end is reached.
            while let Some(buf) = msg {
                yield Ok(buf);
                msg = rx.recv().await;
            }
        }
    }

    /// Send data to the channel until the end of the reader is reached. Returns the size of the file.
    pub async fn send_to_end(&mut self) -> Result<u64> {
        let txs = self.txs.drain(..);
        let mut size = 0;
        loop {
            // Read data into a buffer.
            let mut buf = vec![0; 1000];
            let n = self.inner.read(&mut buf).await?;

            // Send a stop message if there is no more data.
            if n == 0 {
                break;
            }

            size += n;

            // Send the buffer. An Arc allows sharing the buffer across multiple receivers without
            // copying it.
            let buf: Arc<[u8]> = Arc::from(&buf[0..n]);
            for tx in txs.as_ref() {
                tx.send(buf.clone()).await?;
            }
        }

        // Drop senders to signal closed channel.
        u64::try_from(size).map_err(|_| OverflowError)
    }
}

impl<R> SharedReader for ChannelReader<R>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    async fn read_task(&mut self) -> Result<u64> {
        self.send_to_end().await
    }

    fn as_stream(&mut self) -> impl Stream<Item = Result<Arc<[u8]>>> + 'static {
        self.subscribe_stream()
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::test::TestFileBuilder;
    use anyhow::Result;
    use futures_util::StreamExt;
    use rand::RngCore;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_stream() -> Result<()> {
        let mut rng = TestFileBuilder::default().with_constant_seed().into_rng();
        let mut data = vec![0; 100000];
        rng.fill_bytes(&mut data);

        let mut reader = channel_reader(Cursor::new(data.clone())).await;
        let stream = reader.as_stream();
        reader.read_task().await?;

        let result: Vec<_> = stream
            .map(|value| Ok(value?.to_vec()))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        assert_eq!(result, data);

        Ok(())
    }

    pub(crate) async fn channel_reader<R>(inner: R) -> ChannelReader<R>
    where
        R: AsyncRead + Unpin,
    {
        ChannelReader::new(inner, 1073741825)
    }
}
