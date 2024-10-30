//! A shared reader implementation which makes use on channels.
//!

use crate::error::Result;
use crate::reader::SharedReader;
use async_channel::{unbounded, Receiver, Sender};
use async_stream::stream;
use futures_util::Stream;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};

/// The shared reader implementation using channels.
#[derive(Debug)]
pub struct ChannelReader<R> {
    inner: BufReader<R>,
    tx: Sender<Arc<[u8]>>,
    rx: Receiver<Arc<[u8]>>,
}

impl<R> ChannelReader<R>
where
    R: AsyncRead + Unpin,
{
    /// Create a new shared reader.
    pub fn new(inner: R) -> Self {
        let (tx, rx) = unbounded();
        Self {
            inner: BufReader::new(inner),
            tx,
            rx,
        }
    }

    /// Get the inner buffered reader.
    pub fn into_inner(self) -> BufReader<R> {
        self.inner
    }

    /// Subscribe to the channel returning a stream of elements polled from the sender channel
    pub fn subscribe_stream(&self) -> impl Stream<Item = Result<Arc<[u8]>>> {
        let rx = self.rx.clone();

        stream! {
            let mut msg = rx.recv().await;
            // Poll the channel until the end is reached.
            while let Ok(buf) = msg {
                yield Ok(buf);
                msg = rx.recv().await;
            }
        }
    }

    /// Send data to the channel until the end of the reader is reached.
    pub async fn send_to_end(&mut self) -> Result<()> {
        loop {
            // Read data into a buffer.
            let mut buf = vec![0; 1000];
            let n = self.inner.read(&mut buf).await?;

            // Stop loop if there is no more data.
            if n == 0 {
                break;
            }

            // Send the buffer. An Arc allows sharing the buffer across multiple receivers without
            // copying it.
            self.tx.send(Arc::from(buf)).await?;
        }

        Ok(())
    }
}

impl<R> SharedReader for ChannelReader<R>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
{
    async fn read_task(&mut self) -> Result<()> {
        self.send_to_end().await
    }

    fn to_stream(&self) -> impl Stream<Item = Result<Arc<[u8]>>> + 'static {
        self.subscribe_stream()
    }
}
