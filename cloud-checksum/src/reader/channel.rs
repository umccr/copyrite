//! A shared reader implementation which makes use on channels.
//!

use crate::error::Result;
use crate::reader::channel::Message::{Buf, Stop};
use crate::reader::SharedReader;
use async_broadcast::{broadcast, Receiver, Sender};
use async_stream::stream;
use futures_util::Stream;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc, Notify, Semaphore};
use tokio::task::yield_now;
use tokio::time::sleep;

/// Message type for passing byte data.
#[derive(Debug, Clone)]
pub enum Message {
    Buf(Arc<[u8]>),
    Stop,
}

const SLOW_DOWN_CAPACITY_RATIO: f32 = 0.9;

/// The shared reader implementation using channels.
#[derive(Debug)]
pub struct ChannelReader<R> {
    inner: BufReader<R>,
    tx: Option<broadcast::Sender<Message>>,
    rx: broadcast::Receiver<Message>,
    back_tx: mpsc::Sender<bool>,
    back_rx: Option<mpsc::Receiver<bool>>,
    chunk_size: usize,
    semaphore: Semaphore,
    notify: Notify,
}

impl<R> ChannelReader<R>
where
    R: AsyncRead + Unpin,
{
    /// Create a new shared reader.
    pub fn new(inner: R, chunk_size: usize, subscribers: usize) -> Self {
        let (mut tx, rx) = broadcast::channel(1);

        let (back_tx, back_rx) = mpsc::channel(5);
        Self {
            inner: BufReader::new(inner),
            tx: Some(tx),
            rx,
            chunk_size,
            back_rx: Some(back_rx),
            back_tx,
            semaphore: Semaphore::new(1),
            notify: Notify::new(),
        }
    }

    /// Get the inner buffered reader.
    pub fn into_inner(self) -> BufReader<R> {
        self.inner
    }

    /// Subscribe to the channel returning a stream of elements polled from the sender channel
    pub fn subscribe_stream(&self) -> impl Stream<Item = Result<Arc<[u8]>>> {
        let mut rx = self.tx.as_ref().unwrap().subscribe();
        let back_tx = self.back_tx.clone();
        stream! {
            let mut msg = rx.recv().await?;

            // Poll the channel until the end is reached.
            while let Buf(buf) = msg {
                back_tx.send(true).await.unwrap();
                yield Ok(buf);
                msg = rx.recv().await?;
            }
        }
    }

    /// Send data to the channel until the end of the reader is reached.
    pub async fn send_to_end(&mut self) -> Result<()> {
        // Get the sender and drop it at the end of this function to signal the end of the stream.
        let tx = self
            .tx
            .take()
            .expect("cannot call send_to_end more than once");
        let back_tx = self.back_tx.send(true).await.unwrap();
        let mut rx = self.back_rx.take().unwrap();

        loop {
            let proceed = rx.recv().await.unwrap();

            // Read data into a buffer.
            let mut buf = vec![0; self.chunk_size];
            let n = self.inner.read(&mut buf).await?;

            // Stop loop if there is no more data.
            if n == 0 {
                tx.send(Stop)?;
                break;
            }

            // Send the buffer given the number of bytes read. An Arc allows sharing the
            // buffer across multiple receivers without copying it.
            tx.send(Buf(Arc::from(&buf[0..n])))?;
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
        let stream = reader.to_stream();
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
        ChannelReader::new(inner, 1000, 1000)
    }
}
