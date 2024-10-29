//! A shared reader implementation which makes use on channels.
//!

use crate::error::Result;
use crate::reader::SharedReader;
use async_stream::stream;
use futures_util::Stream;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::broadcast::Sender;
use tokio::task::yield_now;

const SLOW_DOWN_CAPACITY_RATIO: f32 = 0.9;

/// Message type for passing byte data.
#[derive(Debug, Clone)]
pub enum Message {
    Buf(Arc<[u8]>),
    Stop,
}

/// The shared reader implementation using channels.
#[derive(Debug)]
pub struct ChannelReader<R> {
    inner: BufReader<R>,
    tx: Sender<Message>,
    capacity: Option<usize>,
}

impl<R> ChannelReader<R>
where
    R: AsyncRead + Unpin,
{
    /// Create a new shared reader.
    pub fn new(inner: R, tx: Sender<Message>) -> Self {
        Self {
            inner: BufReader::new(inner),
            tx,
            capacity: None,
        }
    }

    pub fn new_with_capacity(inner: R, capacity: usize) -> Self {
        Self {
            inner: BufReader::new(inner),
            tx: Sender::new(capacity),
            capacity: Some(capacity),
        }
    }

    /// Get the inner buffered reader.
    pub fn into_inner(self) -> BufReader<R> {
        self.inner
    }

    /// Subscribe to the channel returning a stream of elements polled from the sender channel
    pub fn subscribe_stream(&self) -> impl Stream<Item = Result<Arc<[u8]>>> {
        let mut rx = self.tx.subscribe();
        stream! {
            let mut msg = rx.recv().await?;
            // Poll the channel until the end is reached.
            while let Message::Buf(buf) = msg {
                yield Ok(buf);
                msg = rx.recv().await?;
            }
        }
    }

    /// Send data to the channel until the end of the reader is reached.
    pub async fn send_to_end(&mut self) -> Result<()> {
        loop {
            // Make sure we don't exceed the capacity of the channel.
            if let Some(capacity) = self.capacity {
                if self.tx.len() >= (SLOW_DOWN_CAPACITY_RATIO * capacity as f32) as usize {
                    yield_now().await;
                }
            }

            // Read data into a buffer.
            let mut buf = vec![0; 1000];
            let n = self.inner.read(&mut buf).await?;

            // Send a stop message if there is no more data.
            if n == 0 {
                self.tx.send(Message::Stop)?;
                break;
            }

            // Send the buffer. An Arc allows sharing the buffer across multiple receivers without
            // copying it.
            self.tx.send(Message::Buf(Arc::from(buf)))?;
        }

        Ok(())
    }
}

impl<R> SharedReader for ChannelReader<R>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    async fn read_task(&mut self) -> Result<()> {
        self.send_to_end().await
    }

    fn to_stream(&self) -> impl Stream<Item = Result<Arc<[u8]>>> + 'static {
        self.subscribe_stream()
    }
}
