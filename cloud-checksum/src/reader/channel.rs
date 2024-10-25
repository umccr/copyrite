//! A shared reader implementation which makes use on channels.
//!

use crate::error::Result;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::broadcast::{Receiver, Sender};

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
        }
    }

    /// Get the inner buffered reader.
    pub fn into_inner(self) -> BufReader<R> {
        self.inner
    }

    /// Subscribe to the channel.
    pub fn subscribe(&self) -> Receiver<Message> {
        self.tx.subscribe()
    }

    /// Send data to the channel until the end of the reader is reached.
    pub async fn send_to_end(&mut self) -> Result<()> {
        loop {
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
