//! Task definitions for different commands.
//!

pub mod check;
pub mod copy;
pub mod generate;

use crate::io::S3Client;

/// An input location paired with the S3 client used to access it.
#[derive(Debug, Clone, Default)]
pub struct ClientInput {
    location: String,
    client: Option<S3Client>,
}

impl ClientInput {
    /// Create a new client input.
    pub fn new(location: String, client: Option<S3Client>) -> Self {
        Self { location, client }
    }

    /// The input location.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// The client for this location, if any.
    pub fn client(&self) -> Option<S3Client> {
        self.client.clone()
    }

    /// Get the inner values.
    pub fn into_inner(self) -> (String, Option<S3Client>) {
        (self.location, self.client)
    }
}
