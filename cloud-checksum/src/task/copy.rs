//! The copy command task implementation.
//!

use crate::error::Result;
use crate::io::reader::ObjectRead;
use crate::io::writer::ObjectWrite;
use crate::io::{aws, file, IoBuilder, Provider};
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use crate::error::Error::CopyError;

/// Build a copy task.
#[derive(Default)]
pub struct CopyTaskBuilder {
    source: String,
    destination: String,
    multipart_threshold: Option<u64>,
    part_size: Option<u64>,
}

impl CopyTaskBuilder {
    /// Set the source
    pub fn with_source(mut self, source: String) -> Self {
        self.source = source;
        self
    }

    /// Set the destination.
    pub fn with_destination(mut self, destination: String) -> Self {
        self.destination = destination;
        self
    }
    
    /// Set the multipart threshold.
    pub fn with_multipart_threshold(mut self, multipart_threshold: u64) -> Self {
        self.multipart_threshold = Some(multipart_threshold);
        self
    }
    
    /// Set the part size.
    pub fn with_part_size(mut self, part_size: u64) -> Self {
        self.part_size = Some(part_size);
        self
    }
    
    /// Build a generate task.
    pub async fn build(self) -> Result<CopyTask> {
        if self.source.is_empty() || self.destination.is_empty() {
            return Err(CopyError("source and destination required".to_string()));
        }
        
        let object_read = IoBuilder.build_read(self.source.to_string()).await?;
        let object_write = IoBuilder.build_write(self.source.to_string()).await?;

        let copy_task = CopyTask {
            source: self.source,
            destination: self.destination,
            multipart_threshold: self.multipart_threshold,
            part_size: self.part_size,
            object_read,
            object_write,
        };

        Ok(copy_task)
    }
}

/// Output of the copy task.
#[derive(Debug, Serialize, Deserialize)]
pub struct CopyInfo {
    source: String,
    destination: String,
    total_bytes: u64,
}

impl CopyInfo {
    /// Convert to a JSON string.
    pub fn to_json_string(&self) -> Result<String> {
        Ok(to_string(&self)?)
    }
}

/// Execute the copy task.
pub struct CopyTask {
    source: String,
    destination: String,
    multipart_threshold: Option<u64>,
    part_size: Option<u64>,
    object_read: Box<dyn ObjectRead + Send>,
    object_write: Box<dyn ObjectWrite + Send>,
}

impl CopyTask {
    /// Runs the copy task and return the output.
    pub async fn run(mut self) -> Result<CopyInfo> {
        // match (Provider::from(self.source.as_str()), Provider::from(self.destination.as_str())) {
        //     // Direct file-to-file just copies the source to destination.
        //     (Provider::File, Provider::File) => {
        //         
        //     },
        //     // Direct s3-to-s3 copies source to destination using `CopyObject`.
        //     (Provider::S3, Provider::S3) => {},
        //     // file-to-s3 requires reading the file and writing to s3.
        //     (Provider::File, Provider::S3) => {},
        //     // s3-to-file requires downloading the file to the destination.
        //     (Provider::S3, Provider::File) => {},
        // }

        let total = self
            .object_write
            .copy_object(self.destination.clone())
            .await?;

        Ok(CopyInfo {
            source: self.source,
            destination: self.destination,
            total_bytes: total,
        })
    }
}
