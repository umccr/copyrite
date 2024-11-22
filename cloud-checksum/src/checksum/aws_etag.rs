//! Compute a checksum using an AWS ETag style, i.e. combined checksums
//! of the parts of a file.
//!

use crate::checksum::ChecksumCtx;
use std::sync::Arc;

#[derive(Debug)]
pub struct AWSEtagChecksum {
    part_size: u64,
    current_bytes: u64,
    remainder: Option<Arc<[u8]>>,
    part_checksums: Vec<Vec<u8>>,
    checksummer: ChecksumCtx,
}

impl AWSEtagChecksum {
    pub fn update(&mut self, data: Arc<[u8]>) {
        self.current_bytes += data.len() as u64;

        if self.current_bytes > self.part_size {
            let (data, remainder) =
                data.split_at((self.part_size - self.current_bytes) as usize - data.len());

            self.current_bytes = remainder.len() as u64;
            self.remainder = Some(Arc::from(remainder));

            self.checksummer.update(Arc::from(data));

            self.part_checksums.push(self.checksummer.finalize());

            self.checksummer = self.checksummer.reset();
        } else {
            let remainder = self.remainder.take();
            if let Some(remainder) = remainder {
                self.checksummer.update(remainder);
                self.remainder = None;
            }

            self.checksummer.update(data);
        }
    }

    pub fn finalize(mut self) -> Vec<u8> {
        let concat: Vec<_> = self.part_checksums.into_iter().flatten().collect();
        self.checksummer.update(Arc::from(concat.as_slice()));
        self.checksummer.finalize()
    }
}
