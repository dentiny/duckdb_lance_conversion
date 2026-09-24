use std::future::Future;
use std::pin::Pin;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::Stream;

use crate::Result;

pub const DEFAULT_MAX_READ_PARALLELISM: usize = 8;

mod huggingface;
mod parquet;
mod warc;
pub use huggingface::HuggingFaceSource;
pub use warc::{warc_schema, WarcSource};

pub type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send + 'static>>;

#[derive(Clone, Copy, Debug)]
pub struct SourceReadOptions {
    pub max_read_parallelism: usize,
}

impl Default for SourceReadOptions {
    fn default() -> Self {
        Self {
            max_read_parallelism: DEFAULT_MAX_READ_PARALLELISM,
        }
    }
}

impl SourceReadOptions {
    fn validate(self) -> Result<Self> {
        if self.max_read_parallelism == 0 {
            return Err(crate::Error::message(
                "max_read_parallelism must be positive",
            ));
        }
        Ok(self)
    }
}

/// Open an async Arrow batch stream and its fixed schema.
pub trait BatchSource: Send {
    fn open(self) -> impl Future<Output = Result<(SchemaRef, BatchStream)>> + Send;
}
