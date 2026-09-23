use std::future::Future;
use std::pin::Pin;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::Stream;

use crate::Result;

mod parquet;
pub use parquet::ParquetFileSource;

pub type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send + 'static>>;

/// Open an async Arrow batch stream and its fixed schema.
pub trait BatchSource: Send {
    fn open(self) -> impl Future<Output = Result<(SchemaRef, BatchStream)>> + Send;
}
