use std::future::Future;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::Stream;

use crate::Result;

mod lance;
mod lance_index;
pub use lance::{LanceSink, LanceWriter, WriteMode, WriteOptions};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteSummary {
    pub rows_written: u64,
}

/// Consume an async Arrow batch stream with a fixed schema.
pub trait BatchSink: Send {
    fn write<S>(
        self,
        schema: SchemaRef,
        batches: S,
    ) -> impl Future<Output = Result<WriteSummary>> + Send
    where
        S: Stream<Item = Result<RecordBatch>> + Send + 'static;
}
