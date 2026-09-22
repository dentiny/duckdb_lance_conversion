use std::future::Future;

use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::Stream;

mod lance;
pub use lance::{LanceSink, LanceWriter, WriteOptions};

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
