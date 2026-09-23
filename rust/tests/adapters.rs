use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::SchemaRef;
use futures::{stream, Stream, TryStreamExt};
use lance_conversion::{convert, BatchSink, BatchSource, BatchStream, Error, Result, WriteSummary};

struct MemorySource(RecordBatch);

impl BatchSource for MemorySource {
    async fn open(self) -> Result<(SchemaRef, BatchStream)> {
        let schema = self.0.schema();
        let batches = vec![Ok(self.0.slice(0, 2)), Ok(self.0.slice(2, 1))];
        Ok((schema, Box::pin(stream::iter(batches))))
    }
}

struct MemorySink(RecordBatch);

impl BatchSink for MemorySink {
    async fn write<S>(self, schema: SchemaRef, batches: S) -> Result<WriteSummary>
    where
        S: Stream<Item = Result<RecordBatch>> + Send + 'static,
    {
        if schema != self.0.schema() {
            return Err(Error::message("schema changed"));
        }
        let mut batches = Box::pin(batches);
        let mut rows_written = 0;
        while let Some(batch) = batches.try_next().await? {
            let expected = self.0.slice(rows_written, batch.num_rows());
            assert_eq!(batch, expected);
            rows_written += batch.num_rows();
        }
        Ok(WriteSummary {
            rows_written: rows_written as u64,
        })
    }
}

#[tokio::test]
async fn converts_between_custom_arrow_adapters() {
    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as arrow_array::ArrayRef,
    )])
    .unwrap();
    let result = convert(MemorySource(batch.clone()), MemorySink(batch))
        .await
        .unwrap();
    assert_eq!(result.rows_written, 3);
}
