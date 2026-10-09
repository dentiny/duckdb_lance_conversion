use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use futures::{future, stream, StreamExt, TryStreamExt};
use parquet::arrow::async_reader::{ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_util::task::AbortOnDropHandle;

use super::parquet_metadata::{load_parquet_metadata, OpendalParquetReader};
use super::{BatchStream, ReadMetrics};
use crate::{Error, Result};

/// Decoded bytes one row-group task may queue ahead of its consumer.
///
/// Each queued batch holds permits equal to its Arrow memory size until the
/// consumer takes it. A batch larger than the whole budget takes every permit,
/// so it still makes progress, one batch at a time.
struct ReadAheadBudget {
    permits: Arc<Semaphore>,
    capacity: u32,
}

impl ReadAheadBudget {
    fn new(bytes: usize) -> Self {
        let capacity = u32::try_from(bytes).unwrap_or(u32::MAX).max(1);
        Self {
            permits: Arc::new(Semaphore::new(capacity as usize)),
            capacity,
        }
    }

    /// Waits until `batch` fits.
    async fn reserve(&self, batch: &RecordBatch) -> OwnedSemaphorePermit {
        let bytes = u32::try_from(batch.get_array_memory_size()).unwrap_or(u32::MAX);
        self.permits
            .clone()
            .acquire_many_owned(bytes.clamp(1, self.capacity))
            .await
            .expect("read-ahead budget is never closed")
    }
}

/// Reads and decodes one row group in its own runtime task.
///
/// DuckDB pulls the source stream through `block_on` while holding its Arrow
/// scan lock, so decoding while polling would run every row group on
/// whichever thread holds that lock, one at a time. Dropping the returned
/// stream aborts the task, and a panic surfaces as an error rather than a
/// truncated row group.
fn spawn_row_group(
    mut reader: ParquetRecordBatchStream<OpendalParquetReader>,
    read_ahead_bytes: usize,
) -> BatchStream {
    let budget = ReadAheadBudget::new(read_ahead_bytes);
    // Unbounded because `budget` bounds the queued bytes instead.
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        while let Some(batch) = reader.next().await {
            let item = match batch {
                Ok(batch) => {
                    let permit = budget.reserve(&batch).await;
                    Ok((batch, permit))
                }
                Err(error) => Err(Error::from(error)),
            };
            if sender.send(item).is_err() {
                break;
            }
        }
    }));
    // Dropping the permit returns the batch's bytes to the budget.
    let batches = stream::poll_fn(move |cx| receiver.poll_recv(cx))
        .map(|item| item.map(|(batch, _permit)| batch));
    let completion = stream::once(task).filter_map(|result| {
        future::ready(result.err().map(|error| {
            Err(Error::message(format!(
                "Parquet row group task failed: {error}"
            )))
        }))
    });
    Box::pin(batches.chain(completion))
}

/// Top-level columns to read. A Parquet projection mask always yields columns
/// in file order, so `order` maps the file-order output back to the requested
/// order.
///
/// For example, requesting columns `[2, 0]` of file columns `[a, b, c]` gives
/// `roots = [0, 2]`, so the reader yields `[a, c]`; `order = [1, 0]` then
/// reorders each batch to the requested `[c, a]`.
struct ColumnProjection {
    /// Requested column indices in file order, used to build the projection mask.
    roots: Vec<usize>,
    /// For each requested column, its position in the file-order output.
    order: Vec<usize>,
}

impl ColumnProjection {
    /// Returns `None` for an empty request, which reads every column.
    fn new(columns: Vec<usize>) -> Option<Self> {
        if columns.is_empty() {
            return None;
        }
        let mut roots = columns.clone();
        roots.sort_unstable();
        roots.dedup();
        let order = columns
            .iter()
            .map(|column| roots.partition_point(|root| root < column))
            .collect();
        Some(Self { roots, order })
    }
}

/// Returns the column chunk bytes that `mask` selects, or every column
/// chunk's bytes without a mask.
fn column_chunk_bytes(metadata: &ParquetMetaData, mask: Option<&ProjectionMask>) -> u64 {
    metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns().iter().enumerate())
        .filter(|(leaf, _)| mask.is_none_or(|mask| mask.leaf_included(*leaf)))
        .map(|(_, column)| column.byte_range().1)
        .fold(0, u64::saturating_add)
}

/// Opens multiple Parquet files as one Arrow batch stream, with one reader per
/// row group.
///
/// File footers are loaded concurrently, at most `max_read_parallelism` at a
/// time, but retained in `paths` order: the first file defines the returned
/// schema, and every file must match it before any batch is emitted. Each row
/// group is read and decoded in its own task, at most `max_read_parallelism`
/// at a time. When `preserve_insertion_order` is `true`, batches are emitted
/// in `paths` and row-group order while later row groups are read ahead. When
/// it is `false`, batches are emitted as soon as they are available.
///
/// Each row group may queue up to `read_ahead_bytes` of decoded batches ahead
/// of the consumer, so decoded data waiting to be consumed stays below about
/// `max_read_parallelism * read_ahead_bytes`, plus one batch per row group.
///
/// `projection` lists top-level column indices in output order; only those
/// columns are fetched and decoded. `None` or an empty list reads every
/// column.
///
/// Returns an error when `paths` is empty, any numeric option is zero, a
/// projected index is out of range, a file cannot be opened, or schemas
/// differ.
pub(crate) async fn open_parquet_paths(
    operator: opendal::Operator,
    paths: Vec<String>,
    batch_size: usize,
    preserve_insertion_order: bool,
    max_read_parallelism: usize,
    read_ahead_bytes: usize,
    projection: Option<Vec<usize>>,
    metrics: Arc<ReadMetrics>,
) -> Result<(SchemaRef, BatchStream)> {
    if batch_size == 0 {
        return Err(Error::message("batch_size must be positive"));
    }
    if max_read_parallelism == 0 {
        return Err(Error::message("max_read_parallelism must be positive"));
    }
    if read_ahead_bytes == 0 {
        return Err(Error::message("read_ahead_bytes must be positive"));
    }
    let projection = projection.and_then(ColumnProjection::new);
    let (schema, files) = load_parquet_metadata(
        operator.clone(),
        paths,
        max_read_parallelism,
        metrics.clone(),
    )
    .await?;
    // Projecting the schema rejects out-of-range indices, which would panic in
    // `ProjectionMask::roots`.
    let schema = match &projection {
        Some(projection) => Arc::new(
            schema
                .project(&projection.roots)?
                .project(&projection.order)?,
        ),
        None => schema,
    };
    let mut row_groups = Vec::new();
    let mut unread_bytes = 0_u64;
    for file in files {
        let mask = projection.as_ref().map(|projection| {
            ProjectionMask::roots(
                file.metadata.parquet_schema(),
                projection.roots.iter().copied(),
            )
        });
        unread_bytes = unread_bytes
            .saturating_add(column_chunk_bytes(file.metadata.metadata(), mask.as_ref()));
        for index in 0..file.metadata.metadata().num_row_groups() {
            row_groups.push((
                file.path.clone(),
                file.metadata.clone(),
                mask.clone(),
                index,
            ));
        }
    }
    // Every footer has been read; only the selected column chunks remain.
    metrics.set_total_bytes(metrics.snapshot().bytes_read.saturating_add(unread_bytes));
    let readers = stream::iter(row_groups).map(move |(path, metadata, mask, row_group)| {
        let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            OpendalParquetReader::new(operator.clone(), path, metrics.clone()),
            metadata,
        )
        .with_batch_size(batch_size)
        .with_row_groups(vec![row_group]);
        let builder = match mask {
            Some(mask) => builder.with_projection(mask),
            None => builder,
        };
        builder
            .build()
            .map(|reader| spawn_row_group(reader, read_ahead_bytes))
            .map_err(Error::from)
    });
    // A row group starts reading as soon as it is pulled from `readers`, so
    // both modes keep at most `max_read_parallelism` row groups in flight.
    let batches: BatchStream = if preserve_insertion_order {
        Box::pin(
            readers
                .map(future::ready)
                .buffered(max_read_parallelism)
                .try_flatten(),
        )
    } else {
        Box::pin(readers.try_flatten_unordered(Some(max_read_parallelism)))
    };
    let Some(ColumnProjection { order, .. }) = projection else {
        return Ok((schema, batches));
    };
    let batches = batches.map(move |batch| batch.and_then(|batch| Ok(batch.project(&order)?)));
    Ok((schema, Box::pin(batches)))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use arrow_array::{ArrayRef, Int32Array};

    use super::*;

    fn batch(rows: i32) -> RecordBatch {
        RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from_iter_values(0..rows)) as ArrayRef,
        )])
        .unwrap()
    }

    #[tokio::test]
    async fn read_ahead_budget_waits_until_queued_bytes_are_consumed() {
        let batch = batch(1024);
        let budget = ReadAheadBudget::new(batch.get_array_memory_size() * 3 / 2);
        let first = budget.reserve(&batch).await;
        let second = budget.reserve(&batch);
        tokio::pin!(second);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut second)
                .await
                .is_err(),
            "a second batch must wait while the first fills the budget"
        );
        drop(first);
        second.await;
    }

    #[tokio::test]
    async fn batch_larger_than_read_ahead_budget_takes_the_whole_budget() {
        let budget = ReadAheadBudget::new(1);
        let permit = budget.reserve(&batch(1024)).await;
        assert_eq!(budget.permits.available_permits(), 0);
        drop(permit);
        assert_eq!(budget.permits.available_permits(), 1);
    }
}
