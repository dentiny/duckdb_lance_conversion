//! Reads `blob_columns` objects before Lance ingests them, so an unreadable
//! object can become NULL or drop its row instead of failing the write.

use std::sync::Arc;

use arrow_array::{
    builder::{LargeBinaryBuilder, StringBuilder},
    cast::AsArray,
    Array, ArrayRef, BooleanArray, RecordBatch, StringArray, StructArray,
};
use arrow_cast::cast;
use arrow_schema::{ArrowError, DataType, SchemaRef};
use arrow_select::filter::filter_record_batch;
use bytes::Bytes;
use datafusion_physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};
use futures::{future::join_all, stream, FutureExt, StreamExt, TryStreamExt};
use lance::deps::datafusion::error::DataFusionError;
use lance_io::object_store::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};

use super::BlobOnError;

/// Objects read concurrently by one fragment writer.
const FETCH_CONCURRENCY: usize = 64;
/// Fetched bytes after which the rows read so far are emitted as one batch.
const BATCH_BYTES: usize = 64 * 1024 * 1024;

enum Fetched {
    Null,
    Data(Bytes),
    /// Too large to buffer; Lance streams it from the URI.
    Uri,
    Failed,
}

impl Fetched {
    fn len(&self) -> usize {
        match self {
            Self::Data(data) => data.len(),
            _ => 0,
        }
    }
}

/// An input batch with its URI blob columns cast to `Utf8`.
struct InputBatch {
    batch: RecordBatch,
    uris: Vec<StringArray>,
}

struct FetchedRow {
    input: Arc<InputBatch>,
    row: usize,
    /// One value per URI blob column.
    values: Vec<Fetched>,
}

pub(super) struct BlobFetcher {
    pub(super) registry: Arc<ObjectStoreRegistry>,
    pub(super) on_error: BlobOnError,
    pub(super) dedicated_threshold: usize,
}

impl BlobFetcher {
    async fn fetch(&self, uri: &str) -> lance::Result<Fetched> {
        let (store, path) = ObjectStore::from_uri_and_params(
            self.registry.clone(),
            uri,
            &ObjectStoreParams::default(),
        )
        .await?;
        let reader = store.open(&path).await?;
        let size = reader.size().await?;
        if size > self.dedicated_threshold {
            return Ok(Fetched::Uri);
        }
        if size == 0 {
            return Ok(Fetched::Data(Bytes::new()));
        }
        Ok(Fetched::Data(reader.get_range(0..size).await?))
    }

    async fn fetch_row(self: Arc<Self>, input: Arc<InputBatch>, row: usize) -> FetchedRow {
        let values = join_all(input.uris.iter().map(|uris| async {
            if uris.is_null(row) {
                Fetched::Null
            } else {
                self.fetch(uris.value(row)).await.unwrap_or(Fetched::Failed)
            }
        }))
        .await;
        FetchedRow { input, row, values }
    }

    /// Replaces the URI blob columns of `stream` with fetched data, applying
    /// `transform` to each emitted slice of input rows first.
    pub(super) fn fetch_stream(
        self: Arc<Self>,
        stream: SendableRecordBatchStream,
        schema: SchemaRef,
        uri_columns: Vec<usize>,
        transform: impl Fn(RecordBatch) -> Result<RecordBatch, ArrowError> + Send + Sync + 'static,
    ) -> SendableRecordBatchStream {
        let on_error = self.on_error;
        let uri_columns = Arc::new(uri_columns);
        let columns = uri_columns.clone();
        let rows = stream
            .map(move |batch| -> Result<_, DataFusionError> {
                let batch = batch?;
                let uris = columns
                    .iter()
                    .map(|&index| {
                        Ok(cast(batch.column(index), &DataType::Utf8)?
                            .as_string()
                            .clone())
                    })
                    .collect::<Result<Vec<_>, ArrowError>>()?;
                let input = Arc::new(InputBatch { batch, uris });
                Ok(stream::iter((0..input.batch.num_rows()).map(move |row| {
                    Ok::<_, DataFusionError>((input.clone(), row))
                })))
            })
            .try_flatten()
            .map_ok(move |(input, row)| {
                self.clone()
                    .fetch_row(input, row)
                    .map(Ok::<_, DataFusionError>)
            })
            .try_buffered(FETCH_CONCURRENCY);
        let transform = Arc::new(transform);
        let output_schema = schema.clone();
        let batches = stream::try_unfold(Box::pin(rows.peekable()), move |mut rows| {
            let schema = output_schema.clone();
            let uri_columns = uri_columns.clone();
            let transform = transform.clone();
            async move {
                let Some(first) = rows.next().await.transpose()? else {
                    return Ok::<_, DataFusionError>(None);
                };
                let input = first.input.clone();
                let mut bytes = row_bytes(&first);
                let mut slice = vec![first];
                while bytes < BATCH_BYTES {
                    let Some(next) = rows
                        .as_mut()
                        .next_if(
                            |next| matches!(next, Ok(next) if Arc::ptr_eq(&next.input, &input)),
                        )
                        .await
                    else {
                        break;
                    };
                    let next = next?;
                    bytes += row_bytes(&next);
                    slice.push(next);
                }
                let batch =
                    build_batch(&slice, &schema, &uri_columns, on_error, transform.as_ref())?;
                Ok(Some((batch, rows)))
            }
        })
        .try_filter(|batch| futures::future::ready(batch.num_rows() > 0));
        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }
}

fn row_bytes(row: &FetchedRow) -> usize {
    row.values.iter().map(Fetched::len).sum()
}

/// Builds the output batch for consecutive rows of one input batch.
fn build_batch(
    slice: &[FetchedRow],
    schema: &SchemaRef,
    uri_columns: &[usize],
    on_error: BlobOnError,
    transform: &impl Fn(RecordBatch) -> Result<RecordBatch, ArrowError>,
) -> Result<RecordBatch, ArrowError> {
    let input = &slice[0].input;
    let start = slice[0].row;
    let mut columns = transform(input.batch.slice(start, slice.len()))?
        .columns()
        .to_vec();
    let mut keep = vec![true; slice.len()];
    for (position, &index) in uri_columns.iter().enumerate() {
        let DataType::Struct(children) = schema.field(index).data_type() else {
            return Err(ArrowError::SchemaError(format!(
                "blob field '{}' is not a struct",
                schema.field(index).name()
            )));
        };
        let uris = &input.uris[position];
        let mut data = LargeBinaryBuilder::new();
        let mut uri = StringBuilder::new();
        let mut valid = Vec::with_capacity(slice.len());
        for (offset, row) in slice.iter().enumerate() {
            match &row.values[position] {
                Fetched::Data(bytes) => {
                    data.append_value(bytes);
                    uri.append_null();
                    valid.push(true);
                }
                Fetched::Uri => {
                    data.append_null();
                    uri.append_value(uris.value(row.row));
                    valid.push(true);
                }
                fetched => {
                    if matches!(fetched, Fetched::Failed) && on_error == BlobOnError::Skip {
                        keep[offset] = false;
                    }
                    data.append_null();
                    uri.append_null();
                    valid.push(false);
                }
            }
        }
        columns[index] = Arc::new(StructArray::try_new(
            children.clone(),
            vec![
                Arc::new(data.finish()) as ArrayRef,
                Arc::new(uri.finish()) as ArrayRef,
            ],
            Some(BooleanArray::from(valid).values().clone().into()),
        )?);
    }
    let batch = RecordBatch::try_new(schema.clone(), columns)?;
    if keep.iter().all(|&keep| keep) {
        return Ok(batch);
    }
    filter_record_batch(&batch, &BooleanArray::from(keep))
}
