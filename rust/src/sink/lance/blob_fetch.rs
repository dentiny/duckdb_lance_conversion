//! Reads `blob_columns` objects before Lance ingests them, so a permanently
//! unreadable object can become NULL or drop its row instead of failing the
//! write. Transient errors, such as rate limits or timeouts, still fail it.

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
use object_store::path::Path;

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
    /// Permanently unreadable, with the reason.
    Failed(String),
}

impl Fetched {
    fn len(&self) -> usize {
        match self {
            Self::Data(data) => data.len(),
            _ => 0,
        }
    }
}

/// Whether `error` means the object is missing or forbidden, so retrying
/// cannot succeed. Anything unrecognized is treated as transient.
fn is_permanent(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<opendal::Error>() {
            return matches!(
                error.kind(),
                opendal::ErrorKind::NotFound | opendal::ErrorKind::PermissionDenied
            );
        }
        let permanent = matches!(
            error.downcast_ref::<object_store::Error>(),
            Some(
                object_store::Error::NotFound { .. } | object_store::Error::PermissionDenied { .. }
            )
        ) || matches!(
            error.downcast_ref::<lance::Error>(),
            Some(lance::Error::NotFound { .. })
        ) || error.downcast_ref::<std::io::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            )
        });
        if permanent {
            return true;
        }
        current = error.source();
    }
    false
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
    async fn fetch(&self, uri: &str) -> Result<Fetched, DataFusionError> {
        // Resolving a URI performs no IO, so its errors are permanent.
        let (store, path) = match ObjectStore::from_uri_and_params(
            self.registry.clone(),
            uri,
            &ObjectStoreParams::default(),
        )
        .await
        {
            Ok(resolved) => resolved,
            Err(error) => return Ok(Fetched::Failed(error.to_string())),
        };
        match self.read(&store, &path).await {
            Ok(fetched) => Ok(fetched),
            Err(error) if is_permanent(&error) => Ok(Fetched::Failed(error.to_string())),
            Err(error) => Err(DataFusionError::Execution(format!(
                "reading blob {uri}: {error}"
            ))),
        }
    }

    async fn read(&self, store: &ObjectStore, path: &Path) -> lance::Result<Fetched> {
        let reader = store.open(path).await?;
        let size = reader.size().await?;
        if size > self.dedicated_threshold {
            return Ok(Fetched::Uri);
        }
        if size == 0 {
            return Ok(Fetched::Data(Bytes::new()));
        }
        Ok(Fetched::Data(reader.get_range(0..size).await?))
    }

    async fn fetch_row(
        self: Arc<Self>,
        input: Arc<InputBatch>,
        row: usize,
    ) -> Result<FetchedRow, DataFusionError> {
        let values = join_all(input.uris.iter().map(|uris| async {
            if uris.is_null(row) {
                Ok(Fetched::Null)
            } else {
                self.fetch(uris.value(row)).await
            }
        }))
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;
        Ok(FetchedRow { input, row, values })
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
        let uri_columns = Arc::new(uri_columns);
        let columns = uri_columns.clone();
        let fetcher = self.clone();
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
            .map_ok(move |(input, row)| fetcher.clone().fetch_row(input, row))
            .try_buffered(FETCH_CONCURRENCY);
        let transform = Arc::new(transform);
        let output_schema = schema.clone();
        let batches = stream::try_unfold(Box::pin(rows.peekable()), move |mut rows| {
            let fetcher = self.clone();
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
                    fetcher.build_batch(&slice, &schema, &uri_columns, transform.as_ref())?;
                Ok(Some((batch, rows)))
            }
            .boxed()
        })
        .try_filter(|batch| futures::future::ready(batch.num_rows() > 0));
        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }

    /// Builds the output batch for consecutive rows of one input batch.
    fn build_batch(
        &self,
        slice: &[FetchedRow],
        schema: &SchemaRef,
        uri_columns: &[usize],
        transform: &impl Fn(RecordBatch) -> Result<RecordBatch, ArrowError>,
    ) -> Result<RecordBatch, ArrowError> {
        let input = &slice[0].input;
        let rows = input.batch.slice(slice[0].row, slice.len());
        let mut columns = transform(rows)?.columns().to_vec();
        let mut keep = vec![true; slice.len()];
        for (position, &index) in uri_columns.iter().enumerate() {
            let field = schema.field(index);
            let DataType::Struct(children) = field.data_type() else {
                return Err(ArrowError::SchemaError(format!(
                    "blob field '{}' is not a struct",
                    field.name()
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
                    }
                    Fetched::Uri => {
                        data.append_null();
                        uri.append_value(uris.value(row.row));
                    }
                    Fetched::Null => {
                        data.append_null();
                        uri.append_null();
                    }
                    Fetched::Failed(error) => {
                        data.append_null();
                        uri.append_null();
                        let action = if self.on_error == BlobOnError::Skip {
                            keep[offset] = false;
                            "skipped the row"
                        } else {
                            "stored NULL"
                        };
                        eprintln!(
                            "WARNING: FORMAT LANCE {action} for unreadable BLOB_COLUMNS value '{}' in column '{}': {error}",
                            uris.value(row.row),
                            field.name()
                        );
                    }
                }
                valid.push(matches!(
                    row.values[position],
                    Fetched::Data(_) | Fetched::Uri
                ));
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
}

fn row_bytes(row: &FetchedRow) -> usize {
    row.values.iter().map(Fetched::len).sum()
}
