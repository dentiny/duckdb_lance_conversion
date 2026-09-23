use std::num::NonZeroUsize;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use arrow_array::{new_null_array, ArrayRef, RecordBatch, StructArray};
use arrow_cast::cast;
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use datafusion_physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};
use futures::{stream, FutureExt, Stream, StreamExt, TryStreamExt};
pub use lance::dataset::write::WriteMode;
use lance::{
    blob_field_with_options,
    dataset::write::{ExternalBlobMode, InsertBuilder, WriteParams},
    session::Session,
    BlobFieldOptions,
};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};

use super::lance_index::LanceIndexPlan;
use super::{BatchSink, WriteSummary};
use crate::error::ResultExt;
use crate::schema::validate_schema;
use crate::storage::{OpendalStorage, OpendalStoreProvider};
use crate::{Error, Result, S3StorageConfig};

const DEFAULT_BLOB_INLINE_SIZE_THRESHOLD: usize = 2 * 1024 * 1024;
const DEFAULT_BLOB_DEDICATED_SIZE_THRESHOLD: usize = 16 * 1024 * 1024;
const DEFAULT_TARGET_FILE_SIZE: usize = 512 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct WriteOptions {
    pub mode: WriteMode,
    pub s3_config: Option<S3StorageConfig>,
    pub blob_inline_size_threshold: Option<usize>,
    pub blob_dedicated_size_threshold: Option<usize>,
    pub target_file_size: usize,
    pub blob_columns: Vec<String>,
    pub scalar_index_columns: Vec<String>,
    pub vector_index_columns: Vec<String>,
    pub text_index_columns: Vec<String>,
    pub bloom_filter_index_columns: Vec<String>,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            mode: WriteMode::Create,
            s3_config: None,
            blob_inline_size_threshold: Some(DEFAULT_BLOB_INLINE_SIZE_THRESHOLD),
            blob_dedicated_size_threshold: Some(DEFAULT_BLOB_DEDICATED_SIZE_THRESHOLD),
            target_file_size: DEFAULT_TARGET_FILE_SIZE,
            blob_columns: Vec::new(),
            scalar_index_columns: Vec::new(),
            vector_index_columns: Vec::new(),
            text_index_columns: Vec::new(),
            bloom_filter_index_columns: Vec::new(),
        }
    }
}

pub struct LanceSink {
    destination: PathBuf,
    options: WriteOptions,
}

impl LanceSink {
    pub fn new(destination: impl Into<PathBuf>, options: WriteOptions) -> Self {
        Self {
            destination: destination.into(),
            options,
        }
    }
}

impl BatchSink for LanceSink {
    async fn write<S>(self, schema: SchemaRef, batches: S) -> Result<WriteSummary>
    where
        S: Stream<Item = Result<RecordBatch>> + Send + 'static,
    {
        validate_schema(&schema)?;
        let expected_schema = schema.clone();
        let batches = batches.and_then(move |batch| {
            let result = if batch.schema().as_ref() == expected_schema.as_ref() {
                Ok(batch)
            } else {
                Err(Error::message(
                    "batch schema does not match the conversion schema",
                ))
            };
            futures::future::ready(result)
        });
        let batches = batches.map_err(|error| ArrowError::ExternalError(error.into()).into());
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, batches));
        let destination_path = self
            .destination
            .to_str()
            .ok_or_else(|| Error::message("destination must be valid UTF-8"))?;
        let destination =
            OpendalStorage::from_path(destination_path, self.options.s3_config.as_ref())?;
        write_stream(&destination, stream, self.options).await
    }
}

enum Message {
    Batch(RecordBatch),
    Finish,
}

fn batch_stream(schema: SchemaRef, receiver: Receiver<Message>) -> SendableRecordBatchStream {
    let stream = stream::unfold(Some(receiver), |receiver| async move {
        let mut receiver = receiver?;
        match receiver.recv().await {
            Some(Message::Batch(batch)) => Some((Ok(batch), Some(receiver))),
            Some(Message::Finish) => None,
            None => Some((
                Err(ArrowError::ComputeError("conversion aborted before finish".into()).into()),
                None,
            )),
        }
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

enum SinkState {
    Open {
        sender: Sender<Message>,
        worker: JoinHandle<Result<WriteSummary>>,
    },
    Closing(JoinHandle<Result<WriteSummary>>),
    Finished,
    Failed,
}

/// Push-based adapter for callers such as DuckDB, with at most one queued batch.
/// Dropping the sender lets the writer task stop an unfinished write.
pub struct LanceWriter {
    schema: SchemaRef,
    state: SinkState,
}

impl LanceWriter {
    pub async fn create(
        destination: impl AsRef<Path>,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Self> {
        validate_schema(&schema)?;
        let destination_path = destination
            .as_ref()
            .to_str()
            .ok_or_else(|| Error::message("destination must be valid UTF-8"))?;
        let destination = OpendalStorage::from_path(destination_path, options.s3_config.as_ref())?;
        let (sender, receiver) = channel(1);
        let stream = batch_stream(schema.clone(), receiver);
        let worker = tokio::spawn(async move { write_stream(&destination, stream, options).await });
        Ok(Self {
            schema,
            state: SinkState::Open { sender, worker },
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let SinkState::Open { sender, .. } = &self.state else {
            return Err(Error::message("writer is already finished or failed"));
        };
        if batch.schema().as_ref() != self.schema.as_ref() {
            self.abort().await;
            return Err(Error::message(
                "batch schema does not match the conversion schema",
            ));
        }
        if sender.send(Message::Batch(batch)).await.is_err() {
            self.close_input();
            self.join_worker().await?;
            return Err(Error::message(
                "Lance writer stopped before accepting a batch",
            ));
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<WriteSummary> {
        let SinkState::Open { sender, .. } = &self.state else {
            return Err(Error::message("writer is already finished or failed"));
        };
        let sent = sender.send(Message::Finish).await.is_ok();
        self.close_input();
        let summary = self.join_worker().await?;
        if !sent {
            return Err(Error::message("Lance writer stopped before finish"));
        }
        self.state = SinkState::Finished;
        Ok(summary)
    }

    pub(crate) async fn abort(&mut self) {
        self.close_input();
        if matches!(self.state, SinkState::Closing(_)) {
            let _ = self.join_worker().await;
        }
    }

    fn close_input(&mut self) {
        let state = std::mem::replace(&mut self.state, SinkState::Failed);
        self.state = match state {
            SinkState::Open { worker, .. } => SinkState::Closing(worker),
            state => state,
        };
    }

    async fn join_worker(&mut self) -> Result<WriteSummary> {
        let SinkState::Closing(worker) = &mut self.state else {
            return Err(Error::message("writer is closed"));
        };
        let result = worker.await;
        self.state = SinkState::Failed;
        result.context("Lance writer task failed")?
    }
}

fn transform_blob_batch(
    batch: RecordBatch,
    schema: SchemaRef,
    binary_blob_columns: &[usize],
    uri_blob_columns: &[usize],
) -> std::result::Result<RecordBatch, ArrowError> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, column) in batch.columns().iter().enumerate() {
        let is_binary = binary_blob_columns.binary_search(&index).is_ok();
        let is_uri = uri_blob_columns.binary_search(&index).is_ok();
        if !is_binary && !is_uri {
            columns.push(column.clone());
            continue;
        }
        let field = schema.field(index);
        let DataType::Struct(children) = field.data_type() else {
            return Err(ArrowError::SchemaError(format!(
                "blob field '{}' is not a struct",
                field.name()
            )));
        };
        let (data, uri) = if is_uri {
            (
                new_null_array(&DataType::LargeBinary, column.len()),
                cast(column, &DataType::Utf8)?,
            )
        } else {
            (
                cast(column, &DataType::LargeBinary)?,
                new_null_array(&DataType::Utf8, column.len()),
            )
        };
        columns.push(Arc::new(StructArray::try_new(
            children.clone(),
            vec![data, uri],
            column.nulls().cloned(),
        )?) as ArrayRef);
    }
    RecordBatch::try_new(schema, columns)
}

fn configure_blob_storage(
    stream: SendableRecordBatchStream,
    options: &WriteOptions,
) -> Result<SendableRecordBatchStream> {
    if options.blob_inline_size_threshold.is_none()
        && options.blob_dedicated_size_threshold.is_none()
        && options.blob_columns.is_empty()
    {
        return Ok(stream);
    }

    let mut blob_options = BlobFieldOptions::default();
    if let Some(threshold) = options.blob_inline_size_threshold {
        blob_options = blob_options.with_inline_size_threshold(threshold);
    }
    if let Some(threshold) = options.blob_dedicated_size_threshold {
        let threshold = NonZeroUsize::new(threshold).ok_or_else(|| {
            Error::invalid_argument("blob dedicated size threshold must be greater than zero")
        })?;
        blob_options = blob_options.with_dedicated_size_threshold(threshold);
    }

    let input_schema = stream.schema();
    let binary_blob_columns = input_schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            matches!(
                field.data_type(),
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            )
            .then_some(index)
        })
        .collect::<Vec<_>>();
    let mut uri_blob_columns = options
        .blob_columns
        .iter()
        .map(|name| {
            let index = input_schema.index_of(name).map_err(|_| {
                Error::invalid_argument(format!("BLOB_COLUMNS column not found: {name}"))
            })?;
            ensure_uri_column(input_schema.field(index).data_type(), name)?;
            Ok(index)
        })
        .collect::<Result<Vec<_>>>()?;
    uri_blob_columns.sort_unstable();
    if uri_blob_columns
        .windows(2)
        .any(|indices| indices[0] == indices[1])
    {
        return Err(Error::invalid_argument(
            "BLOB_COLUMNS cannot contain duplicate columns",
        ));
    }
    let mut blob_columns = binary_blob_columns.clone();
    blob_columns.extend_from_slice(&uri_blob_columns);
    blob_columns.sort_unstable();
    if blob_columns.is_empty() {
        return Ok(stream);
    }

    let fields = input_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if blob_columns.binary_search(&index).is_ok() {
                Arc::new(blob_field_with_options(
                    field.name(),
                    field.is_nullable(),
                    blob_options.clone(),
                ))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    let output_schema = Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ));
    let batch_schema = output_schema.clone();
    let batches = stream.map(move |batch| {
        let batch = batch?;
        Ok(transform_blob_batch(
            batch,
            batch_schema.clone(),
            &binary_blob_columns,
            &uri_blob_columns,
        )?)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        batches,
    )))
}

fn ensure_uri_column(data_type: &DataType, name: &str) -> Result<()> {
    if matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        Ok(())
    } else {
        Err(Error::invalid_argument(format!(
            "BLOB_COLUMNS column must be a string: {name}"
        )))
    }
}

async fn write_stream(
    destination: &OpendalStorage,
    stream: SendableRecordBatchStream,
    options: WriteOptions,
) -> Result<WriteSummary> {
    let stream = configure_blob_storage(stream, &options)?;
    let rows_written = Arc::new(AtomicU64::new(0));
    let count = rows_written.clone();
    let schema = stream.schema();
    let counted = stream.inspect_ok(move |batch| {
        count.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
    });
    let stream = Box::pin(RecordBatchStreamAdapter::new(schema, counted));
    let result = AssertUnwindSafe(write_dataset(destination, stream, &options))
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(Error::message("Lance writer task panicked")));
    result?;
    Ok(WriteSummary {
        rows_written: rows_written.load(Ordering::Relaxed),
    })
}

async fn write_dataset(
    destination: &OpendalStorage,
    stream: SendableRecordBatchStream,
    options: &WriteOptions,
) -> Result<()> {
    if destination.object_path.as_ref().is_empty() {
        return Err(Error::message(
            "Lance destination must not be a storage root",
        ));
    }
    let indexes = LanceIndexPlan::new(&stream.schema(), options)?;
    let mut params = WriteParams {
        mode: options.mode,
        max_bytes_per_file: options.target_file_size,
        external_blob_mode: if options.blob_columns.is_empty() {
            ExternalBlobMode::Reference
        } else {
            ExternalBlobMode::Ingest
        },
        ..Default::default()
    };
    let session = Session::default();
    session.store_registry().insert(
        destination.location.scheme(),
        Arc::new(OpendalStoreProvider::new(destination.object_store.clone())),
    );
    params.session = Some(Arc::new(session));
    let uri = destination.location.as_str();
    let mut dataset = InsertBuilder::new(uri)
        .with_params(&params)
        .execute_stream(stream)
        .await
        .context(match options.mode {
            WriteMode::Create => "creating Lance dataset",
            WriteMode::Append => "appending to Lance dataset",
            WriteMode::Overwrite => "overwriting Lance dataset",
        })?;
    indexes.create(&mut dataset).await?;
    Ok(())
}
