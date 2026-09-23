use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use anyhow::{anyhow, ensure, Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use datafusion_physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};
use futures::{stream, FutureExt, Stream, TryStreamExt};
use lance::{
    dataset::{write::InsertBuilder, WriteMode, WriteParams},
    io::ObjectStoreParams,
};
use lance_table::io::commit::ConditionalPutCommitHandler;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};

use super::{BatchSink, WriteSummary};
use crate::schema::validate_schema;
use crate::storage::OpendalStorage;
use crate::S3StorageConfig;

#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    pub overwrite: bool,
    pub s3_config: Option<S3StorageConfig>,
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
                Err(anyhow!("batch schema does not match the conversion schema"))
            };
            futures::future::ready(result)
        });
        let batches = batches.map_err(|error| ArrowError::ExternalError(error.into()).into());
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, batches));
        let destination_path = self
            .destination
            .to_str()
            .context("destination must be valid UTF-8")?;
        let destination =
            OpendalStorage::from_path(destination_path, self.options.s3_config.as_ref())?;
        write_stream(&destination, stream, !self.options.overwrite).await
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
            .context("destination must be valid UTF-8")?;
        let destination = OpendalStorage::from_path(destination_path, options.s3_config.as_ref())?;
        let create_new_dataset = !options.overwrite;
        let (sender, receiver) = channel(1);
        let stream = batch_stream(schema.clone(), receiver);
        let worker =
            tokio::spawn(
                async move { write_stream(&destination, stream, create_new_dataset).await },
            );
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
            return Err(anyhow!("writer is already finished or failed"));
        };
        if batch.schema().as_ref() != self.schema.as_ref() {
            self.abort().await;
            return Err(anyhow!("batch schema does not match the conversion schema"));
        }
        if sender.send(Message::Batch(batch)).await.is_err() {
            self.close_input();
            self.join_worker().await?;
            return Err(anyhow!("Lance writer stopped before accepting a batch"));
        }
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<WriteSummary> {
        let SinkState::Open { sender, .. } = &self.state else {
            return Err(anyhow!("writer is already finished or failed"));
        };
        let sent = sender.send(Message::Finish).await.is_ok();
        self.close_input();
        let summary = self.join_worker().await?;
        ensure!(sent, "Lance writer stopped before finish");
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
            return Err(anyhow!("writer is closed"));
        };
        let result = worker.await;
        self.state = SinkState::Failed;
        result.context("Lance writer task failed")?
    }
}

async fn write_stream(
    destination: &OpendalStorage,
    stream: SendableRecordBatchStream,
    create_new_dataset: bool,
) -> Result<WriteSummary> {
    let rows_written = Arc::new(AtomicU64::new(0));
    let count = rows_written.clone();
    let schema = stream.schema();
    let counted = stream.inspect_ok(move |batch| {
        count.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
    });
    let stream = Box::pin(RecordBatchStreamAdapter::new(schema, counted));
    let result = AssertUnwindSafe(write_dataset(destination, stream, create_new_dataset))
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(anyhow!("Lance writer task panicked")));
    result?;
    Ok(WriteSummary {
        rows_written: rows_written.load(Ordering::Relaxed),
    })
}

async fn write_dataset(
    destination: &OpendalStorage,
    stream: SendableRecordBatchStream,
    create_new_dataset: bool,
) -> Result<()> {
    ensure!(
        !destination.object_path.as_ref().is_empty(),
        "Lance destination must not be a storage root"
    );
    let mut params = WriteParams {
        mode: if create_new_dataset {
            WriteMode::Create
        } else {
            WriteMode::Overwrite
        },
        ..Default::default()
    };
    #[allow(deprecated)]
    let store_params = ObjectStoreParams {
        object_store: Some((
            destination.object_store.clone(),
            destination.location.clone(),
        )),
        ..Default::default()
    };
    params.store_params = Some(store_params);
    params.commit_handler = Some(Arc::new(ConditionalPutCommitHandler));
    let uri = destination.location.as_str();
    InsertBuilder::new(uri)
        .with_params(&params)
        .execute_stream(stream)
        .await
        .context(if create_new_dataset {
            "creating Lance dataset"
        } else {
            "OVERWRITE requires an existing valid Lance dataset"
        })?;
    Ok(())
}
