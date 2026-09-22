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
    Dataset,
};
use tokio::{
    fs,
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};

use super::{BatchSink, WriteSummary};
use crate::schema::validate_schema;

#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    pub overwrite: bool,
}

/// Local Lance output configuration for a native Arrow stream.
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
        let owns_destination =
            reserve_destination(&self.destination, self.options.overwrite).await?;
        write_reserved_stream(&self.destination, stream, owns_destination).await
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
/// Dropping the sender lets the writer task clean up an unfinished dataset.
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
        let destination = destination.as_ref().to_path_buf();
        let owns_destination = reserve_destination(&destination, options.overwrite).await?;
        let (sender, receiver) = channel(1);
        let stream = batch_stream(schema.clone(), receiver);
        let worker = tokio::spawn(async move {
            write_reserved_stream(&destination, stream, owns_destination).await
        });
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

async fn reserve_destination(destination: &Path, overwrite: bool) -> Result<bool> {
    // Reserve new paths atomically; existing datasets remain owned by their caller.
    match fs::create_dir(destination).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && overwrite => {
            ensure!(
                fs::symlink_metadata(destination)
                    .await?
                    .file_type()
                    .is_dir(),
                "overwrite requires an existing Lance dataset directory"
            );
            Ok(false)
        }
        Err(error) => Err(error).with_context(|| {
            format!(
                "creating destination {} (must not exist unless OVERWRITE is enabled)",
                destination.display()
            )
        }),
    }
}

async fn write_reserved_stream(
    destination: &Path,
    stream: SendableRecordBatchStream,
    owns_destination: bool,
) -> Result<WriteSummary> {
    let rows_written = Arc::new(AtomicU64::new(0));
    let count = rows_written.clone();
    let schema = stream.schema();
    let counted = stream.inspect_ok(move |batch| {
        count.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
    });
    let stream = Box::pin(RecordBatchStreamAdapter::new(schema, counted));
    let result = AssertUnwindSafe(write_dataset(destination, stream, owns_destination))
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(anyhow!("Lance writer task panicked")));
    if result.is_err() && owns_destination {
        let _ = fs::remove_dir_all(destination).await;
    }
    result?;
    Ok(WriteSummary {
        rows_written: rows_written.load(Ordering::Relaxed),
    })
}

async fn write_dataset(
    destination: &Path,
    stream: SendableRecordBatchStream,
    owns_destination: bool,
) -> Result<()> {
    let params = WriteParams {
        mode: if owns_destination {
            WriteMode::Create
        } else {
            WriteMode::Overwrite
        },
        ..Default::default()
    };
    let uri = destination
        .to_str()
        .context("destination must be valid UTF-8")?;
    if owns_destination {
        InsertBuilder::new(uri)
            .with_params(&params)
            .execute_stream(stream)
            .await?;
    } else {
        let existing = Dataset::open(uri)
            .await
            .context("OVERWRITE requires an existing valid Lance dataset")?;
        InsertBuilder::new(Arc::new(existing))
            .with_params(&params)
            .execute_stream(stream)
            .await?;
    }
    Ok(())
}
