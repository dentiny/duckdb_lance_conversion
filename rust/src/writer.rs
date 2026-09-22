use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, ensure, Context, Result};
use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, DataType, SchemaRef};
use datafusion_physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};
use futures::{stream, FutureExt};
use lance::{
    dataset::{write::InsertBuilder, WriteMode, WriteParams},
    Dataset,
};
use tokio::{
    fs,
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};

#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    pub overwrite: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteSummary {
    pub rows_written: u64,
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

/// One dataset write, with at most one queued input batch.
/// Drop closes the input; the writer task asynchronously cleans up aborted output.
pub struct LanceSink {
    schema: SchemaRef,
    sender: Option<Sender<Message>>,
    worker: Option<JoinHandle<Result<()>>>,
    rows_written: u64,
    committed: bool,
    failed: bool,
}

impl LanceSink {
    pub async fn create(
        destination: impl AsRef<Path>,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Self> {
        validate_schema(&schema)?;
        let destination = resolve_destination(destination.as_ref()).await?;
        let owns_destination = reserve_destination(&destination, options.overwrite).await?;
        let (sender, receiver) = channel(1);
        let worker = spawn_writer(destination, schema.clone(), receiver, owns_destination);
        Ok(Self {
            schema,
            sender: Some(sender),
            worker: Some(worker),
            rows_written: 0,
            committed: false,
            failed: false,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        ensure!(
            !self.committed && !self.failed,
            "writer is already finished or failed"
        );
        if batch.schema().as_ref() != self.schema.as_ref() {
            self.abort().await;
            return Err(anyhow!("batch schema does not match the conversion schema"));
        }
        let rows = batch.num_rows() as u64;
        if self
            .sender
            .as_ref()
            .context("writer is closed")?
            .send(Message::Batch(batch))
            .await
            .is_err()
        {
            self.failed = true;
            self.sender.take();
            self.join_worker().await?;
            return Err(anyhow!("Lance writer stopped before accepting a batch"));
        }
        self.rows_written += rows;
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<WriteSummary> {
        ensure!(
            !self.committed && !self.failed,
            "writer is already finished or failed"
        );
        let sender = self.sender.take().context("writer is closed")?;
        let sent = sender.send(Message::Finish).await.is_ok();
        drop(sender);
        if let Err(error) = self.join_worker().await {
            self.failed = true;
            return Err(error);
        }
        ensure!(sent, "Lance writer stopped before finish");
        self.committed = true;
        Ok(WriteSummary {
            rows_written: self.rows_written,
        })
    }

    pub(crate) async fn abort(&mut self) {
        self.failed = true;
        self.sender.take();
        let _ = self.join_worker().await;
    }

    async fn join_worker(&mut self) -> Result<()> {
        if let Some(worker) = self.worker.as_mut() {
            let result = worker.await;
            self.worker.take();
            result.context("Lance writer task failed")??;
        }
        Ok(())
    }
}

fn validate_schema(schema: &SchemaRef) -> Result<()> {
    ensure!(
        !schema.fields().is_empty(),
        "Lance requires at least one column"
    );
    for field in schema.fields() {
        validate_type(field.data_type())?;
    }
    Ok(())
}

async fn resolve_destination(destination: &Path) -> Result<PathBuf> {
    let path_text = destination
        .to_str()
        .context("destination must be valid UTF-8")?;
    ensure!(
        !path_text.contains("://"),
        "only local Lance destinations are supported"
    );
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = fs::canonicalize(parent)
        .await
        .context("destination parent directory must exist")?;
    let name = destination
        .file_name()
        .context("destination must name a new dataset directory")?;
    Ok(parent.join(name))
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

fn spawn_writer(
    destination: PathBuf,
    schema: SchemaRef,
    receiver: Receiver<Message>,
    owns_destination: bool,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let stream = batch_stream(schema, receiver);
        let result = AssertUnwindSafe(write_dataset(&destination, stream, owns_destination))
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(anyhow!("Lance writer task panicked")));
        if result.is_err() && owns_destination {
            let _ = fs::remove_dir_all(&destination).await;
        }
        result
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

fn validate_type(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal128(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _) => Ok(()),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            validate_type(field.data_type())
        }
        DataType::Struct(fields) => {
            for field in fields {
                validate_type(field.data_type())?;
            }
            Ok(())
        }
        _ => Err(anyhow!(
            "unsupported Arrow type {data_type}; cast it explicitly"
        )),
    }
}
