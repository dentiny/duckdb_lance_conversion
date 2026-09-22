use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use anyhow::{anyhow, ensure, Context, Result};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, SchemaRef};
use lance::{
    dataset::{WriteMode, WriteParams},
    Dataset,
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

struct ChannelReader {
    schema: SchemaRef,
    receiver: Receiver<Message>,
    finished: bool,
}

impl Iterator for ChannelReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match self.receiver.recv() {
            Ok(Message::Batch(batch)) => Some(Ok(batch)),
            Ok(Message::Finish) => {
                self.finished = true;
                None
            }
            Err(_) => {
                self.finished = true;
                Some(Err(ArrowError::ComputeError(
                    "conversion aborted before finish".into(),
                )))
            }
        }
    }
}

impl RecordBatchReader for ChannelReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// One dataset write. Drop aborts; cleanup only removes a directory this write created.
/// The rendezvous channel provides backpressure without buffering input batches.
pub struct LanceSink {
    schema: SchemaRef,
    destination: PathBuf,
    owns_destination: bool,
    sender: Option<SyncSender<Message>>,
    worker: Option<JoinHandle<Result<()>>>,
    rows_written: u64,
    committed: bool,
    failed: bool,
}

impl LanceSink {
    pub fn create(
        destination: impl AsRef<Path>,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Self> {
        validate_schema(&schema)?;
        let destination = resolve_destination(destination.as_ref())?;
        let owns_destination = reserve_destination(&destination, options.overwrite)?;
        let (sender, receiver) = sync_channel(0);
        let mut sink = Self {
            schema: schema.clone(),
            destination: destination.clone(),
            owns_destination,
            sender: Some(sender),
            worker: None,
            rows_written: 0,
            committed: false,
            failed: false,
        };
        sink.worker = Some(spawn_writer(
            destination,
            schema,
            receiver,
            owns_destination,
        )?);
        Ok(sink)
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        ensure!(
            !self.committed && !self.failed,
            "writer is already finished or failed"
        );
        if batch.schema().as_ref() != self.schema.as_ref() {
            self.failed = true;
            self.sender.take();
            return Err(anyhow!("batch schema does not match the conversion schema"));
        }
        let rows = batch.num_rows() as u64;
        if self
            .sender
            .as_ref()
            .context("writer is closed")?
            .send(Message::Batch(batch))
            .is_err()
        {
            self.failed = true;
            self.sender.take();
            self.join_worker()?;
            return Err(anyhow!("Lance writer stopped before accepting a batch"));
        }
        self.rows_written += rows;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<WriteSummary> {
        ensure!(
            !self.committed && !self.failed,
            "writer is already finished or failed"
        );
        let sender = self.sender.take().context("writer is closed")?;
        let sent = sender.send(Message::Finish).is_ok();
        drop(sender);
        if let Err(error) = self.join_worker() {
            self.failed = true;
            return Err(error);
        }
        ensure!(sent, "Lance writer stopped before finish");
        self.committed = true;
        Ok(WriteSummary {
            rows_written: self.rows_written,
        })
    }

    fn join_worker(&mut self) -> Result<()> {
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| anyhow!("Lance writer thread panicked"))??;
        }
        Ok(())
    }
}

impl Drop for LanceSink {
    fn drop(&mut self) {
        self.sender.take();
        let _ = self.join_worker();
        if !self.committed && self.owns_destination {
            let _ = fs::remove_dir_all(&self.destination);
        }
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

fn resolve_destination(destination: &Path) -> Result<PathBuf> {
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
    let parent = parent
        .canonicalize()
        .context("destination parent directory must exist")?;
    let name = destination
        .file_name()
        .context("destination must name a new dataset directory")?;
    Ok(parent.join(name))
}

fn reserve_destination(destination: &Path, overwrite: bool) -> Result<bool> {
    // Reserve new paths atomically; existing datasets remain owned by their caller.
    match fs::create_dir(destination) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && overwrite => {
            ensure!(
                fs::symlink_metadata(destination)?.file_type().is_dir(),
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
) -> Result<JoinHandle<Result<()>>> {
    thread::Builder::new()
        .name("lance-conversion".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            let reader = ChannelReader {
                schema,
                receiver,
                finished: false,
            };
            runtime.block_on(write_dataset(&destination, reader, owns_destination))
        })
        .context("starting Lance writer")
}

async fn write_dataset(
    destination: &Path,
    reader: ChannelReader,
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
    let uri = destination.to_str().unwrap();
    if owns_destination {
        Dataset::write(reader, uri, Some(params)).await?;
    } else {
        let existing = Dataset::open(uri)
            .await
            .context("OVERWRITE requires an existing valid Lance dataset")?;
        Dataset::write(reader, Arc::new(existing), Some(params)).await?;
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
