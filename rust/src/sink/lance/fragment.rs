use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use datafusion_physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};
use futures::stream;
use lance::dataset::transaction::Transaction;
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinHandle,
};

use super::dataset::DatasetWrite;
use crate::error::ResultExt;
use crate::{Error, Result};

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
        worker: JoinHandle<Result<Transaction>>,
    },
    Closing(JoinHandle<Result<Transaction>>),
    Finished,
    Failed,
}

/// Push-based fragment writer for callers such as DuckDB, with at most one queued batch.
/// Dropping the sender lets the writer task stop an unfinished write.
pub struct LanceFragmentWriter {
    write: Arc<DatasetWrite>,
    state: SinkState,
}

impl LanceFragmentWriter {
    /// Must be called inside a Tokio runtime.
    pub(super) fn spawn(write: Arc<DatasetWrite>) -> Self {
        let (sender, receiver) = channel(1);
        let stream = batch_stream(write.schema().clone(), receiver);
        let worker = tokio::spawn({
            let write = write.clone();
            async move { write.write_fragments(stream).await }
        });
        Self {
            write,
            state: SinkState::Open { sender, worker },
        }
    }

    pub fn schema(&self) -> &SchemaRef {
        self.write.schema()
    }

    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let SinkState::Open { sender, .. } = &self.state else {
            return Err(Error::message("writer is already finished or failed"));
        };
        if batch.schema().as_ref() != self.write.schema().as_ref() {
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

    /// Writes the remaining data; the fragments become visible when the dataset writer commits.
    pub async fn finish(&mut self) -> Result<()> {
        let SinkState::Open { sender, .. } = &self.state else {
            return Err(Error::message("writer is already finished or failed"));
        };
        let sent = sender.send(Message::Finish).await.is_ok();
        self.close_input();
        let transaction = self.join_worker().await?;
        if !sent {
            return Err(Error::message("Lance writer stopped before finish"));
        }
        self.write.add_transaction(transaction);
        self.state = SinkState::Finished;
        Ok(())
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

    async fn join_worker(&mut self) -> Result<Transaction> {
        let SinkState::Closing(worker) = &mut self.state else {
            return Err(Error::message("writer is closed"));
        };
        let result = worker.await;
        self.state = SinkState::Failed;
        result.context("Lance writer task failed")?
    }
}
