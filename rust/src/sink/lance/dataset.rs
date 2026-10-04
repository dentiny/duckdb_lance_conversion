use std::panic::AssertUnwindSafe;
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, PoisonError,
};

use arrow_schema::SchemaRef;
use datafusion_physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};
use futures::{stream, FutureExt, TryStreamExt};
use lance::{
    dataset::{
        builder::DatasetBuilder,
        transaction::{Operation, Transaction},
        write::{CommitBuilder, ExternalBlobMode, InsertBuilder, WriteParams},
    },
    session::Session,
    Dataset,
};
use lance_io::{object_store::WrappingObjectStore, utils::tracking_store::IOTracker};

use super::column_storage::configure_column_storage;
use super::fragment::LanceFragmentWriter;
use super::index::LanceIndexPlan;
use super::{WriteMode, WriteOptions};
use crate::error::ResultExt;
use crate::schema::validate_schema;
use crate::sink::WriteSummary;
use crate::storage::{OpendalStorage, OpendalStoreProvider};
use crate::{Error, Result};

fn empty_stream(schema: SchemaRef) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(schema, stream::empty()))
}

/// State shared by every fragment writer of one dataset write.
pub(super) struct DatasetWrite {
    uri: String,
    /// The destination as of `create`; `None` when it does not exist yet.
    dataset: Option<Arc<Dataset>>,
    params: WriteParams,
    options: WriteOptions,
    schema: SchemaRef,
    indexes: LanceIndexPlan,
    io_tracker: Arc<IOTracker>,
    rows_written: Arc<AtomicU64>,
    transactions: Mutex<Vec<Transaction>>,
}

impl DatasetWrite {
    pub(super) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn context(&self) -> &'static str {
        match self.options.mode {
            WriteMode::Create => "creating Lance dataset",
            WriteMode::Append => "appending to Lance dataset",
            WriteMode::Overwrite => "overwriting Lance dataset",
        }
    }

    /// Writes data files without committing them.
    pub(super) async fn write_fragments(
        &self,
        stream: SendableRecordBatchStream,
    ) -> Result<Transaction> {
        let stream = configure_column_storage(stream, &self.options)?;
        let count = self.rows_written.clone();
        let schema = stream.schema();
        let counted = stream.inspect_ok(move |batch| {
            count.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        });
        let stream: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, counted));
        let builder = match &self.dataset {
            Some(dataset) => InsertBuilder::new(dataset.clone()),
            None => InsertBuilder::new(self.uri.as_str()),
        };
        let result = AssertUnwindSafe(
            builder
                .with_params(&self.params)
                .execute_uncommitted_stream(stream),
        )
        .catch_unwind()
        .await
        .map_err(|_| Error::message("Lance writer task panicked"))?;
        result.context(self.context())
    }

    pub(super) fn add_transaction(&self, transaction: Transaction) {
        self.transactions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(transaction);
    }

    async fn commit(&self) -> Result<WriteSummary> {
        let mut transactions = std::mem::take(
            &mut *self
                .transactions
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        if transactions.is_empty() {
            // Create and overwrite still commit a dataset version for an empty input.
            transactions.push(
                self.write_fragments(empty_stream(self.schema.clone()))
                    .await?,
            );
        }
        let transaction = merge_transactions(transactions)?;
        let mut builder = match &self.dataset {
            Some(dataset) => CommitBuilder::new(dataset.clone()),
            None => CommitBuilder::new(self.uri.as_str()),
        };
        if let Some(session) = &self.params.session {
            builder = builder.with_session(session.clone());
        }
        if let Some(version) = self.params.data_storage_version {
            builder = builder.with_storage_format(version);
        }
        let mut dataset = builder.execute(transaction).await.context(self.context())?;
        self.indexes.create(&mut dataset).await?;
        let stats = self.io_tracker.stats();
        Ok(WriteSummary {
            rows_written: self.rows_written.load(Ordering::Relaxed),
            bytes_read: stats.read_bytes,
            bytes_written: stats.written_bytes,
        })
    }
}

/// Concatenates the fragments of transactions written against the same loaded destination.
/// Fragment ids are assigned when the merged transaction is committed.
fn merge_transactions(transactions: Vec<Transaction>) -> Result<Transaction> {
    let mut transactions = transactions.into_iter();
    let mut merged = transactions
        .next()
        .ok_or_else(|| Error::message("no Lance transaction to commit"))?;
    for transaction in transactions {
        match (&mut merged.operation, transaction.operation) {
            (Operation::Append { fragments }, Operation::Append { fragments: more })
            | (
                Operation::Overwrite { fragments, .. },
                Operation::Overwrite {
                    fragments: more, ..
                },
            ) => fragments.extend(more),
            _ => {
                return Err(Error::message(
                    "Lance fragment writers produced incompatible transactions",
                ))
            }
        }
    }
    Ok(merged)
}

/// One Lance dataset write. Any number of fragment writers may run concurrently;
/// `commit` publishes every finished one as a single dataset version.
pub struct LanceDatasetWriter {
    write: Arc<DatasetWrite>,
}

impl LanceDatasetWriter {
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
        if destination.object_path.as_ref().is_empty() {
            return Err(Error::message(
                "Lance destination must not be a storage root",
            ));
        }
        // Validate options against the stored schema before any file is written.
        let stored = configure_column_storage(empty_stream(schema.clone()), &options)?;
        let indexes = LanceIndexPlan::new(&stored.schema(), &options)?;

        let session = Session::default();
        let io_tracker = Arc::new(IOTracker::default());
        let tracked_store = io_tracker.wrap(
            destination.location.as_str(),
            destination.object_store.clone(),
        );
        session.store_registry().insert(
            destination.location.scheme(),
            Arc::new(OpendalStoreProvider::new(tracked_store)),
        );
        let session = Arc::new(session);
        let uri = destination.location.as_str().to_owned();
        let dataset = match DatasetBuilder::from_uri(&uri)
            .with_session(session.clone())
            .load()
            .await
        {
            Ok(dataset) => Some(Arc::new(dataset)),
            Err(lance::Error::DatasetNotFound { .. } | lance::Error::NotFound { .. }) => None,
            Err(error) => return Err(error).context("opening Lance destination"),
        };
        if matches!(options.mode, WriteMode::Create) && dataset.is_some() {
            return Err(Error::invalid_argument(format!(
                "Lance dataset already exists: {uri}"
            )));
        }
        let params = WriteParams {
            mode: options.mode,
            max_bytes_per_file: options.target_file_size,
            data_storage_version: Some(options.storage_version.parse()?),
            external_blob_mode: if options.blob_columns.is_empty() {
                ExternalBlobMode::Reference
            } else {
                ExternalBlobMode::Ingest
            },
            session: Some(session),
            ..Default::default()
        };
        Ok(Self {
            write: Arc::new(DatasetWrite {
                uri,
                dataset,
                params,
                options,
                schema,
                indexes,
                io_tracker,
                rows_written: Arc::new(AtomicU64::new(0)),
                transactions: Mutex::new(Vec::new()),
            }),
        })
    }

    /// Starts a writer whose fragments join this write. Must be called inside a Tokio runtime.
    pub fn fragment_writer(&self) -> LanceFragmentWriter {
        LanceFragmentWriter::spawn(self.write.clone())
    }

    /// Writes a whole stream as fragments of this write, without a channel.
    pub(super) async fn write_stream(&self, stream: SendableRecordBatchStream) -> Result<()> {
        let transaction = self.write.write_fragments(stream).await?;
        self.write.add_transaction(transaction);
        Ok(())
    }

    /// Commits every finished fragment writer as one dataset version, then builds indexes.
    /// Fragments of fragment writers that were not finished are left uncommitted.
    pub async fn commit(self) -> Result<WriteSummary> {
        self.write.commit().await
    }
}
