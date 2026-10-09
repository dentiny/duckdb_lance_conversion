use std::path::{Path, PathBuf};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, TryStreamExt};
pub use lance::dataset::write::WriteMode;

use super::{BatchSink, WriteSummary};
use crate::{Error, OpendalConfig, Result, S3StorageConfig};

mod column_storage;
mod dataset;
mod fragment;
mod index;

pub use dataset::LanceDatasetWriter;
pub use fragment::LanceFragmentWriter;

const DEFAULT_BLOB_INLINE_SIZE_THRESHOLD: usize = 2 * 1024 * 1024;
const DEFAULT_BLOB_DEDICATED_SIZE_THRESHOLD: usize = 16 * 1024 * 1024;
const DEFAULT_TARGET_FILE_SIZE: usize = 512 * 1024 * 1024;
const DEFAULT_STORAGE_VERSION: &str = "stable";

#[derive(Clone, Debug)]
pub struct WriteOptions {
    pub mode: WriteMode,
    pub s3_config: Option<S3StorageConfig>,
    pub opendal_config: OpendalConfig,
    pub blob_inline_size_threshold: Option<usize>,
    pub blob_dedicated_size_threshold: Option<usize>,
    pub target_file_size: usize,
    pub storage_version: String,
    /// (column, algorithm) pairs set as `lance-encoding:compression` field metadata.
    pub column_compression: Vec<(String, String)>,
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
            opendal_config: OpendalConfig::default(),
            blob_inline_size_threshold: Some(DEFAULT_BLOB_INLINE_SIZE_THRESHOLD),
            blob_dedicated_size_threshold: Some(DEFAULT_BLOB_DEDICATED_SIZE_THRESHOLD),
            target_file_size: DEFAULT_TARGET_FILE_SIZE,
            storage_version: DEFAULT_STORAGE_VERSION.into(),
            column_compression: Vec::new(),
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
        let writer =
            LanceDatasetWriter::create(&self.destination, schema.clone(), self.options).await?;
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
        writer
            .write_stream(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
            .await?;
        writer.commit().await
    }
}

/// Single fragment writer that commits its own dataset write on `finish`.
pub struct LanceWriter {
    dataset: Option<LanceDatasetWriter>,
    writer: LanceFragmentWriter,
}

impl LanceWriter {
    pub async fn create(
        destination: impl AsRef<Path>,
        schema: SchemaRef,
        options: WriteOptions,
    ) -> Result<Self> {
        let dataset = LanceDatasetWriter::create(destination, schema, options).await?;
        let writer = dataset.fragment_writer();
        Ok(Self {
            dataset: Some(dataset),
            writer,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        self.writer.schema()
    }

    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.write_batch(batch).await
    }

    pub async fn finish(&mut self) -> Result<WriteSummary> {
        let dataset = self
            .dataset
            .take()
            .ok_or_else(|| Error::message("writer is already finished or failed"))?;
        self.writer.finish().await?;
        dataset.commit().await
    }
}
