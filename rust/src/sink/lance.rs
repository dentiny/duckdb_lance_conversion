use std::collections::BTreeMap;
use std::path::Path;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
pub use lance::dataset::write::WriteMode;

use super::WriteSummary;
use crate::{Error, OpendalConfig, Result, S3StorageConfig};

mod blob_fetch;
mod column_encoding;
mod column_storage;
mod dataset;
mod fragment;
mod index;

pub use column_encoding::ColumnEncoding;
pub use dataset::LanceDatasetWriter;
pub use fragment::LanceFragmentWriter;

const DEFAULT_BLOB_INLINE_SIZE_THRESHOLD: usize = 2 * 1024 * 1024;
const DEFAULT_BLOB_DEDICATED_SIZE_THRESHOLD: usize = 16 * 1024 * 1024;
const DEFAULT_TARGET_FILE_SIZE: usize = 512 * 1024 * 1024;
const DEFAULT_STORAGE_VERSION: &str = "stable";

/// What to do with a `blob_columns` value whose object is missing or
/// forbidden. Other read errors, such as rate limits, always fail the write.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BlobOnError {
    /// Fail the write.
    #[default]
    Fail,
    /// Store NULL for that value.
    Null,
    /// Drop the row.
    Skip,
}

#[derive(Clone, Debug)]
pub struct WriteOptions {
    pub mode: WriteMode,
    pub s3_config: Option<S3StorageConfig>,
    pub opendal_config: OpendalConfig,
    pub blob_inline_size_threshold: Option<usize>,
    pub blob_dedicated_size_threshold: Option<usize>,
    pub target_file_size: usize,
    pub storage_version: String,
    /// Per-column Lance encoding settings, keyed by column name.
    pub column_encoding: BTreeMap<String, ColumnEncoding>,
    pub blob_columns: Vec<String>,
    pub blob_on_error: BlobOnError,
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
            column_encoding: BTreeMap::new(),
            blob_columns: Vec::new(),
            blob_on_error: BlobOnError::Fail,
            scalar_index_columns: Vec::new(),
            vector_index_columns: Vec::new(),
            text_index_columns: Vec::new(),
            bloom_filter_index_columns: Vec::new(),
        }
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
