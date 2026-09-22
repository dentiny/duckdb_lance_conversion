use std::path::PathBuf;

use anyhow::{ensure, Context, Result};
use arrow_schema::ArrowError;
use datafusion_physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};
use futures::TryStreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;

use super::BatchSource;

const DEFAULT_BATCH_SIZE: usize = 8192;

pub struct ParquetFileSource {
    path: PathBuf,
    batch_size: usize,
}

impl ParquetFileSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl BatchSource for ParquetFileSource {
    async fn open(self) -> Result<SendableRecordBatchStream> {
        ensure!(self.batch_size > 0, "batch_size must be positive");
        let file = File::open(&self.path)
            .await
            .context("opening Parquet input")?;
        ensure!(
            file.metadata().await?.is_file(),
            "input must be a single local Parquet file: {}",
            self.path.display()
        );
        let reader = ParquetRecordBatchStreamBuilder::new(file)
            .await?
            .with_batch_size(self.batch_size)
            .build()?;
        let schema = reader.schema().clone();
        let stream = reader.map_err(|error| ArrowError::ExternalError(Box::new(error)).into());
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
