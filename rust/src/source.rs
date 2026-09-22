use std::fs::File;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use arrow_array::RecordBatchReader;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::{LanceSink, WriteOptions, WriteSummary};

/// Adapters resolve their own input and yield batches with a fixed schema.
/// Future directory, Hub and WARC readers can implement this same contract.
pub trait BatchSource {
    fn open(self) -> Result<Box<dyn RecordBatchReader + Send>>;
}

pub struct ParquetFileSource {
    path: PathBuf,
    batch_size: usize,
}

impl ParquetFileSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            batch_size: 8192,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl BatchSource for ParquetFileSource {
    fn open(self) -> Result<Box<dyn RecordBatchReader + Send>> {
        ensure!(self.batch_size > 0, "batch_size must be positive");
        ensure!(
            self.path.is_file(),
            "input must be a single local Parquet file: {}",
            self.path.display()
        );
        let file = File::open(&self.path).context("opening Parquet input")?;
        Ok(Box::new(
            ParquetRecordBatchReaderBuilder::try_new(file)?
                .with_batch_size(self.batch_size)
                .build()?,
        ))
    }
}

pub fn convert(
    source: impl BatchSource,
    destination: impl AsRef<Path>,
    options: WriteOptions,
) -> Result<WriteSummary> {
    let reader = source.open()?;
    let mut sink = LanceSink::create(destination, reader.schema(), options)?;
    for batch in reader {
        sink.write_batch(batch?)?;
    }
    sink.finish()
}
