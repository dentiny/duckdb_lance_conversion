use std::future::Future;
use std::path::Path;

use anyhow::Result;
use datafusion_physical_plan::SendableRecordBatchStream;

use crate::{write_stream, WriteOptions, WriteSummary};

mod parquet;
pub use parquet::ParquetFileSource;

/// Adapters asynchronously open a batch stream with a fixed schema.
/// Future directory, Hub and WARC readers can implement this same contract.
pub trait BatchSource: Send {
    fn open(self) -> impl Future<Output = Result<SendableRecordBatchStream>> + Send;
}

pub async fn convert(
    source: impl BatchSource,
    destination: impl AsRef<Path>,
    options: WriteOptions,
) -> Result<WriteSummary> {
    write_stream(destination, source.open().await?, options).await
}
