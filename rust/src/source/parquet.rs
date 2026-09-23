use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use bytes::Bytes;
use futures::{future::BoxFuture, stream, FutureExt, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{
    AsyncFileReader, MetadataSuffixFetch, ParquetRecordBatchStreamBuilder,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use super::{BatchSource, BatchStream};
use crate::storage::OpendalStorage;
use crate::{Error, Result, S3StorageConfig};

const DEFAULT_BATCH_SIZE: usize = 8192;
const MAX_CONCURRENT_RANGE_READS: usize = 8;

struct OpendalParquetReader {
    operator: opendal::Operator,
    path: String,
}

fn opendal_error(error: opendal::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

impl AsyncFileReader for OpendalParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            self.operator
                .read_with(&self.path)
                .range(range)
                .await
                .map(|buffer| buffer.to_bytes())
                .map_err(opendal_error)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let operator = &self.operator;
        let path = &self.path;
        stream::iter(ranges)
            .map(move |range| async move {
                operator
                    .read_with(path)
                    .range(range)
                    .await
                    .map(|buffer| buffer.to_bytes())
                    .map_err(opendal_error)
            })
            .buffered(MAX_CONCURRENT_RANGE_READS)
            .try_collect()
            .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        async move {
            let metadata_options = options.map(|options| options.metadata_options().clone());
            let metadata = ParquetMetaDataReader::new()
                .with_metadata_options(metadata_options)
                .load_via_suffix_and_finish(self)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

impl MetadataSuffixFetch for &mut OpendalParquetReader {
    fn fetch_suffix(&mut self, suffix: usize) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            self.operator
                .read_with(&self.path)
                .range(opendal::BytesRange::suffix(suffix as u64))
                .await
                .map(|buffer| buffer.to_bytes())
                .map_err(opendal_error)
        }
        .boxed()
    }
}

pub struct ParquetFileSource {
    path: PathBuf,
    batch_size: usize,
    s3_config: Option<S3StorageConfig>,
}

impl ParquetFileSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            batch_size: DEFAULT_BATCH_SIZE,
            s3_config: None,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_s3_config(mut self, config: S3StorageConfig) -> Self {
        self.s3_config = Some(config);
        self
    }
}

impl BatchSource for ParquetFileSource {
    async fn open(self) -> Result<(SchemaRef, BatchStream)> {
        if self.batch_size == 0 {
            return Err(Error::message("batch_size must be positive"));
        }
        let path = self
            .path
            .to_str()
            .ok_or_else(|| Error::message("Parquet input path must be valid UTF-8"))?;
        let storage = OpendalStorage::from_path(path, self.s3_config.as_ref())?;
        let object_path = storage.object_path.to_string();
        let object_reader = OpendalParquetReader {
            operator: storage.operator,
            path: object_path,
        };
        let reader = ParquetRecordBatchStreamBuilder::new(object_reader)
            .await?
            .with_batch_size(self.batch_size)
            .build()?;
        let schema = reader.schema().clone();
        let batches = reader.map_err(Error::from);
        Ok((schema, Box::pin(batches)))
    }
}
