use std::ops::Range;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use bytes::Bytes;
use futures::{future::BoxFuture, stream, FutureExt, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::{AsyncFileReader, MetadataSuffixFetch};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use super::ReadMetrics;
use crate::{Error, Result};

const MAX_CONCURRENT_RANGE_READS: usize = 8;

pub(super) struct OpendalParquetReader {
    operator: opendal::Operator,
    path: String,
    metrics: Arc<ReadMetrics>,
}

impl OpendalParquetReader {
    pub(super) fn new(
        operator: opendal::Operator,
        path: String,
        metrics: Arc<ReadMetrics>,
    ) -> Self {
        Self {
            operator,
            path,
            metrics,
        }
    }
}

fn opendal_error(error: opendal::Error) -> ParquetError {
    ParquetError::External(Box::new(error))
}

impl AsyncFileReader for OpendalParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        async move {
            let bytes = self
                .operator
                .read_with(&self.path)
                .range(range)
                .await
                .map(|buffer| buffer.to_bytes())
                .map_err(opendal_error)?;
            self.metrics.add_bytes_read(bytes.len());
            Ok(bytes)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let operator = &self.operator;
        let path = &self.path;
        let metrics = &self.metrics;
        stream::iter(ranges)
            .map(move |range| async move {
                let bytes = operator
                    .read_with(path)
                    .range(range)
                    .await
                    .map(|buffer| buffer.to_bytes())
                    .map_err(opendal_error)?;
                metrics.add_bytes_read(bytes.len());
                Ok(bytes)
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
            let bytes = self
                .operator
                .read_with(&self.path)
                .range(opendal::BytesRange::suffix(suffix as u64))
                .await
                .map(|buffer| buffer.to_bytes())
                .map_err(opendal_error)?;
            self.metrics.add_bytes_read(bytes.len());
            Ok(bytes)
        }
        .boxed()
    }
}

pub(super) struct ParquetFileMetadata {
    pub path: String,
    pub metadata: ArrowReaderMetadata,
}

/// Loads file footers with bounded concurrency while retaining `paths` order,
/// then validates every Arrow schema against the first file.
pub(super) async fn load_parquet_metadata(
    operator: opendal::Operator,
    paths: Vec<String>,
    max_read_parallelism: usize,
    metrics: Arc<ReadMetrics>,
) -> Result<(SchemaRef, Vec<ParquetFileMetadata>)> {
    let parallelism = paths.len().min(max_read_parallelism).max(1);
    let files = stream::iter(paths)
        .map(move |path| {
            let operator = operator.clone();
            let metrics = metrics.clone();
            async move {
                let mut reader = OpendalParquetReader::new(operator, path.clone(), metrics);
                let metadata =
                    ArrowReaderMetadata::load_async(&mut reader, ArrowReaderOptions::new()).await?;
                Ok::<_, Error>(ParquetFileMetadata { path, metadata })
            }
        })
        .buffered(parallelism)
        .try_collect::<Vec<_>>()
        .await?;

    let first = files
        .first()
        .ok_or_else(|| Error::message("Parquet source contains no .parquet files"))?;
    let schema = first.metadata.schema().clone();
    for file in files.iter().skip(1) {
        if file.metadata.schema() != &schema {
            return Err(Error::message(format!(
                "Parquet schema does not match the first file: {}",
                file.path
            )));
        }
    }
    Ok((schema, files))
}
