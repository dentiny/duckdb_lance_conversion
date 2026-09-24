use std::ops::Range;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use bytes::Bytes;
use futures::{future::BoxFuture, stream, FutureExt, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{
    AsyncFileReader, MetadataSuffixFetch, ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};

use super::BatchStream;
use crate::{Error, Result};

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

async fn open_parquet(
    operator: opendal::Operator,
    path: String,
    batch_size: usize,
) -> Result<ParquetRecordBatchStream<OpendalParquetReader>> {
    Ok(
        ParquetRecordBatchStreamBuilder::new(OpendalParquetReader { operator, path })
            .await?
            .with_batch_size(batch_size)
            .build()?,
    )
}

pub(crate) async fn open_parquet_paths(
    operator: opendal::Operator,
    paths: Vec<String>,
    batch_size: usize,
) -> Result<(SchemaRef, BatchStream)> {
    if batch_size == 0 {
        return Err(Error::message("batch_size must be positive"));
    }
    let mut paths = paths.into_iter();
    let first_path = paths
        .next()
        .ok_or_else(|| Error::message("Parquet source contains no .parquet files"))?;
    let reader = open_parquet(operator.clone(), first_path, batch_size).await?;
    let schema = reader.schema().clone();
    let expected_schema = schema.clone();
    let remaining = stream::iter(paths)
        .then(move |path| {
            let operator = operator.clone();
            let expected_schema = expected_schema.clone();
            async move {
                let reader = open_parquet(operator, path.clone(), batch_size).await?;
                if reader.schema() != &expected_schema {
                    return Err(Error::message(format!(
                        "Parquet schema does not match the first file: {path}"
                    )));
                }
                Ok(reader.map_err(Error::from))
            }
        })
        .try_flatten();
    let batches = reader.map_err(Error::from).chain(remaining);
    Ok((schema, Box::pin(batches)))
}
