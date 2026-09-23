use std::ops::Range;
use std::path::PathBuf;
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

async fn parquet_paths(storage: &OpendalStorage) -> Result<Vec<String>> {
    let path = storage.object_path.to_string();
    if !path.ends_with('/') {
        match storage.operator.stat(&path).await {
            Ok(metadata) if metadata.is_file() => return Ok(vec![path]),
            Ok(metadata) if !metadata.is_dir() => {
                return Err(Error::message("Parquet input must be a file or directory"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }

    let prefix = if path.is_empty() || path.ends_with('/') {
        path
    } else {
        format!("{path}/")
    };
    let mut entries = storage
        .operator
        .lister_with(&prefix)
        .recursive(true)
        .await?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.try_next().await? {
        if entry.metadata().is_file()
            && entry
                .path()
                .rsplit_once('.')
                .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("parquet"))
        {
            paths.push(entry.path().to_owned());
        }
    }
    paths.sort_unstable();
    if paths.is_empty() {
        return Err(Error::message(format!(
            "Parquet directory contains no .parquet files: {}",
            storage.location
        )));
    }
    Ok(paths)
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
        let path = self
            .path
            .to_str()
            .ok_or_else(|| Error::message("Parquet input path must be valid UTF-8"))?;
        let storage = OpendalStorage::from_path(path, self.s3_config.as_ref())?;
        let paths = parquet_paths(&storage).await?;
        open_parquet_paths(storage.operator, paths, self.batch_size).await
    }
}
