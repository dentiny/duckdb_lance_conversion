use std::sync::Arc;

use arrow_schema::SchemaRef;
use futures::{stream, StreamExt, TryStreamExt};
use parquet::arrow::async_reader::{ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use super::parquet_metadata::{load_parquet_metadata, OpendalParquetReader};
use super::{BatchStream, ReadMetrics};
use crate::{Error, Result};

/// Top-level columns to read. A Parquet projection mask always yields columns
/// in file order, so `order` maps the file-order output back to the requested
/// order.
#[derive(Clone)]
struct ColumnProjection {
    roots: Vec<usize>,
    order: Vec<usize>,
}

impl ColumnProjection {
    /// Returns `None` for an empty request, which reads every column.
    fn new(columns: Vec<usize>) -> Result<Option<Self>> {
        if columns.is_empty() {
            return Ok(None);
        }
        let mut roots = columns.clone();
        roots.sort_unstable();
        if let Some(pair) = roots.windows(2).find(|pair| pair[0] == pair[1]) {
            return Err(Error::message(format!(
                "duplicate Parquet column index: {}",
                pair[0]
            )));
        }
        let order = columns
            .iter()
            .map(|column| roots.partition_point(|root| root < column))
            .collect();
        Ok(Some(Self { roots, order }))
    }

    fn mask(
        &self,
        parquet_schema: &SchemaDescriptor,
        field_count: usize,
    ) -> Result<ProjectionMask> {
        if let Some(&index) = self.roots.last().filter(|&&index| index >= field_count) {
            return Err(Error::message(format!(
                "invalid Parquet column index: {index}"
            )));
        }
        Ok(ProjectionMask::roots(
            parquet_schema,
            self.roots.iter().copied(),
        ))
    }
}

async fn open_parquet(
    operator: opendal::Operator,
    path: String,
    batch_size: usize,
    projection: Option<&ColumnProjection>,
    metrics: Arc<ReadMetrics>,
) -> Result<ParquetRecordBatchStream<OpendalParquetReader>> {
    let builder =
        ParquetRecordBatchStreamBuilder::new(OpendalParquetReader::new(operator, path, metrics))
            .await?
            .with_batch_size(batch_size);
    let builder = match projection {
        Some(projection) => {
            let mask =
                projection.mask(builder.parquet_schema(), builder.schema().fields().len())?;
            builder.with_projection(mask)
        }
        None => builder,
    };
    Ok(builder.build()?)
}

async fn open_parquet_in_order(
    operator: opendal::Operator,
    paths: Vec<String>,
    batch_size: usize,
    projection: Option<ColumnProjection>,
    metrics: Arc<ReadMetrics>,
) -> Result<(SchemaRef, BatchStream)> {
    let mut paths = paths.into_iter();
    let first_path = paths
        .next()
        .ok_or_else(|| Error::message("Parquet source contains no .parquet files"))?;
    let reader = open_parquet(
        operator.clone(),
        first_path,
        batch_size,
        projection.as_ref(),
        metrics.clone(),
    )
    .await?;
    let schema = reader.schema().clone();
    let expected_schema = schema.clone();
    let open_remaining = move |path: String| {
        let operator = operator.clone();
        let expected_schema = expected_schema.clone();
        let projection = projection.clone();
        let metrics = metrics.clone();
        async move {
            let reader = open_parquet(
                operator,
                path.clone(),
                batch_size,
                projection.as_ref(),
                metrics,
            )
            .await?;
            if reader.schema() != &expected_schema {
                return Err(Error::message(format!(
                    "Parquet schema does not match the first file: {path}"
                )));
            }
            Ok(reader.map_err(Error::from))
        }
    };
    let remaining = stream::iter(paths).then(open_remaining).try_flatten();
    let batches = reader.map_err(Error::from).chain(remaining);
    Ok((schema, Box::pin(batches)))
}

/// Opens one reader per row group and merges their batches as they become
/// available.
///
/// File metadata is loaded concurrently but retained in `paths` order so the
/// first file defines the expected schema. All files are validated before any
/// batches are emitted. At most `max_read_parallelism` row-group readers are
/// active at once; values above the total row-group count are effectively
/// capped. Batch order is intentionally nondeterministic.
async fn open_parquet_row_groups(
    operator: opendal::Operator,
    paths: Vec<String>,
    batch_size: usize,
    max_read_parallelism: usize,
    projection: Option<ColumnProjection>,
    metrics: Arc<ReadMetrics>,
) -> Result<(SchemaRef, BatchStream)> {
    let (schema, files) = load_parquet_metadata(
        operator.clone(),
        paths,
        max_read_parallelism,
        metrics.clone(),
    )
    .await?;
    let mut row_groups = Vec::new();
    for file in files {
        let mask = projection
            .as_ref()
            .map(|projection| {
                projection.mask(file.metadata.parquet_schema(), schema.fields().len())
            })
            .transpose()?;
        for index in 0..file.metadata.metadata().num_row_groups() {
            row_groups.push((
                file.path.clone(),
                file.metadata.clone(),
                mask.clone(),
                index,
            ));
        }
    }
    let schema = match &projection {
        Some(projection) => Arc::new(schema.project(&projection.roots)?),
        None => schema,
    };
    let parallelism = row_groups.len().min(max_read_parallelism).max(1);
    let readers = stream::iter(row_groups).map(move |(path, metadata, mask, row_group)| {
        let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
            OpendalParquetReader::new(operator.clone(), path, metrics.clone()),
            metadata,
        )
        .with_batch_size(batch_size)
        .with_row_groups(vec![row_group]);
        let builder = match mask {
            Some(mask) => builder.with_projection(mask),
            None => builder,
        };
        builder
            .build()
            .map(|reader| reader.map_err(Error::from))
            .map_err(Error::from)
    });
    Ok((
        schema,
        Box::pin(readers.try_flatten_unordered(Some(parallelism))),
    ))
}

/// Opens multiple Parquet files as one Arrow batch stream.
///
/// The first path defines the returned schema, and every subsequent file must
/// match it. When `preserve_insertion_order` is `true`, files and row groups
/// are read sequentially in `paths` order. When it is `false`, file metadata
/// and row groups are read concurrently, batches are emitted as soon as they
/// are available, and `max_read_parallelism` caps both phases.
///
/// `projection` lists top-level column indices in output order; only those
/// columns are fetched and decoded. `None` or an empty list reads every
/// column.
///
/// Returns an error when `paths` is empty, either numeric option is zero, a
/// projected index is invalid or repeated, a file cannot be opened, or
/// schemas differ.
pub(crate) async fn open_parquet_paths(
    operator: opendal::Operator,
    paths: Vec<String>,
    batch_size: usize,
    preserve_insertion_order: bool,
    max_read_parallelism: usize,
    projection: Option<Vec<usize>>,
    metrics: Arc<ReadMetrics>,
) -> Result<(SchemaRef, BatchStream)> {
    if batch_size == 0 {
        return Err(Error::message("batch_size must be positive"));
    }
    if paths.is_empty() {
        return Err(Error::message("Parquet source contains no .parquet files"));
    }
    if max_read_parallelism == 0 {
        return Err(Error::message("max_read_parallelism must be positive"));
    }
    let projection = projection.map(ColumnProjection::new).transpose()?.flatten();
    let (schema, batches) = if preserve_insertion_order {
        open_parquet_in_order(operator, paths, batch_size, projection.clone(), metrics).await?
    } else {
        open_parquet_row_groups(
            operator,
            paths,
            batch_size,
            max_read_parallelism,
            projection.clone(),
            metrics,
        )
        .await?
    };
    let Some(ColumnProjection { order, .. }) = projection else {
        return Ok((schema, batches));
    };
    let schema = Arc::new(schema.project(&order)?);
    let batches = batches.map(move |batch| batch.and_then(|batch| Ok(batch.project(&order)?)));
    Ok((schema, Box::pin(batches)))
}
