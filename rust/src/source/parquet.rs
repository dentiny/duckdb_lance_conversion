use std::sync::Arc;

use arrow_schema::SchemaRef;
use futures::{stream, StreamExt, TryStreamExt};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescriptor;

use super::parquet_metadata::{load_parquet_metadata, OpendalParquetReader};
use super::{BatchStream, ReadMetrics};
use crate::{Error, Result};

/// Top-level columns to read. A Parquet projection mask always yields columns
/// in file order, so `order` maps the file-order output back to the requested
/// order.
///
/// For example, requesting columns `[2, 0]` of file columns `[a, b, c]` gives
/// `roots = [0, 2]`, so the reader yields `[a, c]`; `order = [1, 0]` then
/// reorders each batch to the requested `[c, a]`.
struct ColumnProjection {
    /// Requested column indices in file order, used to build the projection mask.
    roots: Vec<usize>,
    /// For each requested column, its position in the file-order output.
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

/// Returns the column chunk bytes that `mask` selects, or every column
/// chunk's bytes without a mask.
fn column_chunk_bytes(metadata: &ParquetMetaData, mask: Option<&ProjectionMask>) -> u64 {
    metadata
        .row_groups()
        .iter()
        .flat_map(|row_group| row_group.columns().iter().enumerate())
        .filter(|(leaf, _)| mask.is_none_or(|mask| mask.leaf_included(*leaf)))
        .map(|(_, column)| column.byte_range().1)
        .fold(0, u64::saturating_add)
}

/// Opens multiple Parquet files as one Arrow batch stream, with one reader per
/// row group.
///
/// File footers are loaded concurrently, at most `max_read_parallelism` at a
/// time, but retained in `paths` order: the first file defines the returned
/// schema, and every file must match it before any batch is emitted. When
/// `preserve_insertion_order` is `true`, row groups are read one at a time in
/// `paths` order. When it is `false`, at most `max_read_parallelism` row-group
/// readers are active at once and batches are emitted as soon as they are
/// available.
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
    let (schema, files) = load_parquet_metadata(
        operator.clone(),
        paths,
        max_read_parallelism,
        metrics.clone(),
    )
    .await?;
    let mut row_groups = Vec::new();
    let mut unread_bytes = 0_u64;
    for file in files {
        let mask = projection
            .as_ref()
            .map(|projection| {
                projection.mask(file.metadata.parquet_schema(), schema.fields().len())
            })
            .transpose()?;
        unread_bytes = unread_bytes
            .saturating_add(column_chunk_bytes(file.metadata.metadata(), mask.as_ref()));
        for index in 0..file.metadata.metadata().num_row_groups() {
            row_groups.push((
                file.path.clone(),
                file.metadata.clone(),
                mask.clone(),
                index,
            ));
        }
    }
    // Every footer has been read; only the selected column chunks remain.
    metrics.set_total_bytes(metrics.snapshot().bytes_read.saturating_add(unread_bytes));
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
    let batches: BatchStream = if preserve_insertion_order {
        Box::pin(readers.try_flatten())
    } else {
        Box::pin(readers.try_flatten_unordered(Some(parallelism)))
    };
    let Some(ColumnProjection { roots, order }) = projection else {
        return Ok((schema, batches));
    };
    let schema = Arc::new(schema.project(&roots)?.project(&order)?);
    let batches = batches.map(move |batch| batch.and_then(|batch| Ok(batch.project(&order)?)));
    Ok((schema, Box::pin(batches)))
}
