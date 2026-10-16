use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{new_null_array, ArrayRef, RecordBatch, StructArray};
use arrow_cast::cast;
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use datafusion_physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};
use futures::StreamExt;
use lance::{blob_field_with_options, BlobFieldOptions};

use super::{column_encoding::configure_column_encoding, WriteOptions};
use crate::{Error, Result};

/// Rewrites the stream schema into the schema Lance stores: blob columns and
/// per-column encoding metadata.
pub(super) fn configure_column_storage(
    stream: SendableRecordBatchStream,
    options: &WriteOptions,
) -> Result<SendableRecordBatchStream> {
    let stream = configure_blob_storage(stream, options)?;
    configure_column_encoding(stream, &options.column_encoding)
}

fn transform_blob_batch(
    batch: RecordBatch,
    schema: SchemaRef,
    binary_blob_columns: &[usize],
    uri_blob_columns: &[usize],
) -> std::result::Result<RecordBatch, ArrowError> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, column) in batch.columns().iter().enumerate() {
        let is_binary = binary_blob_columns.binary_search(&index).is_ok();
        let is_uri = uri_blob_columns.binary_search(&index).is_ok();
        if !is_binary && !is_uri {
            columns.push(column.clone());
            continue;
        }
        let field = schema.field(index);
        let DataType::Struct(children) = field.data_type() else {
            return Err(ArrowError::SchemaError(format!(
                "blob field '{}' is not a struct",
                field.name()
            )));
        };
        let (data, uri) = if is_uri {
            (
                new_null_array(&DataType::LargeBinary, column.len()),
                cast(column, &DataType::Utf8)?,
            )
        } else {
            (
                cast(column, &DataType::LargeBinary)?,
                new_null_array(&DataType::Utf8, column.len()),
            )
        };
        columns.push(Arc::new(StructArray::try_new(
            children.clone(),
            vec![data, uri],
            column.nulls().cloned(),
        )?) as ArrayRef);
    }
    RecordBatch::try_new(schema, columns)
}

fn configure_blob_storage(
    stream: SendableRecordBatchStream,
    options: &WriteOptions,
) -> Result<SendableRecordBatchStream> {
    if options.blob_inline_size_threshold.is_none()
        && options.blob_dedicated_size_threshold.is_none()
        && options.blob_columns.is_empty()
    {
        return Ok(stream);
    }
    let mut blob_options = BlobFieldOptions::default();
    if let Some(threshold) = options.blob_inline_size_threshold {
        blob_options = blob_options.with_inline_size_threshold(threshold);
    }
    if let Some(threshold) = options.blob_dedicated_size_threshold {
        let threshold = NonZeroUsize::new(threshold).ok_or_else(|| {
            Error::message("blob dedicated size threshold must be greater than zero")
        })?;
        blob_options = blob_options.with_dedicated_size_threshold(threshold);
    }

    let input_schema = stream.schema();
    let binary_blob_columns = input_schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| {
            matches!(
                field.data_type(),
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            )
            .then_some(index)
        })
        .collect::<Vec<_>>();
    let mut uri_blob_columns = options
        .blob_columns
        .iter()
        .map(|name| {
            let index = input_schema
                .index_of(name)
                .map_err(|_| Error::message(format!("BLOB_COLUMNS column not found: {name}")))?;
            ensure_uri_column(input_schema.field(index).data_type(), name)?;
            Ok(index)
        })
        .collect::<Result<Vec<_>>>()?;
    uri_blob_columns.sort_unstable();
    if uri_blob_columns
        .windows(2)
        .any(|indices| indices[0] == indices[1])
    {
        return Err(Error::message(
            "BLOB_COLUMNS cannot contain duplicate columns",
        ));
    }
    let mut blob_columns = binary_blob_columns.clone();
    blob_columns.extend_from_slice(&uri_blob_columns);
    blob_columns.sort_unstable();
    if blob_columns.is_empty() {
        return Ok(stream);
    }

    let fields = input_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if blob_columns.binary_search(&index).is_ok() {
                Arc::new(blob_field_with_options(
                    field.name(),
                    field.is_nullable(),
                    blob_options.clone(),
                ))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    let output_schema = Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ));
    let batch_schema = output_schema.clone();
    let batches = stream.map(move |batch| {
        let batch = batch?;
        Ok(transform_blob_batch(
            batch,
            batch_schema.clone(),
            &binary_blob_columns,
            &uri_blob_columns,
        )?)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        batches,
    )))
}

fn ensure_uri_column(data_type: &DataType, name: &str) -> Result<()> {
    if matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        Ok(())
    } else {
        Err(Error::message(format!(
            "BLOB_COLUMNS column must be a string: {name}"
        )))
    }
}
