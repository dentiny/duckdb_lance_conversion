use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{make_array, new_null_array, Array, ArrayRef, RecordBatch, StructArray};
use arrow_cast::cast;
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema};
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

fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

fn is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_blob(field: &Field) -> bool {
    field
        .metadata()
        .get("ARROW:extension:name")
        .is_some_and(|name| name == "lance.blob.v2")
}

/// ATIF's fixed ContentPart shape survives DuckDB's Arrow round trip, which
/// drops field metadata and narrows LargeBinary to Binary.
fn is_atif_content(fields: &arrow_schema::Fields) -> bool {
    if fields.len() != 3
        || fields[0].name() != "type"
        || !is_string(fields[0].data_type())
        || fields[1].name() != "text"
        || !is_binary(fields[1].data_type())
        || fields[2].name() != "source"
    {
        return false;
    }
    let DataType::Struct(source) = fields[2].data_type() else {
        return false;
    };
    source.len() == 3
        && source[0].name() == "media_type"
        && is_string(source[0].data_type())
        && source[1].name() == "path"
        && (is_binary(source[1].data_type())
            || is_string(source[1].data_type())
            || is_blob(&source[1]))
        && source[2].name() == "duration_sec"
        && source[2].data_type() == &DataType::Float64
}

fn atif_text(field: &FieldRef) -> FieldRef {
    let mut metadata = field.metadata().clone();
    metadata
        .entry("lance-encoding:compression".into())
        .or_insert_with(|| "lz4".into());
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(DataType::LargeBinary)
            .with_metadata(metadata),
    )
}

/// Promote through structs and variable-length lists, which the pinned Lance
/// blob preprocessor supports. Fixed-size lists, maps and dictionaries retain
/// their existing binary storage. Already-logical blobs are opaque.
fn blob_field(field: &FieldRef, options: Option<&BlobFieldOptions>) -> FieldRef {
    if is_blob(field) {
        return field.clone();
    }
    if is_binary(field.data_type()) {
        let Some(options) = options else {
            return field.clone();
        };
        let mut blob = blob_field_with_options(field.name(), field.is_nullable(), options.clone());
        let mut metadata = field.metadata().clone();
        metadata.extend(blob.metadata().clone());
        blob = blob.with_metadata(metadata);
        return Arc::new(blob);
    }
    let data_type = match field.data_type() {
        DataType::Struct(fields) => {
            let atif = is_atif_content(fields);
            DataType::Struct(
                fields
                    .iter()
                    .map(|field| {
                        if atif && field.name() == "text" {
                            atif_text(field)
                        } else {
                            blob_field(field, options)
                        }
                    })
                    .collect(),
            )
        }
        DataType::List(child) => DataType::List(blob_field(child, options)),
        DataType::LargeList(child) => DataType::LargeList(blob_field(child, options)),
        _ => return field.clone(),
    };
    Arc::new(field.as_ref().clone().with_data_type(data_type))
}

pub(super) fn transform_array(
    array: &ArrayRef,
    target: &Field,
) -> std::result::Result<ArrayRef, ArrowError> {
    if array.data_type() == target.data_type() {
        return Ok(array.clone());
    }
    if is_binary(array.data_type()) && target.data_type() == &DataType::LargeBinary {
        return cast(array, target.data_type());
    }
    if is_blob(target) {
        let DataType::Struct(children) = target.data_type() else {
            return Err(ArrowError::SchemaError("blob field is not a struct".into()));
        };
        let (data, uri) = if is_binary(array.data_type()) {
            (
                cast(array, &DataType::LargeBinary)?,
                new_null_array(&DataType::Utf8, array.len()),
            )
        } else {
            (
                new_null_array(&DataType::LargeBinary, array.len()),
                cast(array, &DataType::Utf8)?,
            )
        };
        return Ok(Arc::new(StructArray::try_new(
            children.clone(),
            vec![data, uri],
            array.nulls().cloned(),
        )?));
    }
    let children: Vec<FieldRef> = match target.data_type() {
        DataType::Struct(fields) => fields.to_vec(),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            vec![field.clone()]
        }
        _ => {
            return Err(ArrowError::SchemaError(format!(
                "unsupported blob container {}",
                target.data_type()
            )))
        }
    };
    let data = array.to_data();
    if data.child_data().len() != children.len() {
        return Err(ArrowError::SchemaError(
            "blob container child count mismatch".into(),
        ));
    }
    let transformed = data
        .child_data()
        .iter()
        .zip(children.iter())
        .map(|(child, field)| {
            transform_array(&make_array(child.clone()), field).map(|array| array.to_data())
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(make_array(
        data.into_builder()
            .data_type(target.data_type().clone())
            .child_data(transformed)
            .build()?,
    ))
}

fn configure_blob_storage(
    stream: SendableRecordBatchStream,
    options: &WriteOptions,
) -> Result<SendableRecordBatchStream> {
    let promote_binary = options.blob_inline_size_threshold.is_some()
        || options.blob_dedicated_size_threshold.is_some()
        || !options.blob_columns.is_empty();
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
    let fields = input_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if uri_blob_columns.binary_search(&index).is_ok() {
                let blob = blob_field_with_options(
                    field.name(),
                    field.is_nullable(),
                    blob_options.clone(),
                );
                let mut metadata = field.metadata().clone();
                metadata.extend(blob.metadata().clone());
                Arc::new(blob.with_metadata(metadata))
            } else {
                blob_field(field, promote_binary.then_some(&blob_options))
            }
        })
        .collect::<Vec<_>>();
    let output_schema = Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ));
    if output_schema == input_schema {
        return Ok(stream);
    }
    let batch_schema = output_schema.clone();
    let batches = stream.map(move |batch| {
        let batch = batch?;
        let columns = batch
            .columns()
            .iter()
            .zip(batch_schema.fields().iter())
            .map(|(array, field)| transform_array(array, field))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
        Ok(RecordBatch::try_new(batch_schema.clone(), columns)?)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        batches,
    )))
}

fn ensure_uri_column(data_type: &DataType, name: &str) -> Result<()> {
    if is_string(data_type) {
        Ok(())
    } else {
        Err(Error::message(format!(
            "BLOB_COLUMNS column must be a string: {name}"
        )))
    }
}
