use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{make_array, new_null_array, Array, ArrayRef, RecordBatch, StructArray};
use arrow_cast::cast;
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Schema, SchemaRef};
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
    let input_schema = stream.schema();
    let schema = blob_schema(&input_schema, options)?;
    let schema = configure_column_encoding(schema, &options.column_encoding)?;
    if schema == input_schema {
        return Ok(stream);
    }
    let output = schema.clone();
    let batches = stream.map(move |batch| {
        let batch = batch?;
        let columns = batch
            .columns()
            .iter()
            .zip(output.fields())
            .map(|(array, field)| transform_array(array, field))
            .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
        Ok(RecordBatch::try_new(output.clone(), columns)?)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, batches)))
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
    let [kind, text, source] = fields.as_ref() else {
        return false;
    };
    let DataType::Struct(source) = source.data_type() else {
        return false;
    };
    let [mime, path, duration] = source.as_ref() else {
        return false;
    };
    kind.name() == "type"
        && is_string(kind.data_type())
        && text.name() == "text"
        && is_binary(text.data_type())
        && fields[2].name() == "source"
        && mime.name() == "media_type"
        && is_string(mime.data_type())
        && path.name() == "path"
        && (is_binary(path.data_type()) || is_string(path.data_type()) || is_blob(path))
        && duration.name() == "duration_sec"
        && duration.data_type() == &DataType::Float64
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

fn blob_field(field: &FieldRef, options: &BlobFieldOptions) -> FieldRef {
    let blob = blob_field_with_options(field.name(), field.is_nullable(), options.clone());
    let mut metadata = field.metadata().clone();
    metadata.extend(blob.metadata().clone());
    Arc::new(blob.with_metadata(metadata))
}

/// Apply ATIF defaults inside structs/lists. Other nested binary fields retain
/// their storage policy; top-level binary and explicit URI blobs are handled separately.
fn atif_field(field: &FieldRef, options: Option<&BlobFieldOptions>) -> FieldRef {
    if is_blob(field) {
        return field.clone();
    }
    let data_type = match field.data_type() {
        DataType::Struct(fields) if is_atif_content(fields) => {
            let mut fields = fields.to_vec();
            fields[1] = atif_text(&fields[1]);
            if let (Some(options), DataType::Struct(source)) = (options, fields[2].data_type()) {
                if is_binary(source[1].data_type()) {
                    let mut source = source.to_vec();
                    source[1] = blob_field(&source[1], options);
                    fields[2] = Arc::new(
                        fields[2]
                            .as_ref()
                            .clone()
                            .with_data_type(DataType::Struct(source.into())),
                    );
                }
            }
            DataType::Struct(fields.into())
        }
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| atif_field(field, options))
                .collect(),
        ),
        DataType::List(child) => DataType::List(atif_field(child, options)),
        DataType::LargeList(child) => DataType::LargeList(atif_field(child, options)),
        _ => return field.clone(),
    };
    Arc::new(field.as_ref().clone().with_data_type(data_type))
}

fn transform_array(array: &ArrayRef, target: &Field) -> std::result::Result<ArrayRef, ArrowError> {
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
                "unsupported Arrow container {}",
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

fn blob_schema(input_schema: &SchemaRef, options: &WriteOptions) -> Result<SchemaRef> {
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
            if uri_blob_columns.binary_search(&index).is_ok()
                || (promote_binary && is_binary(field.data_type()))
            {
                blob_field(field, &blob_options)
            } else {
                atif_field(field, promote_binary.then_some(&blob_options))
            }
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_nested_binary_retains_its_storage_policy() {
        let payload = Arc::new(Field::new("payload", DataType::Binary, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "record",
                DataType::Struct(vec![payload.clone()].into()),
                true,
            ),
            Field::new("list", DataType::List(payload.clone()), true),
            Field::new("large_list", DataType::LargeList(payload), true),
        ]));
        assert_eq!(
            blob_schema(&schema, &WriteOptions::default()).unwrap(),
            schema
        );
    }
}
