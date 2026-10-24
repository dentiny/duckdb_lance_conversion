//! ATIF-specific field recognition and Lance storage defaults.

use std::sync::Arc;

use arrow_schema::{DataType, FieldRef};
use lance::BlobFieldOptions;

use super::{blob_field, is_binary, is_blob, is_string};

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

/// Apply ATIF defaults inside structs/lists. Other nested binary fields retain
/// their storage policy; top-level binary and explicit URI blobs are handled separately.
pub(super) fn configure_field(field: &FieldRef, options: Option<&BlobFieldOptions>) -> FieldRef {
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
                .map(|field| configure_field(field, options))
                .collect(),
        ),
        DataType::List(child) => DataType::List(configure_field(child, options)),
        DataType::LargeList(child) => DataType::LargeList(configure_field(child, options)),
        _ => return field.clone(),
    };
    Arc::new(field.as_ref().clone().with_data_type(data_type))
}
