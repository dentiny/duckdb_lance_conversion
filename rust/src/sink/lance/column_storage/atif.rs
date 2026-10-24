//! ATIF-specific field recognition and Lance storage defaults.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use lance::BlobFieldOptions;

use super::{blob_field, is_binary, is_blob};
use crate::atif::{CONTENT_TYPE, SOURCE_TYPE};

fn has_role(field: &Field, role: &str) -> bool {
    field
        .metadata()
        .get(SOURCE_TYPE)
        .is_some_and(|source| source == "atif")
        && field
            .metadata()
            .get(CONTENT_TYPE)
            .is_some_and(|content| content == role)
}

pub(super) fn is_text(field: &Field) -> bool {
    has_role(field, "text")
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
    if is_text(field) && is_binary(field.data_type()) {
        return atif_text(field);
    }
    if has_role(field, "media") && is_binary(field.data_type()) {
        return options.map_or_else(|| field.clone(), |options| blob_field(field, options));
    }
    let data_type = match field.data_type() {
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
