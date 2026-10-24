//! ATIF-specific field recognition and Lance storage defaults.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use lance::BlobFieldOptions;

use super::{blob_field, is_binary, is_blob};
use crate::atif::{ContentType, CONTENT_TYPES, SOURCE_TYPE};
use crate::{Error, Result};

/// A field describes allowed variants; image/audio share ATIF's source.path.
/// Individual ContentPart.type values retain the concrete variant per content item.
pub(super) fn content_types(field: &Field) -> Result<Vec<ContentType>> {
    if !field
        .metadata()
        .get(SOURCE_TYPE)
        .is_some_and(|source| source == "atif")
    {
        return Ok(Vec::new());
    }
    match field.metadata().get(CONTENT_TYPES) {
        Some(types) => serde_json::from_str(types)
            .map_err(|error| Error::message(format!("invalid ATIF content types: {error}"))),
        None => Ok(Vec::new()),
    }
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
pub(super) fn configure_field(
    field: &FieldRef,
    options: Option<&BlobFieldOptions>,
) -> Result<FieldRef> {
    if is_blob(field) {
        return Ok(field.clone());
    }
    if is_binary(field.data_type()) {
        let types = content_types(field)?;
        if types.contains(&ContentType::Text) {
            return Ok(atif_text(field));
        }
        if types
            .iter()
            .any(|kind| matches!(kind, ContentType::Image | ContentType::Audio))
        {
            return Ok(options.map_or_else(|| field.clone(), |options| blob_field(field, options)));
        }
    }
    let data_type = match field.data_type() {
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|field| configure_field(field, options))
                .collect::<Result<_>>()?,
        ),
        DataType::List(child) => DataType::List(configure_field(child, options)?),
        DataType::LargeList(child) => DataType::LargeList(configure_field(child, options)?),
        _ => return Ok(field.clone()),
    };
    Ok(Arc::new(field.as_ref().clone().with_data_type(data_type)))
}
