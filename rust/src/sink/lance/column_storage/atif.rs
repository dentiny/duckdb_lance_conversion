//! ATIF-specific field recognition and Lance storage defaults.

use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef};
use lance::BlobFieldOptions;

use super::{blob_field, is_binary, is_blob};
use crate::atif::{StoragePolicy, SOURCE_TYPE, STORAGE_POLICY};
use crate::{Error, Result};

/// Field metadata describes storage, never the content type of an individual item.
pub(super) fn storage_policy(field: &Field) -> Result<Option<StoragePolicy>> {
    if !field
        .metadata()
        .get(SOURCE_TYPE)
        .is_some_and(|source| source == "atif")
    {
        return Ok(None);
    }
    match field.metadata().get(STORAGE_POLICY).map(String::as_str) {
        Some("text") => Ok(Some(StoragePolicy::Text)),
        Some("blob") => Ok(Some(StoragePolicy::Blob)),
        None => Ok(None),
        Some(policy) => Err(Error::message(format!(
            "invalid ATIF storage policy: {policy}"
        ))),
    }
}

fn atif_text(field: &FieldRef) -> FieldRef {
    let mut field = field.as_ref().clone().with_data_type(DataType::LargeBinary);
    field
        .metadata_mut()
        .entry("lance-encoding:compression".into())
        .or_insert_with(|| "lz4".into());
    Arc::new(field)
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
        match storage_policy(field)? {
            Some(StoragePolicy::Text) => return Ok(atif_text(field)),
            Some(StoragePolicy::Blob) => {
                return Ok(
                    options.map_or_else(|| field.clone(), |options| blob_field(field, options))
                );
            }
            None => {}
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
