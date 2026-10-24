use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, FieldRef, Schema};

use super::column_storage::transform_array;
use datafusion_physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream};
use futures::StreamExt;

use crate::{Error, Result};

const COMPRESSION_META_KEY: &str = "lance-encoding:compression";
const COMPRESSION_LEVEL_META_KEY: &str = "lance-encoding:compression-level";
const STRUCTURAL_ENCODING_META_KEY: &str = "lance-encoding:structural-encoding";

/// How Lance lays out a column's values within a page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StructuralEncoding {
    MiniBlock,
    FullZip,
}

impl StructuralEncoding {
    fn as_str(self) -> &'static str {
        match self {
            Self::MiniBlock => "miniblock",
            Self::FullZip => "fullzip",
        }
    }
}

/// Lance encoding settings for one column; unset fields keep Lance's defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ColumnEncoding {
    /// Set as `lance-encoding:compression` field metadata.
    pub compression: Option<String>,
    /// Set as `lance-encoding:compression-level` field metadata.
    pub compression_level: Option<i32>,
    /// Set as `lance-encoding:structural-encoding` field metadata.
    pub structural_encoding: Option<StructuralEncoding>,
}

impl ColumnEncoding {
    fn metadata(&self) -> impl Iterator<Item = (&'static str, String)> {
        [
            (COMPRESSION_META_KEY, self.compression.clone()),
            (
                COMPRESSION_LEVEL_META_KEY,
                self.compression_level.map(|level| level.to_string()),
            ),
            (
                STRUCTURAL_ENCODING_META_KEY,
                self.structural_encoding
                    .map(|encoding| encoding.as_str().to_owned()),
            ),
        ]
        .into_iter()
        .filter_map(|(key, value)| Some((key, value?)))
    }
}

/// Apply to a named field, crossing list containers without exposing Arrow's
/// implementation-specific list item names in the SQL path.
fn encode_field(field: &FieldRef, path: &[&str], encoding: &ColumnEncoding) -> Result<FieldRef> {
    if path.is_empty() {
        let mut metadata = field.metadata().clone();
        metadata.extend(encoding.metadata().map(|(key, value)| (key.into(), value)));
        return Ok(Arc::new(field.as_ref().clone().with_metadata(metadata)));
    }
    let data_type = match field.data_type() {
        DataType::Struct(fields) => {
            let mut fields = fields.to_vec();
            let index = fields
                .iter()
                .position(|field| field.name() == path[0])
                .ok_or_else(|| {
                    Error::message(format!("column encoding field not found: {}", path[0]))
                })?;
            fields[index] = encode_field(&fields[index], &path[1..], encoding)?;
            DataType::Struct(fields.into())
        }
        DataType::List(item) => DataType::List(encode_field(item, path, encoding)?),
        DataType::LargeList(item) => DataType::LargeList(encode_field(item, path, encoding)?),
        DataType::FixedSizeList(item, size) => {
            DataType::FixedSizeList(encode_field(item, path, encoding)?, *size)
        }
        _ => {
            return Err(Error::message(format!(
                "column encoding field not found: {}",
                path[0]
            )))
        }
    };
    Ok(Arc::new(field.as_ref().clone().with_data_type(data_type)))
}

/// Explicit settings override source metadata and ATIF defaults. Dot paths name
/// nested fields; existing exact top-level column names keep precedence.
pub(super) fn configure_column_encoding(
    stream: SendableRecordBatchStream,
    column_encoding: &BTreeMap<String, ColumnEncoding>,
) -> Result<SendableRecordBatchStream> {
    if column_encoding.is_empty() {
        return Ok(stream);
    }
    let input_schema = stream.schema();
    let mut fields = input_schema.fields().to_vec();
    for (name, encoding) in column_encoding {
        let (index, path) = if let Ok(index) = input_schema.index_of(name) {
            (index, Vec::new())
        } else {
            let parts = name.split('.').collect::<Vec<_>>();
            let index = input_schema
                .index_of(parts[0])
                .map_err(|_| Error::message(format!("column encoding column not found: {name}")))?;
            (index, parts[1..].to_vec())
        };
        if !path.is_empty() && encoding.structural_encoding.is_some() {
            return Err(Error::message(
                "COLUMN_STRUCTURAL_ENCODING applies to top-level columns, not nested fields",
            ));
        }
        fields[index] = encode_field(&fields[index], &path, encoding)?;
    }
    let output_schema = Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ));
    let batch_schema = output_schema.clone();
    let batches = stream.map(move |batch| {
        let batch = batch?;
        let columns = batch
            .columns()
            .iter()
            .zip(batch_schema.fields())
            .map(|(array, field)| transform_array(array, field))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(RecordBatch::try_new(batch_schema.clone(), columns)?)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        batches,
    )))
}
