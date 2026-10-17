use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::Schema;
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

/// Sets each column's encoding settings as Lance field metadata.
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
        let index = input_schema
            .index_of(name)
            .map_err(|_| Error::message(format!("column encoding column not found: {name}")))?;
        let mut metadata = fields[index].metadata().clone();
        metadata.extend(encoding.metadata().map(|(key, value)| (key.into(), value)));
        fields[index] = Arc::new(fields[index].as_ref().clone().with_metadata(metadata));
    }
    let output_schema = Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ));
    let batch_schema = output_schema.clone();
    let batches = stream.map(move |batch| Ok(batch?.with_schema(batch_schema.clone())?));
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        output_schema,
        batches,
    )))
}
