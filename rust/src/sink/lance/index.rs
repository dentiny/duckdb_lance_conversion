use arrow_schema::{DataType, SchemaRef};
use lance::{
    index::{vector::VectorIndexParams, DatasetIndexExt},
    Dataset,
};
use lance_index::{
    scalar::{BuiltinIndexType, InvertedIndexParams, ScalarIndexParams},
    IndexParams, IndexType,
};
use lance_linalg::distance::MetricType;

use super::WriteOptions;
use crate::error::ResultExt;
use crate::{Error, Result};

const TARGET_VECTOR_PARTITION_ROWS: usize = 8192;

#[derive(Clone, Copy, PartialEq, Eq)]
enum IndexKind {
    BTree,
    Vector,
    Text,
    BloomFilter,
}

pub(super) struct LanceIndexPlan {
    indexes: Vec<(String, IndexKind)>,
}

impl LanceIndexPlan {
    pub(super) fn new(schema: &SchemaRef, options: &WriteOptions) -> Result<Self> {
        let mut indexes = Vec::new();
        for (columns, kind) in [
            (&options.scalar_index_columns, IndexKind::BTree),
            (&options.vector_index_columns, IndexKind::Vector),
            (&options.text_index_columns, IndexKind::Text),
            (&options.bloom_filter_index_columns, IndexKind::BloomFilter),
        ] {
            for column in columns {
                let field = schema
                    .field_with_name(column)
                    .map_err(|_| Error::message(format!("index column not found: {column}")))?;
                validate_index_type(field.data_type(), column, kind)?;
                indexes.push((column.clone(), kind));
            }
        }
        indexes.sort_by(|left, right| left.0.cmp(&right.0));
        if indexes
            .windows(2)
            .any(|indexes| indexes[0].0 == indexes[1].0)
        {
            return Err(Error::message(
                "a column cannot have multiple indexes in one write",
            ));
        }
        Ok(Self { indexes })
    }

    pub(super) async fn create(&self, dataset: &mut Dataset) -> Result<()> {
        let vector_partitions = if self
            .indexes
            .iter()
            .any(|(_, kind)| *kind == IndexKind::Vector)
        {
            dataset
                .count_rows(None)
                .await?
                .div_ceil(TARGET_VECTOR_PARTITION_ROWS)
                .clamp(1, 4096)
        } else {
            0
        };

        for (column, kind) in &self.indexes {
            let (index_type, params, label): (_, Box<dyn IndexParams>, _) = match kind {
                IndexKind::BTree => (
                    IndexType::BTree,
                    Box::new(ScalarIndexParams::for_builtin(BuiltinIndexType::BTree)),
                    "BTree",
                ),
                IndexKind::Vector => (
                    IndexType::IvfFlat,
                    Box::new(VectorIndexParams::ivf_flat(
                        vector_partitions,
                        MetricType::L2,
                    )),
                    "vector",
                ),
                IndexKind::Text => (
                    IndexType::Inverted,
                    Box::new(InvertedIndexParams::default()),
                    "text",
                ),
                IndexKind::BloomFilter => (
                    IndexType::BloomFilter,
                    Box::new(ScalarIndexParams::for_builtin(
                        BuiltinIndexType::BloomFilter,
                    )),
                    "Bloom filter",
                ),
            };
            dataset
                .create_index(
                    &[column.as_str()],
                    index_type,
                    /*name=*/ None,
                    params.as_ref(),
                    /*replace=*/ true,
                )
                .await
                .context(format!("creating {label} index on '{column}'"))?;
        }
        Ok(())
    }
}

fn validate_index_type(data_type: &DataType, column: &str, kind: IndexKind) -> Result<()> {
    use DataType::*;
    let supported = match kind {
        IndexKind::BTree => matches!(
            data_type,
            Boolean
                | Int8
                | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float16
                | Float32
                | Float64
                | Decimal128(_, _)
                | Utf8
                | LargeUtf8
                | Utf8View
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
        ),
        IndexKind::Vector => matches!(
            data_type,
            FixedSizeList(field, _)
                if matches!(field.data_type(), Int8 | UInt8 | Float16 | Float32 | Float64)
        ),
        IndexKind::Text => matches!(data_type, Utf8 | LargeUtf8 | Utf8View),
        IndexKind::BloomFilter => matches!(
            data_type,
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float32
                | Float64
                | Utf8
                | LargeUtf8
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
        ),
    };
    if supported {
        Ok(())
    } else {
        Err(Error::message(format!(
            "cannot create the requested index on column '{column}' with type {data_type}"
        )))
    }
}
