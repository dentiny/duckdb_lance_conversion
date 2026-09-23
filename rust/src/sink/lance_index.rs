use arrow_schema::{DataType, SchemaRef};
use lance::{
    index::{vector::VectorIndexParams, DatasetIndexExt},
    Dataset,
};
use lance_index::{
    scalar::{BuiltinIndexType, InvertedIndexParams, ScalarIndexParams},
    IndexType,
};
use lance_linalg::distance::MetricType;

use super::lance::WriteOptions;
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
        append_indexes(
            schema,
            &options.scalar_index_columns,
            IndexKind::BTree,
            &mut indexes,
        )?;
        append_indexes(
            schema,
            &options.vector_index_columns,
            IndexKind::Vector,
            &mut indexes,
        )?;
        append_indexes(
            schema,
            &options.text_index_columns,
            IndexKind::Text,
            &mut indexes,
        )?;
        append_indexes(
            schema,
            &options.bloom_filter_index_columns,
            IndexKind::BloomFilter,
            &mut indexes,
        )?;
        indexes.sort_by(|left, right| left.0.cmp(&right.0));
        if indexes
            .windows(2)
            .any(|indexes| indexes[0].0 == indexes[1].0)
        {
            return Err(Error::invalid_argument(
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
            Some(
                dataset
                    .count_rows(None)
                    .await?
                    .div_ceil(TARGET_VECTOR_PARTITION_ROWS)
                    .clamp(1, 4096),
            )
        } else {
            None
        };

        for (column, kind) in &self.indexes {
            match kind {
                IndexKind::BTree => {
                    let params = ScalarIndexParams::for_builtin(BuiltinIndexType::BTree);
                    dataset
                        .create_index(
                            &[column.as_str()],
                            IndexType::BTree,
                            /*name=*/ None,
                            &params,
                            /*replace=*/ true,
                        )
                        .await
                        .context(format!("creating BTree index on '{column}'"))?;
                }
                IndexKind::Vector => {
                    let params = VectorIndexParams::ivf_flat(
                        vector_partitions.expect("vector partition count must exist"),
                        MetricType::L2,
                    );
                    dataset
                        .create_index(
                            &[column.as_str()],
                            IndexType::IvfFlat,
                            /*name=*/ None,
                            &params,
                            /*replace=*/ true,
                        )
                        .await
                        .context(format!("creating vector index on '{column}'"))?;
                }
                IndexKind::Text => {
                    dataset
                        .create_index(
                            &[column.as_str()],
                            IndexType::Inverted,
                            /*name=*/ None,
                            &InvertedIndexParams::default(),
                            /*replace=*/ true,
                        )
                        .await
                        .context(format!("creating text index on '{column}'"))?;
                }
                IndexKind::BloomFilter => {
                    let params = ScalarIndexParams::for_builtin(BuiltinIndexType::BloomFilter);
                    dataset
                        .create_index(
                            &[column.as_str()],
                            IndexType::BloomFilter,
                            /*name=*/ None,
                            &params,
                            /*replace=*/ true,
                        )
                        .await
                        .context(format!("creating Bloom filter index on '{column}'"))?;
                }
            }
        }
        Ok(())
    }
}

fn append_indexes(
    schema: &SchemaRef,
    columns: &[String],
    kind: IndexKind,
    indexes: &mut Vec<(String, IndexKind)>,
) -> Result<()> {
    for column in columns {
        let field = schema
            .field_with_name(column)
            .map_err(|_| Error::invalid_argument(format!("index column not found: {column}")))?;
        validate_index_type(field.data_type(), column, kind)?;
        indexes.push((column.clone(), kind));
    }
    Ok(())
}

fn validate_index_type(data_type: &DataType, column: &str, kind: IndexKind) -> Result<()> {
    let supported = match kind {
        IndexKind::BTree => supports_scalar_index(data_type),
        IndexKind::Vector => supports_vector_index(data_type),
        IndexKind::Text => matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ),
        IndexKind::BloomFilter => supports_bloom_filter_index(data_type),
    };
    if supported {
        Ok(())
    } else {
        Err(Error::invalid_argument(format!(
            "cannot create the requested index on column '{column}' with type {data_type}"
        )))
    }
}

fn supports_scalar_index(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
    )
}

fn supports_vector_index(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::FixedSizeList(field, _)
            if matches!(
                field.data_type(),
                DataType::Int8
                    | DataType::UInt8
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
            )
    )
}

fn supports_bloom_filter_index(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
    )
}
