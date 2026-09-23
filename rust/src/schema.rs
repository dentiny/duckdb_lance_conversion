//! Schema and type validation for the Lance sink.

use arrow_schema::{DataType, SchemaRef};

use crate::{Error, Result};

pub(crate) fn validate_schema(schema: &SchemaRef) -> Result<()> {
    if schema.fields().is_empty() {
        return Err(Error::message("Lance requires at least one column"));
    }
    for field in schema.fields() {
        validate_type(field.data_type())?;
    }
    Ok(())
}

fn validate_type(data_type: &DataType) -> Result<()> {
    match data_type {
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
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal128(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _) => Ok(()),
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            validate_type(field.data_type())
        }
        DataType::Struct(fields) => {
            for field in fields {
                validate_type(field.data_type())?;
            }
            Ok(())
        }
        _ => Err(Error::message(format!(
            "unsupported Arrow type {data_type}; cast it explicitly"
        ))),
    }
}
