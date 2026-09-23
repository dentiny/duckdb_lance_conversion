use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_cast::cast;
use arrow_schema::DataType;
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use lance::{io::RecordBatchStream, Dataset};

pub fn fixture(rows: usize) -> RecordBatch {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from_iter((0..rows).map(|i| Some(i as i64)))) as ArrayRef,
        ),
        (
            "text",
            Arc::new(StringArray::from_iter((0..rows).map(|i| {
                if i % 7 == 0 {
                    None
                } else {
                    Some(format!("row-{i}-你好"))
                }
            }))) as ArrayRef,
        ),
        (
            "flag",
            Arc::new(BooleanArray::from_iter((0..rows).map(|i| {
                if i % 3 == 0 {
                    None
                } else {
                    Some(i % 2 == 0)
                }
            }))) as ArrayRef,
        ),
        (
            "body",
            Arc::new(BinaryArray::from_iter((0..rows).map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some(vec![0, 255, (i % 256) as u8])
                }
            }))) as ArrayRef,
        ),
    ])
    .unwrap();
    let schema = arrow_schema::Schema::new(
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    );
    RecordBatch::try_new(Arc::new(schema), batch.columns().to_vec()).unwrap()
}

pub async fn read_lance(path: &Path) -> RecordBatch {
    let dataset = Arc::new(Dataset::open(path.to_str().unwrap()).await.unwrap());
    let mut scanner = dataset.scan();
    scanner.scan_in_order(true);
    let stream = scanner.try_into_stream().await.unwrap();
    let schema = stream.schema();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let batch = concat_batches(&schema, &batches).unwrap();
    let logical_schema = arrow_schema::Schema::from(dataset.schema());
    let blob_columns = logical_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            field
                .metadata()
                .get("ARROW:extension:name")
                .is_some_and(|name| name == "lance.blob.v2")
        })
        .map(|(index, field)| (index, field.name().clone()))
        .collect::<Vec<_>>();
    if blob_columns.is_empty() {
        return batch;
    }

    let row_indices = (0..batch.num_rows() as u64).collect::<Vec<_>>();
    let mut fields = schema.fields().to_vec();
    let mut columns = batch.columns().to_vec();
    for (index, name) in blob_columns {
        let blobs = dataset
            .take_blobs_by_indices(&row_indices, &name)
            .await
            .unwrap();
        let mut values = Vec::with_capacity(blobs.len());
        for blob in blobs {
            values.push(match blob {
                Some(blob) => Some(blob.read().await.unwrap().to_vec()),
                None => None,
            });
        }
        fields[index] = Arc::new(arrow_schema::Field::new(name, DataType::Binary, true));
        columns[index] = Arc::new(BinaryArray::from_iter(
            values.iter().map(|value| value.as_deref()),
        ));
    }
    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).unwrap()
}

pub fn assert_values(actual: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(actual.num_rows(), expected.num_rows());
    assert_eq!(actual.num_columns(), expected.num_columns());
    for i in 0..expected.num_columns() {
        assert_eq!(
            actual.schema().field(i).name(),
            expected.schema().field(i).name()
        );
        let actual = if expected.schema().field(i).data_type() == &DataType::Binary
            && actual.schema().field(i).data_type() != &DataType::Binary
        {
            cast(actual.column(i), &DataType::Binary).unwrap()
        } else {
            actual.column(i).clone()
        };
        assert_eq!(actual.to_data(), expected.column(i).to_data(), "column {i}");
    }
}
