use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use lance::Dataset;

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

pub fn read_lance(path: &Path) -> RecordBatch {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
        let mut scanner = dataset.scan();
        scanner.scan_in_order(true);
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let schema = Arc::new(arrow_schema::Schema::from(dataset.schema()));
        concat_batches(&schema, &batches).unwrap()
    })
}

pub fn assert_values(actual: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(actual.num_rows(), expected.num_rows());
    assert_eq!(actual.num_columns(), expected.num_columns());
    for i in 0..expected.num_columns() {
        assert_eq!(
            actual.schema().field(i).name(),
            expected.schema().field(i).name()
        );
        assert_eq!(
            actual.column(i).to_data(),
            expected.column(i).to_data(),
            "column {i}"
        );
    }
}
