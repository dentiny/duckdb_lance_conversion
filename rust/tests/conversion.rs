mod common;

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use lance::index::DatasetIndexExt;
use lance_conversion::{LanceWriter, WriteMode, WriteOptions};
use tempfile::tempdir;
use tokio::{fs, task::spawn_blocking};

use common::{assert_values, fixture, read_lance};

#[tokio::test]
async fn overwrite_replaces_rows_and_can_commit_an_empty_dataset() {
    let temp = spawn_blocking(tempdir).await.unwrap().unwrap();
    let output = temp.path().join("overwrite.lance");
    for (rows, overwrite) in [(10, false), (3, true), (0, true)] {
        let batch = fixture(rows);
        let mut writer = LanceWriter::create(
            &output,
            batch.schema(),
            WriteOptions {
                mode: if overwrite {
                    WriteMode::Overwrite
                } else {
                    WriteMode::Create
                },
                ..Default::default()
            },
        )
        .await
        .unwrap();
        if rows != 0 {
            writer.write_batch(batch.clone()).await.unwrap();
        }
        assert_eq!(writer.finish().await.unwrap().rows_written, rows as u64);
        assert_values(&read_lance(&output).await, &batch);
    }
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}

#[tokio::test]
async fn string_uri_column_is_ingested_as_a_blob() {
    let temp = spawn_blocking(tempdir).await.unwrap().unwrap();
    let first = temp.path().join("first.bin");
    let empty = temp.path().join("empty.bin");
    let output = temp.path().join("uris.lance");
    fs::write(&first, b"first").await.unwrap();
    fs::write(&empty, b"").await.unwrap();

    let uris = [
        Some(url::Url::from_file_path(&first).unwrap().to_string()),
        Some(url::Url::from_file_path(&empty).unwrap().to_string()),
        None,
    ];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("uri", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 2])) as ArrayRef,
            Arc::new(StringArray::from_iter(
                uris.iter().map(|value| value.as_deref()),
            )) as ArrayRef,
        ],
    )
    .unwrap();
    let expected = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("uri", DataType::Binary, true),
        ])),
        vec![
            batch.column(0).clone(),
            Arc::new(BinaryArray::from(vec![
                Some(b"first".as_slice()),
                Some(b"".as_slice()),
                None,
            ])),
        ],
    )
    .unwrap();
    let mut writer = LanceWriter::create(
        &output,
        batch.schema(),
        WriteOptions {
            blob_columns: vec!["uri".into()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    writer.write_batch(batch).await.unwrap();
    writer.finish().await.unwrap();
    assert_values(&read_lance(&output).await, &expected);
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}

#[tokio::test]
async fn index_types_are_inferred_from_the_schema() {
    let temp = spawn_blocking(tempdir).await.unwrap().unwrap();
    let output = temp.path().join("indexed.lance");
    let rows = 32;
    let vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        4,
        Arc::new(Float32Array::from_iter(
            (0..rows * 4).map(|value| Some(value as f32)),
        )),
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
        ),
        (
            "name",
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|row| format!("row-{row}")),
            )) as ArrayRef,
        ),
        (
            "category",
            Arc::new(StringArray::from_iter_values(
                (0..rows).map(|row| format!("category-{}", row % 4)),
            )) as ArrayRef,
        ),
        ("embedding", Arc::new(vectors) as ArrayRef),
    ])
    .unwrap();
    let mut writer = LanceWriter::create(
        &output,
        batch.schema(),
        WriteOptions {
            scalar_index_columns: vec!["id".into()],
            vector_index_columns: vec!["embedding".into()],
            text_index_columns: vec!["name".into()],
            bloom_filter_index_columns: vec!["category".into()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    writer.write_batch(batch).await.unwrap();
    writer.finish().await.unwrap();

    let dataset = lance::Dataset::open(output.to_str().unwrap())
        .await
        .unwrap();
    let mut names = dataset
        .load_indices()
        .await
        .unwrap()
        .iter()
        .map(|index| index.name.clone())
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(
        names,
        ["category_idx", "embedding_idx", "id_idx", "name_idx"]
    );
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}
