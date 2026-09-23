mod common;

use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use lance_conversion::{
    convert, LanceSink, LanceWriter, ParquetFileSource, WriteMode, WriteOptions,
};
use parquet::arrow::async_writer::AsyncArrowWriter;
use tempfile::tempdir;
use tokio::{
    fs::{self, File},
    task::spawn_blocking,
};

use common::{assert_values, fixture, read_lance};

async fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).await.unwrap();
    let mut writer = AsyncArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(batch).await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn single_parquet_streams_multiple_batches_into_lance() {
    let temp = spawn_blocking(tempdir).await.unwrap().unwrap();
    let input = temp.path().join("input.parquet");
    let output = temp.path().join("output.lance");
    let expected = fixture(10_001);
    let file = File::create(&input).await.unwrap();
    let mut writer = AsyncArrowWriter::try_new(file, expected.schema(), None).unwrap();
    writer.write(&expected).await.unwrap();
    writer.close().await.unwrap();
    let result = convert(
        ParquetFileSource::new(input).with_batch_size(257),
        LanceSink::new(&output, WriteOptions::default()),
    )
    .await
    .unwrap();
    assert_eq!(result.rows_written, 10_001);
    assert_values(&read_lance(&output).await, &expected);
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}

#[tokio::test]
async fn parquet_directory_streams_files_recursively() {
    let temp = spawn_blocking(tempdir).await.unwrap().unwrap();
    let input = temp.path().join("input");
    let nested = input.join("nested");
    let output = temp.path().join("output.lance");
    fs::create_dir_all(&nested).await.unwrap();

    let first = fixture(11);
    let second = fixture(7);
    write_parquet(&input.join("a.parquet"), &first).await;
    write_parquet(&nested.join("b.PARQUET"), &second).await;
    fs::write(input.join("ignored.txt"), b"not parquet")
        .await
        .unwrap();

    let expected = concat_batches(&first.schema(), [&first, &second]).unwrap();
    let result = convert(
        ParquetFileSource::new(input).with_batch_size(3),
        LanceSink::new(&output, WriteOptions::default()),
    )
    .await
    .unwrap();

    assert_eq!(result.rows_written, 18);
    assert_values(&read_lance(&output).await, &expected);
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}

#[tokio::test]
async fn empty_parquet_preserves_schema() {
    let temp = spawn_blocking(tempdir).await.unwrap().unwrap();
    let input = temp.path().join("empty.parquet");
    let output = temp.path().join("empty.lance");
    let expected = fixture(0);
    let file = File::create(&input).await.unwrap();
    AsyncArrowWriter::try_new(file, expected.schema(), None)
        .unwrap()
        .close()
        .await
        .unwrap();
    let result = convert(
        ParquetFileSource::new(input),
        LanceSink::new(&output, WriteOptions::default()),
    )
    .await
    .unwrap();
    assert_eq!(result.rows_written, 0);
    assert_values(&read_lance(&output).await, &expected);
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}

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
