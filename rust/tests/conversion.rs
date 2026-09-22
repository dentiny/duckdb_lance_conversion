mod common;

use lance_conversion::{convert, LanceSink, ParquetFileSource, WriteOptions};
use parquet::arrow::async_writer::AsyncArrowWriter;
use tempfile::tempdir;
use tokio::{fs::File, task::spawn_blocking};

use common::{assert_values, fixture, read_lance};

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
        &output,
        WriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(result.rows_written, 10_001);
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
        &output,
        WriteOptions::default(),
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
        let mut writer = LanceSink::create(&output, batch.schema(), WriteOptions { overwrite })
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
