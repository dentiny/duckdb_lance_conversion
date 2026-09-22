mod common;

use std::fs::File;

use lance_conversion::{convert, LanceSink, ParquetFileSource, WriteOptions};
use parquet::arrow::ArrowWriter;
use tempfile::tempdir;

use common::{assert_values, fixture, read_lance};

use tokio::task::spawn_blocking;

#[tokio::test]
async fn single_parquet_streams_multiple_batches_into_lance() {
    let (_temp, output, expected) = spawn_blocking(|| {
        let temp = tempdir().unwrap();
        let input = temp.path().join("input.parquet");
        let output = temp.path().join("output.lance");
        let expected = fixture(10_001);
        let mut writer =
            ArrowWriter::try_new(File::create(&input).unwrap(), expected.schema(), None).unwrap();
        writer.write(&expected).unwrap();
        writer.close().unwrap();
        let result = convert(
            ParquetFileSource::new(input).with_batch_size(257),
            &output,
            WriteOptions::default(),
        )
        .unwrap();
        assert_eq!(result.rows_written, 10_001);

        (temp, output, expected)
    })
    .await
    .unwrap();
    assert_values(&read_lance(&output).await, &expected);
}

#[tokio::test]
async fn empty_parquet_preserves_schema() {
    let (_temp, output, expected) = spawn_blocking(|| {
        let temp = tempdir().unwrap();
        let input = temp.path().join("empty.parquet");
        let output = temp.path().join("empty.lance");
        let expected = fixture(0);
        ArrowWriter::try_new(File::create(&input).unwrap(), expected.schema(), None)
            .unwrap()
            .close()
            .unwrap();
        assert_eq!(
            convert(
                ParquetFileSource::new(input),
                &output,
                WriteOptions::default()
            )
            .unwrap()
            .rows_written,
            0
        );

        (temp, output, expected)
    })
    .await
    .unwrap();
    assert_values(&read_lance(&output).await, &expected);
}

#[tokio::test]
async fn overwrite_replaces_rows_and_can_commit_an_empty_dataset() {
    let temp = tempdir().unwrap();
    let output = temp.path().join("overwrite.lance");
    for (rows, overwrite) in [(10, false), (3, true), (0, true)] {
        let batch = fixture(rows);
        let writer_output = output.clone();
        let writer_batch = batch.clone();
        spawn_blocking(move || {
            let mut writer = LanceSink::create(
                &writer_output,
                writer_batch.schema(),
                WriteOptions { overwrite },
            )
            .unwrap();
            if rows != 0 {
                writer.write_batch(writer_batch).unwrap();
            }
            assert_eq!(writer.finish().unwrap().rows_written, rows as u64);
        })
        .await
        .unwrap();
        assert_values(&read_lance(&output).await, &batch);
    }
}
