mod common;

use std::fs::{self, File};
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
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
async fn existing_paths_are_never_modified() {
    spawn_blocking(|| {
        let temp = tempdir().unwrap();
        let output = temp.path().join("existing.lance");
        fs::create_dir(&output).unwrap();
        fs::write(output.join("sentinel"), b"keep me").unwrap();
        assert!(LanceSink::create(&output, fixture(0).schema(), WriteOptions::default()).is_err());
        assert_eq!(fs::read(output.join("sentinel")).unwrap(), b"keep me");
        let file = temp.path().join("existing-file");
        fs::write(&file, b"keep file").unwrap();
        assert!(LanceSink::create(&file, fixture(0).schema(), WriteOptions::default()).is_err());
        assert_eq!(fs::read(file).unwrap(), b"keep file");
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropping_unfinished_write_aborts_and_cleans_up() {
    spawn_blocking(|| {
        let temp = tempdir().unwrap();
        let output = temp.path().join("aborted.lance");
        {
            let batch = fixture(4096);
            let mut writer =
                LanceSink::create(&output, batch.schema(), WriteOptions::default()).unwrap();
            writer.write_batch(batch).unwrap();
        }
        assert!(!output.exists());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn schema_mismatch_poisoning_prevents_partial_commit() {
    spawn_blocking(|| {
        let temp = tempdir().unwrap();
        let output = temp.path().join("mismatch.lance");
        {
            let mut writer =
                LanceSink::create(&output, fixture(0).schema(), WriteOptions::default()).unwrap();
            writer.write_batch(fixture(10)).unwrap();
            let other = RecordBatch::try_from_iter(vec![(
                "id",
                Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
            )])
            .unwrap();
            assert!(writer.write_batch(other).is_err());
            assert!(writer.finish().is_err());
        }
        assert!(!output.exists());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn invalid_sources_and_types_do_not_create_output() {
    spawn_blocking(|| {
        let temp = tempdir().unwrap();
        let output = temp.path().join("bad.lance");
        assert!(convert(
            ParquetFileSource::new(temp.path()),
            &output,
            WriteOptions::default()
        )
        .is_err());
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Null, true)]));
        assert!(LanceSink::create(&output, schema, WriteOptions::default()).is_err());
        assert!(!output.exists());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn writer_error_is_returned_and_output_is_cleaned() {
    spawn_blocking(|| {
        let temp = tempdir().unwrap();
        let output = temp.path().join("failed.lance");
        {
            let mut writer =
                LanceSink::create(&output, fixture(0).schema(), WriteOptions::default()).unwrap();
            fs::write(output.join("_versions"), b"not a directory").unwrap();
            let result = writer
                .write_batch(fixture(10))
                .and_then(|_| writer.finish().map(|_| ()));
            assert!(result.is_err());
        }
        assert!(!output.exists());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn finished_writer_rejects_more_batches_without_removing_dataset() {
    let (_temp, output) = spawn_blocking(|| {
        let temp = tempdir().unwrap();
        let output = temp.path().join("finished.lance");
        {
            let mut writer =
                LanceSink::create(&output, fixture(0).schema(), WriteOptions::default()).unwrap();
            writer.write_batch(fixture(10)).unwrap();
            writer.finish().unwrap();
            assert!(writer.write_batch(fixture(1)).is_err());
            assert!(writer.finish().is_err());
        }

        (temp, output)
    })
    .await
    .unwrap();
    assert_values(&read_lance(&output).await, &fixture(10));
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

#[tokio::test]
async fn aborted_overwrite_preserves_previous_data() {
    let (_temp, output) = spawn_blocking(|| {
        let temp = tempdir().unwrap();
        let output = temp.path().join("preserved.lance");
        {
            let mut writer =
                LanceSink::create(&output, fixture(0).schema(), WriteOptions::default()).unwrap();
            writer.write_batch(fixture(10)).unwrap();
            writer.finish().unwrap();
        }
        {
            let mut writer = LanceSink::create(
                &output,
                fixture(0).schema(),
                WriteOptions { overwrite: true },
            )
            .unwrap();
            writer.write_batch(fixture(4096)).unwrap();
        }

        (temp, output)
    })
    .await
    .unwrap();
    assert_values(&read_lance(&output).await, &fixture(10));
}

#[tokio::test]
async fn overwrite_rejects_non_dataset_paths_without_removing_them() {
    spawn_blocking(|| {
        let temp = tempdir().unwrap();
        let output = temp.path().join("not-a-dataset");
        fs::create_dir(&output).unwrap();
        fs::write(output.join("sentinel"), b"keep me").unwrap();
        {
            let mut writer = LanceSink::create(
                &output,
                fixture(0).schema(),
                WriteOptions { overwrite: true },
            )
            .unwrap();
            assert!(writer.finish().is_err());
        }
        assert_eq!(fs::read(output.join("sentinel")).unwrap(), b"keep me");
        let file = temp.path().join("file");
        fs::write(&file, b"keep file").unwrap();
        assert!(
            LanceSink::create(&file, fixture(0).schema(), WriteOptions { overwrite: true })
                .is_err()
        );
        assert_eq!(fs::read(file).unwrap(), b"keep file");
    })
    .await
    .unwrap();
}
