mod common;

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeListArray, Float32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use lance::index::DatasetIndexExt;
use lance_conversion::{
    BlobOnError, ColumnEncoding, LanceWriter, OpendalConfig, WriteMode, WriteOptions,
};
use tempfile::tempdir;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::spawn_blocking,
};

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
        let summary = writer.finish().await.unwrap();
        assert_eq!(summary.rows_written, rows as u64);
        assert!(summary.bytes_written > 0);
        assert_values(&read_lance(&output).await, &batch);
    }
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}

#[tokio::test]
async fn storage_version_selects_the_lance_file_format() {
    let temp = spawn_blocking(tempdir).await.unwrap().unwrap();
    for (name, version, expected) in [
        (
            "default.lance",
            WriteOptions::default().storage_version,
            "2.2",
        ),
        ("v2_1.lance", "2.1".to_string(), "2.1"),
        ("stable.lance", "stable".to_string(), "2.2"),
        ("next.lance", "next".to_string(), "2.3"),
    ] {
        let output = temp.path().join(name);
        let batch = fixture(10).project(&[0, 1, 2]).unwrap();
        let mut writer = LanceWriter::create(
            &output,
            batch.schema(),
            WriteOptions {
                storage_version: version,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        writer.write_batch(batch.clone()).await.unwrap();
        writer.finish().await.unwrap();
        let dataset = lance::Dataset::open(output.to_str().unwrap())
            .await
            .unwrap();
        assert_eq!(
            dataset.manifest().data_storage_format.version.to_string(),
            expected
        );
        assert_values(&read_lance(&output).await, &batch);
    }
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}

#[tokio::test]
async fn column_compression_is_passed_to_lance() {
    let temp = spawn_blocking(tempdir).await.unwrap().unwrap();
    let batch = fixture(1000).project(&[0, 1, 2]).unwrap();
    let write = |name: &str, column_compression: Vec<(&str, &str)>| {
        let output = temp.path().join(name);
        let batch = batch.clone();
        let column_encoding = column_compression
            .into_iter()
            .map(|(column, algorithm)| {
                (
                    column.to_string(),
                    ColumnEncoding {
                        compression: Some(algorithm.to_string()),
                        ..Default::default()
                    },
                )
            })
            .collect();
        async move {
            let mut writer = LanceWriter::create(
                &output,
                batch.schema(),
                WriteOptions {
                    column_encoding,
                    ..Default::default()
                },
            )
            .await?;
            writer.write_batch(batch).await?;
            writer.finish().await?;
            Ok::<_, lance_conversion::Error>(output)
        }
    };

    let output = write("zstd.lance", vec![("id", "zstd"), ("text", "lz4")])
        .await
        .unwrap();
    let dataset = lance::Dataset::open(output.to_str().unwrap())
        .await
        .unwrap();
    for (column, expected) in [("id", Some("zstd")), ("text", Some("lz4")), ("flag", None)] {
        let field = dataset.schema().field(column).unwrap();
        assert_eq!(
            field
                .metadata
                .get("lance-encoding:compression")
                .map(String::as_str),
            expected
        );
    }
    assert_values(&read_lance(&output).await, &batch);

    let error = write("bogus.lance", vec![("id", "bogus")])
        .await
        .unwrap_err();
    assert!(format!("{error:?}").contains("bogus"), "{error:?}");
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}

#[tokio::test]
async fn native_blobs_roundtrip_across_storage_thresholds() {
    let temp = spawn_blocking(tempdir).await.unwrap().unwrap();
    let output = temp.path().join("native_blobs.lance");
    // Cover empty, inline, packed and dedicated blobs, including both threshold
    // boundaries, and keep NULL distinct from an empty blob.
    let payloads = [
        Some(vec![]),
        Some(vec![0, 255, 16]),
        Some(vec![0, 255, 16, 32]),
        Some(vec![0, 1, 2, 3, 4]),
        Some(vec![0, 255, 16, 32, 48, 64, 80, 96]),
        Some(vec![0, 255, 16, 32, 48, 64, 80, 96, 112]),
        None,
    ];
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..payloads.len() as i64)) as ArrayRef,
        ),
        (
            "payload",
            Arc::new(BinaryArray::from_iter(
                payloads.iter().map(|value| value.as_deref()),
            )) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut writer = LanceWriter::create(
        &output,
        batch.schema(),
        WriteOptions {
            blob_inline_size_threshold: Some(4),
            blob_dedicated_size_threshold: Some(8),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    writer.write_batch(batch.clone()).await.unwrap();
    let summary = writer.finish().await.unwrap();
    assert_eq!(summary.rows_written, payloads.len() as u64);
    assert_values(&read_lance(&output).await, &batch);
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
    let server = serve_http(b"second").await;

    let uri_batch = |uris: Vec<Option<String>>| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("uri", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values(0..uris.len() as i64)) as ArrayRef,
                Arc::new(StringArray::from(uris)) as ArrayRef,
            ],
        )
        .unwrap()
    };
    let batch = uri_batch(vec![
        Some(url::Url::from_file_path(&first).unwrap().to_string()),
        Some(url::Url::from_file_path(&empty).unwrap().to_string()),
        None,
        Some(format!("{server}/media/second.bin")),
    ]);
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
                Some(b"second".as_slice()),
            ])),
        ],
    )
    .unwrap();
    let write = |output: std::path::PathBuf, batch: RecordBatch| async move {
        let mut writer = LanceWriter::create(
            &output,
            batch.schema(),
            WriteOptions {
                blob_columns: vec!["uri".into()],
                ..Default::default()
            },
        )
        .await?;
        writer.write_batch(batch).await?;
        writer.finish().await
    };
    write(output.clone(), batch).await.unwrap();
    assert_values(&read_lance(&output).await, &expected);

    let error = write(
        temp.path().join("query.lance"),
        uri_batch(vec![Some(format!("{server}/second.bin?signature=x"))]),
    )
    .await
    .err()
    .unwrap();
    assert!(
        error.to_string().contains("must not have a query"),
        "{error}"
    );
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}

#[tokio::test]
async fn blob_on_error_controls_unreadable_uris() {
    let temp = spawn_blocking(tempdir).await.unwrap().unwrap();
    let small = temp.path().join("small.bin");
    let large = temp.path().join("large.bin");
    fs::write(&small, b"abc").await.unwrap();
    fs::write(&large, b"0123456789").await.unwrap();
    let uri = |path: &std::path::Path| url::Url::from_file_path(path).unwrap().to_string();
    let missing = uri(&temp.path().join("missing.bin"));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("uri", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..5)) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some(uri(&small)),
                Some(missing.clone()),
                None,
                Some(uri(&large)),
                Some(missing),
            ])),
        ],
    )
    .unwrap();
    let write = |name: &str, blob_on_error: BlobOnError| {
        let output = temp.path().join(name);
        let batch = batch.clone();
        async move {
            let mut writer = LanceWriter::create(
                &output,
                batch.schema(),
                WriteOptions {
                    blob_columns: vec!["uri".into()],
                    blob_on_error,
                    // Keeps large.bin as a URI that Lance streams itself.
                    blob_inline_size_threshold: Some(2),
                    blob_dedicated_size_threshold: Some(4),
                    ..Default::default()
                },
            )
            .await?;
            writer.write_batch(batch).await?;
            let summary = writer.finish().await?;
            Ok::<_, lance_conversion::Error>((summary, read_lance(&output).await))
        }
    };
    let expected = |ids: Vec<i64>, values: Vec<Option<&'static [u8]>>| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("uri", DataType::Binary, true),
            ])),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(BinaryArray::from(values)),
            ],
        )
        .unwrap()
    };

    let error = write("fail.lance", BlobOnError::Fail).await.err().unwrap();
    assert!(error.to_string().contains("missing.bin"), "{error}");

    let (summary, actual) = write("null.lance", BlobOnError::Null).await.unwrap();
    assert_eq!(summary.rows_written, 5);
    assert_values(
        &actual,
        &expected(
            vec![0, 1, 2, 3, 4],
            vec![Some(b"abc"), None, None, Some(b"0123456789"), None],
        ),
    );

    let (summary, actual) = write("skip.lance", BlobOnError::Skip).await.unwrap();
    assert_eq!(summary.rows_written, 3);
    assert_values(
        &actual,
        &expected(vec![0, 2, 3], vec![Some(b"abc"), None, Some(b"0123456789")]),
    );
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}

#[tokio::test]
async fn blob_on_error_tolerates_only_permanent_http_errors() {
    let temp = spawn_blocking(tempdir).await.unwrap().unwrap();
    let server = serve_http(b"ok").await;
    let write = |name: &str, paths: &[&str]| {
        let output = temp.path().join(name);
        let uris = paths
            .iter()
            .map(|path| Some(format!("{server}{path}")))
            .collect::<Vec<_>>();
        async move {
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("uri", DataType::Utf8, true)])),
                vec![Arc::new(StringArray::from(uris)) as ArrayRef],
            )
            .unwrap();
            let mut writer = LanceWriter::create(
                &output,
                batch.schema(),
                WriteOptions {
                    blob_columns: vec!["uri".into()],
                    blob_on_error: BlobOnError::Skip,
                    opendal_config: OpendalConfig {
                        retry_max_times: 0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await?;
            writer.write_batch(batch).await?;
            let summary = writer.finish().await?;
            Ok::<_, lance_conversion::Error>(summary)
        }
    };

    let summary = write(
        "permanent.lance",
        &[
            "/ok.bin",
            "/status/404/gone.bin",
            "/status/403/forbidden.bin",
        ],
    )
    .await
    .unwrap();
    assert_eq!(summary.rows_written, 1);

    for status in ["429", "503"] {
        let error = write(
            &format!("transient_{status}.lance"),
            &["/ok.bin", &format!("/status/{status}/busy.bin")],
        )
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("busy.bin"), "{error}");
    }
    spawn_blocking(move || temp.close()).await.unwrap().unwrap();
}

/// Serves `body` at every path, one request per connection, honoring a single
/// byte range.
async fn serve_http(body: &'static [u8]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let read = socket.read(&mut request).await.unwrap();
            let request = String::from_utf8_lossy(&request[..read]).to_ascii_lowercase();
            // Paths under /status/<code>/ return that status with no body.
            let status = request
                .split_whitespace()
                .nth(1)
                .and_then(|path| path.strip_prefix("/status/"))
                .and_then(|path| path.get(..3));
            if let Some(status) = status {
                let response = format!(
                    "HTTP/1.1 {status} Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                continue;
            }
            let range = request
                .lines()
                .find_map(|line| line.strip_prefix("range: bytes="))
                .and_then(|range| range.split_once('-'))
                .map(|(start, end)| {
                    start.parse().unwrap()..end.parse().map_or(body.len(), |end: usize| end + 1)
                });
            let mut response = match &range {
                Some(range) => format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\n",
                    range.start,
                    range.end - 1,
                    body.len()
                ),
                None => "HTTP/1.1 200 OK\r\n".to_owned(),
            };
            let content = &body[range.unwrap_or(0..body.len())];
            response += &format!(
                "Content-Length: {}\r\nConnection: close\r\n\r\n",
                content.len()
            );
            let mut response = response.into_bytes();
            if !request.starts_with("head") {
                response.extend_from_slice(content);
            }
            socket.write_all(&response).await.unwrap();
        }
    });
    format!("http://{address}")
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
