use std::sync::Arc;

use arrow_array::{cast::AsArray, Array, ListArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use lance::{datatypes::BlobHandling, Dataset};
use lance_conversion::{
    AtifMediaOptions, AtifSource, BatchSource, ColumnEncoding, LanceWriter, WriteOptions,
};
use serde_json::json;

fn document(path: &str) -> serde_json::Value {
    json!({"schema_version":"ATIF-v1.8","trajectory_id":"root",
    "agent":{"name":"test","version":"1"},
    "steps":[{"step_id":1,"source":"user","message":[
        {"type":"text","text":"image"},
        {"type":"image","source":{"media_type":"image/png","path":path}}
    ]}]})
}

async fn read(source: AtifSource) -> Result<RecordBatch, lance_conversion::Error> {
    let (schema, batches) = source.open().await?;
    let batches = batches.try_collect::<Vec<_>>().await?;
    Ok(concat_batches(&schema, &batches).unwrap())
}

fn list_value(array: &dyn Array, index: usize) -> arrow_array::ArrayRef {
    match array.data_type() {
        DataType::List(_) => array.as_list::<i32>().value(index),
        DataType::LargeList(_) => array.as_list::<i64>().value(index),
        other => panic!("expected a list: {other}"),
    }
}

fn message(batch: &RecordBatch, row: usize, step: usize) -> StructArray {
    let steps = list_value(batch["steps"].as_ref(), row);
    let parts = list_value(steps.as_struct()["message"].as_ref(), step);
    parts.as_struct().clone()
}

fn path_bytes(parts: &StructArray, index: usize) -> Vec<u8> {
    let array = &parts["source"].as_struct()["path"];
    let binary = arrow_cast::cast(array, &DataType::LargeBinary).unwrap();
    binary.as_binary::<i64>().value(index).to_vec()
}

async fn materialized(dataset: &Dataset) -> RecordBatch {
    let mut scanner = dataset.scan();
    scanner.blob_handling(BlobHandling::AllBinary);
    let batches = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    concat_batches(&batches[0].schema(), &batches).unwrap()
}

#[tokio::test]
async fn embeds_native_message_and_observation_fields_without_a_sidecar_column() {
    let temp = tempfile::tempdir().unwrap();
    let image = vec![0xa5; 2 * 1024 * 1024 + 1];
    tokio::fs::write(temp.path().join("image.png"), &image)
        .await
        .unwrap();
    let mut doc = document("image.png");
    doc["steps"][0]["message"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "type":"audio","source":{"media_type":"audio/wav","path":"data:audio/wav;base64,AP8="}
        }));
    doc["steps"].as_array_mut().unwrap().push(json!({
        "step_id":2,"source":"agent","message":"你好🙂",
        "observation":{"results":[{"content":[
            {"type":"image","source":{"media_type":"image/png","path":"image.png"}}
        ]}]}
    }));
    // Embedded subagents remain opaque JSON, matching the native atif-arrow contract.
    let mut child = document("not-fetched.png");
    child["trajectory_id"] = json!("child");
    doc["subagent_trajectories"] = json!([child]);
    let raw = doc.to_string();
    let path = temp.path().join("trajectory.json");
    tokio::fs::write(&path, &raw).await.unwrap();
    let batch = read(AtifSource::new(path.to_string_lossy())).await.unwrap();
    assert_eq!(batch.num_columns(), 13);
    assert!(batch.column_by_name("media").is_none());
    let parts = message(&batch, 0, 0);
    assert_eq!(path_bytes(&parts, 1), image);
    assert_eq!(path_bytes(&parts, 2), [0, 255]);
    assert!(parts["source"].is_null(0));
    assert_eq!(parts["text"].as_binary::<i64>().value(0), b"image");
    assert!(parts["text"].is_null(1));
    let steps = batch["steps"].as_list::<i32>().value(0);
    let results = steps.as_struct()["observation"].as_struct()["results"]
        .as_list::<i32>()
        .value(1);
    let content = results.as_struct()["content"].as_list::<i32>().value(0);
    assert_eq!(path_bytes(content.as_struct(), 0), image);
    assert_eq!(batch["raw_json"].as_string::<i32>().value(0), raw);
}

#[tokio::test]
async fn missing_media_can_be_inspected_as_references_or_skipped_by_projection() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("trajectory.json");
    tokio::fs::write(&path, document("missing.png").to_string())
        .await
        .unwrap();
    let source = || AtifSource::new(path.to_string_lossy());
    assert!(read(source())
        .await
        .unwrap_err()
        .to_string()
        .contains("ATIF media"));
    let batch = read(source().with_media_options(AtifMediaOptions {
        resolve_media: false,
        ..Default::default()
    }))
    .await
    .unwrap();
    let parts = message(&batch, 0, 0);
    assert_eq!(
        parts["source"].as_struct()["path"]
            .as_string::<i32>()
            .value(1),
        "missing.png"
    );
    let schema = lance_conversion::atif_schema();
    let batch = read(source().with_projection(vec![schema.index_of("trajectory_id").unwrap()]))
        .await
        .unwrap();
    assert_eq!(batch.num_columns(), 1);
}

#[tokio::test]
async fn enforces_per_file_and_trajectory_media_limits() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("trajectory.json");
    let mut doc = document("a.png");
    tokio::fs::write(temp.path().join("a.png"), [0, 255, 42])
        .await
        .unwrap();
    doc["steps"][0]["message"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "type":"audio","source":{"media_type":"audio/wav","path":"data:audio/wav;base64,AP8="}
        }));
    tokio::fs::write(&path, doc.to_string()).await.unwrap();
    let source = || AtifSource::new(path.to_string_lossy());
    for options in [
        AtifMediaOptions {
            max_media_bytes: 2,
            ..Default::default()
        },
        AtifMediaOptions {
            max_trajectory_media_bytes: 4,
            ..Default::default()
        },
    ] {
        assert!(read(source().with_media_options(options))
            .await
            .unwrap_err()
            .to_string()
            .contains("max_media_bytes"));
    }
}

#[tokio::test]
async fn real_atif_media_survives_conversion_and_source_deletion() {
    let temp = tempfile::tempdir().unwrap();
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../test/data");
    let inputs = ["trajectory_media.json", "atif-image.png", "atif-audio.wav"];
    for name in inputs {
        tokio::fs::copy(fixtures.join(name), temp.path().join(name))
            .await
            .unwrap();
    }
    let image = tokio::fs::read(temp.path().join("atif-image.png"))
        .await
        .unwrap();
    let audio = tokio::fs::read(temp.path().join("atif-audio.wav"))
        .await
        .unwrap();
    // Exercise the default out-of-line Blob path with actual image data.
    assert!(image.len() > 2 * 1024 * 1024);
    let path = temp.path().join("trajectory_media.json");
    let batch = read(AtifSource::new(path.to_string_lossy())).await.unwrap();
    // Match COPY's wide Arrow offsets and loss of field metadata.
    let batch = duckdb_export(&batch);
    let output = temp.path().join("trajectory.lance");
    let mut writer = LanceWriter::create(&output, batch.schema(), WriteOptions::default())
        .await
        .unwrap();
    writer.write_batch(batch).await.unwrap();
    writer.finish().await.unwrap();
    for name in inputs {
        tokio::fs::remove_file(temp.path().join(name))
            .await
            .unwrap();
    }
    let dataset = Dataset::open(output.to_str().unwrap()).await.unwrap();
    assert_eq!(
        dataset.manifest().data_storage_format.version.to_string(),
        "2.2"
    );
    let text = dataset
        .schema()
        .field("steps.item.message.item.text")
        .unwrap();
    assert_eq!(text.data_type(), DataType::LargeBinary);
    assert_eq!(
        text.metadata.get("lance-encoding:compression").unwrap(),
        "lz4"
    );
    assert!(!text.metadata.contains_key("ARROW:extension:name"));
    for path in [
        "steps.item.message.item.source.path",
        "steps.item.observation.results.item.content.item.source.path",
    ] {
        let media = dataset.schema().field(path).unwrap();
        assert_eq!(
            media.metadata.get("ARROW:extension:name").unwrap(),
            "lance.blob.v2"
        );
    }
    let batch = materialized(&dataset).await;
    let parts = message(&batch, 0, 0);
    assert_eq!(
        parts["text"].as_binary::<i64>().value(0),
        "hello 你好🙂".as_bytes()
    );
    assert!(parts["text"].is_null(1));
    assert!(parts["source"].is_null(0));
    let steps = list_value(batch["steps"].as_ref(), 0);
    let results = list_value(
        steps.as_struct()["observation"].as_struct()["results"].as_ref(),
        1,
    );
    let content = list_value(results.as_struct()["content"].as_ref(), 0);
    for parts in [&parts, content.as_struct()] {
        assert_eq!(path_bytes(parts, 1), image);
        assert_eq!(path_bytes(parts, 2), audio);
        let source = parts["source"].as_struct();
        assert_eq!(
            source["media_type"].as_string::<i64>().value(1),
            "image/png"
        );
        assert_eq!(
            source["media_type"].as_string::<i64>().value(2),
            "audio/wav"
        );
        assert_eq!(
            source["duration_sec"]
                .as_primitive::<arrow_array::types::Float64Type>()
                .value(2),
            0.1
        );
    }
    assert_eq!(
        content.as_struct()["text"].as_binary::<i64>().value(0),
        b"RGB pattern and a 440 Hz tone."
    );

    // Decode the bytes returned by Lance, not just the source file headers.
    let image = path_bytes(&parts, 1);
    let mut decoder = png::Decoder::new(std::io::Cursor::new(&image))
        .read_info()
        .unwrap();
    let mut pixels = vec![0; decoder.output_buffer_size().unwrap()];
    let info = decoder.next_frame(&mut pixels).unwrap();
    assert_eq!((info.width, info.height), (1024, 768));
    assert_eq!(info.color_type, png::ColorType::Rgb);
    assert_eq!(info.bit_depth, png::BitDepth::Eight);
    assert_eq!(&pixels[..3], &[0, 0, 0]);
    assert_eq!(
        &pixels[info.buffer_size() - 3..info.buffer_size()],
        &[255, 255, 254]
    );
    decoder.finish().unwrap();
    let audio = path_bytes(&parts, 2);
    let mut decoder = hound::WavReader::new(std::io::Cursor::new(&audio)).unwrap();
    let spec = decoder.spec();
    assert_eq!(
        (spec.channels, spec.bits_per_sample, spec.sample_rate),
        (1, 16, 8000)
    );
    assert_eq!(decoder.duration(), 800);
    let samples = decoder
        .samples::<i16>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(samples.len(), 800);
    assert!(samples.iter().any(|sample| *sample != 0));
}

async fn text_page_codec(dataset: &Dataset) -> Option<i32> {
    use lance_encoding::{
        decoder::PageEncoding,
        format::metadata::{compressive_encoding::Compression, page_layout::Layout},
    };
    use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
    let field_id = dataset
        .schema()
        .field("steps.item.message.item.text")
        .unwrap()
        .id;
    let fragment = dataset.get_fragments().remove(0);
    let file = &fragment.metadata().files[0];
    let field_index = file.fields.iter().position(|id| *id == field_id).unwrap();
    let column = file.column_indices[field_index] as usize;
    let store = dataset.object_store(file.base_id).await.unwrap();
    let scheduler = ScanScheduler::new(store, SchedulerConfig::default_for_testing());
    let reader = scheduler
        .open_file(
            &dataset.data_dir().join(file.path.as_str()),
            &file.file_size_bytes,
        )
        .await
        .unwrap();
    let metadata = fragment.get_file_metadata(&reader).await.unwrap();
    let value_compression = match &metadata.column_infos[column].page_infos[0].encoding {
        PageEncoding::Structural(layout) => match &layout.layout {
            Some(Layout::FullZipLayout(layout)) => layout.value_compression.as_ref().unwrap(),
            Some(Layout::MiniBlockLayout(layout)) => layout.value_compression.as_ref().unwrap(),
            other => panic!("expected a data page for distinct wide values: {other:?}"),
        },
        PageEncoding::Planar(layout) => {
            use lance_encoding::format::pb23::value_plane::Codec;
            match layout.values.as_ref().unwrap().codec.as_ref().unwrap() {
                Codec::Chunked(values) => values.compression.as_ref().unwrap(),
                Codec::Zipped(values) => values.compression.as_ref().unwrap(),
                other => panic!("expected a data page for distinct wide values: {other:?}"),
            }
        }
        other => panic!("unexpected page encoding: {other:?}"),
    };
    match &value_compression.compression {
        Some(Compression::General(general)) => Some(general.compression.as_ref().unwrap().scheme),
        Some(Compression::Variable(variable)) => variable
            .values
            .as_ref()
            .map(|compression| compression.scheme),
        other => panic!("unexpected text compression descriptor: {other:?}"),
    }
}

// COPY exports wide offsets and drops input Arrow metadata. The ATIF defaults
// must follow ContentPart structure, not native Utf8/List widths or annotations.
fn duckdb_export(batch: &RecordBatch) -> RecordBatch {
    fn wide(field: &Field) -> Arc<Field> {
        let data_type = match field.data_type() {
            DataType::Utf8 => DataType::LargeUtf8,
            DataType::Struct(fields) => {
                DataType::Struct(fields.iter().map(|field| wide(field)).collect())
            }
            DataType::List(item) => DataType::LargeList(wide(item)),
            other => other.clone(),
        };
        Arc::new(Field::new(field.name(), data_type, field.is_nullable()))
    }
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| wide(field))
        .collect::<Vec<_>>();
    let columns = batch
        .columns()
        .iter()
        .zip(&fields)
        .map(|(array, field)| arrow_cast::cast(array, field.data_type()).unwrap())
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

#[tokio::test]
async fn default_lz4_and_explicit_nested_overrides_reach_the_actual_text_page() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("trajectory.json");
    let mut doc = document("data:image/png;base64,AP8=");
    let text = "long text 你好🙂 ".repeat(16384);
    doc["steps"][0]["message"][0]["text"] = json!(text);
    // Distinct wide values avoid the single-value dictionary optimization.
    for index in 1..4 {
        doc["steps"][0]["message"]
            .as_array_mut()
            .unwrap()
            .push(json!({"type":"text", "text":format!("{text}-{index}")}));
    }
    tokio::fs::write(&path, doc.to_string()).await.unwrap();
    let batch = read(AtifSource::new(path.to_string_lossy())).await.unwrap();
    let batch = duckdb_export(&batch);
    for (version, default_codec) in [("2.2", 2), ("2.3", 1)] {
        for (name, override_codec, expected) in [
            ("default", None, Some(default_codec)),
            ("zstd", Some("zstd"), Some(2)),
            ("none", Some("none"), None),
        ] {
            let mut options = WriteOptions {
                storage_version: version.into(),
                ..Default::default()
            };
            if let Some(codec) = override_codec {
                options.column_encoding.insert(
                    "steps.message.text".into(),
                    ColumnEncoding {
                        compression: Some(codec.into()),
                        ..Default::default()
                    },
                );
            }
            let output = temp.path().join(format!("{version}-{name}.lance"));
            let mut writer = LanceWriter::create(&output, batch.schema(), options)
                .await
                .unwrap();
            writer.write_batch(batch.clone()).await.unwrap();
            writer.finish().await.unwrap();
            let dataset = Dataset::open(output.to_str().unwrap()).await.unwrap();
            let check = materialized(&dataset).await;
            assert_eq!(
                message(&check, 0, 0)["text"].as_binary::<i64>().value(0),
                text.as_bytes()
            );
            assert_eq!(
                text_page_codec(&dataset).await,
                expected,
                "{version}/{name}"
            );
        }
    }
}

#[tokio::test]
async fn jsonl_and_gzip_resolve_relative_to_the_document() {
    use std::io::Write;
    let temp = tempfile::tempdir().unwrap();
    tokio::fs::write(temp.path().join("a.png"), [0, 255])
        .await
        .unwrap();
    for name in ["t.jsonl", "t.json.gz", "t.jsonl.gz"] {
        let one = document("a.png").to_string();
        let text = if name.contains("jsonl") {
            format!("{one}\n{one}\n")
        } else {
            one
        };
        let bytes = if name.ends_with(".gz") {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(text.as_bytes()).unwrap();
            encoder.finish().unwrap()
        } else {
            text.into_bytes()
        };
        let path = temp.path().join(name);
        tokio::fs::write(&path, bytes).await.unwrap();
        let batch = read(AtifSource::new(path.to_string_lossy())).await.unwrap();
        assert_eq!(batch.num_rows(), if name.contains("jsonl") { 2 } else { 1 });
        for row in 0..batch.num_rows() {
            assert_eq!(path_bytes(&message(&batch, row, 0), 1), [0, 255]);
        }
    }
}

#[tokio::test]
async fn http_media_uses_document_relative_paths() {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://{}/runs/trajectory.json",
        listener.local_addr().unwrap()
    );
    let doc = Arc::new(document("images/a.png").to_string().into_bytes());
    let bytes = Arc::new(vec![0, 255, 42]);
    let expected = bytes.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let doc = doc.clone();
            let bytes = bytes.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                loop {
                    let mut chunk = [0; 1024];
                    let count = socket.read(&mut chunk).await.unwrap();
                    if count == 0 {
                        return;
                    }
                    request.extend_from_slice(&chunk[..count]);
                    if request.windows(4).any(|part| part == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(request).unwrap();
                let path = request.split_whitespace().nth(1).unwrap();
                let body = match path {
                    "/runs/trajectory.json" => doc,
                    "/runs/images/a.png" => bytes,
                    _ => {
                        socket.write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                        return;
                    }
                };
                let range = request.lines().find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("range: bytes=")
                        .map(str::to_owned)
                });
                let (status, start, end, extra) = if let Some(range) = range {
                    let (start, end) = range.split_once('-').unwrap();
                    let start: usize = start.parse().unwrap();
                    let end = end
                        .parse::<usize>()
                        .unwrap_or(body.len() - 1)
                        .min(body.len() - 1);
                    (
                        "206 Partial Content",
                        start,
                        end + 1,
                        format!("Content-Range: bytes {start}-{end}/{}\r\n", body.len()),
                    )
                } else {
                    ("200 OK", 0, body.len(), String::new())
                };
                let headers = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n",
                    end - start
                );
                socket.write_all(headers.as_bytes()).await.unwrap();
                if !request.starts_with("HEAD ") {
                    socket.write_all(&body[start..end]).await.unwrap();
                }
            });
        }
    });
    let result = read(AtifSource::new(url)).await;
    server.abort();
    let batch = result.unwrap();
    assert_eq!(path_bytes(&message(&batch, 0, 0), 1), expected.as_slice());
}

#[tokio::test]
async fn nested_blob_rewrite_preserves_slices_nulls_and_empty_lists() {
    use arrow_array::{
        builder::{BinaryBuilder, ListBuilder},
        BinaryArray,
    };
    let temp = tempfile::tempdir().unwrap();
    let mut builder = ListBuilder::new(BinaryBuilder::new());
    builder.values().append_value([9]);
    builder.append(true);
    builder.values().append_value([]);
    builder.values().append_null();
    builder.append(true);
    builder.append(false);
    builder.append(true);
    builder.values().append_value([1, 2, 255]);
    builder.append(true);
    let array = Arc::new(builder.finish()) as arrow_array::ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "values",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array])
        .unwrap()
        .slice(1, 4);
    let output = temp.path().join("lists.lance");
    let mut writer = LanceWriter::create(&output, batch.schema(), WriteOptions::default())
        .await
        .unwrap();
    writer.write_batch(batch).await.unwrap();
    writer.finish().await.unwrap();
    let dataset = Dataset::open(output.to_str().unwrap()).await.unwrap();
    let mut scanner = dataset.scan();
    scanner.blob_handling(BlobHandling::AllBinary);
    let batches = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = array.value(0);
    let binary = arrow_cast::cast(&values, &DataType::Binary).unwrap();
    let binary = binary.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(binary.value(0), b"");
    assert!(binary.is_null(1));
    assert!(array.is_null(1));
    assert_eq!(array.value(2).len(), 0);
    let values = array.value(3);
    let binary = arrow_cast::cast(&values, &DataType::Binary).unwrap();
    assert_eq!(
        binary
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        [1, 2, 255]
    );
}

#[tokio::test]
async fn blob_containers_roundtrip_through_lance() {
    use arrow_array::{
        builder::{BinaryBuilder, LargeListBuilder, MapBuilder, StringBuilder},
        BinaryArray,
    };
    let temp = tempfile::tempdir().unwrap();
    let binary = Arc::new(BinaryArray::from_vec(vec![b"x"])) as arrow_array::ArrayRef;
    let record = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("payload", DataType::Binary, true)),
        binary.clone(),
    )])) as arrow_array::ArrayRef;
    let mut large = LargeListBuilder::new(BinaryBuilder::new());
    large.values().append_value(b"x");
    large.append(true);
    let large = Arc::new(large.finish()) as arrow_array::ArrayRef;
    let mut map = MapBuilder::new(None, StringBuilder::new(), BinaryBuilder::new());
    map.keys().append_value("key");
    map.values().append_value(b"x");
    map.append(true).unwrap();
    let map = Arc::new(map.finish()) as arrow_array::ArrayRef;
    for (name, array) in [("record", record), ("large", large), ("map", map)] {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap();
        let output = temp.path().join(format!("{name}.lance"));
        let mut writer = LanceWriter::create(&output, batch.schema(), WriteOptions::default())
            .await
            .unwrap();
        writer.write_batch(batch).await.unwrap();
        writer
            .finish()
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        let dataset = Dataset::open(output.to_str().unwrap()).await.unwrap();
        let mut scanner = dataset.scan();
        scanner.blob_handling(BlobHandling::AllBinary);
        let batches = scanner
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches[0].num_rows(), 1, "{name}");
        let column = batches[0].column(0);
        let payload = match name {
            "record" => column
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .column(0)
                .clone(),
            "large" => {
                if let Some(list) = column
                    .as_any()
                    .downcast_ref::<arrow_array::LargeListArray>()
                {
                    list.value(0)
                } else {
                    column
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .unwrap()
                        .value(0)
                }
            }
            "map" => column
                .as_any()
                .downcast_ref::<arrow_array::MapArray>()
                .unwrap()
                .values()
                .clone(),
            _ => unreachable!(),
        };
        let payload = arrow_cast::cast(&payload, &DataType::Binary).unwrap();
        assert_eq!(
            payload
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            b"x",
            "{name}"
        );
    }
}
