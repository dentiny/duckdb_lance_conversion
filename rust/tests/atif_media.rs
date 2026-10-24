#[path = "common/http.rs"]
mod http;

use std::sync::Arc;

use arrow_array::{cast::AsArray, Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use futures::TryStreamExt;
use lance::{datatypes::BlobHandling, Dataset};
use lance_conversion::{AtifSource, BatchSource, ColumnEncoding, LanceWriter, WriteOptions};
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
async fn metadata_projection_skips_missing_media() {
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
    let schema = lance_conversion::atif_schema();
    let batch = read(source().with_projection(vec![schema.index_of("trajectory_id").unwrap()]))
        .await
        .unwrap();
    assert_eq!(batch.num_columns(), 1);
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
    let image_path = temp.path().join("atif-image.png");
    let image = tokio::fs::read(&image_path).await.unwrap();
    let mut decoder = png::Decoder::new(std::io::Cursor::new(image))
        .read_info()
        .unwrap();
    let mut pixels = vec![0; decoder.output_buffer_size().unwrap()];
    let info = decoder.next_frame(&mut pixels).unwrap();
    // Re-encode actual pixels without compression to exercise MB-scale Blob storage
    // while keeping the committed PNG small and decodable.
    {
        let mut encoder = png::Encoder::new(
            std::fs::File::create(&image_path).unwrap(),
            info.width,
            info.height,
        );
        encoder.set_color(info.color_type);
        encoder.set_depth(info.bit_depth);
        encoder.set_compression(png::Compression::NoCompression);
        let mut writer = encoder.write_header().unwrap();
        writer
            .write_image_data(&pixels[..info.buffer_size()])
            .unwrap();
        writer.finish().unwrap();
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
    let mut doc: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
    let mut child = document("not-fetched.png");
    child["trajectory_id"] = json!("child");
    doc["subagent_trajectories"] = json!([child]);
    let raw = doc.to_string();
    tokio::fs::write(&path, &raw).await.unwrap();
    let batch = read(AtifSource::new(path.to_string_lossy())).await.unwrap();
    assert_eq!(batch.num_columns(), 13);
    assert_eq!(batch["raw_json"].as_string::<i32>().value(0), raw);
    // Match COPY's wide Arrow offsets and loss of field metadata.
    let batch = duckdb_export(&batch);
    // A nonzero row offset checks recursive rewriting of sliced list/struct data.
    let batch = concat_batches(&batch.schema(), [&batch, &batch])
        .unwrap()
        .slice(1, 1);
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
        decoder::PageEncoding, format::metadata::compressive_encoding::Compression,
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
    use lance_encoding::format::pb23::value_plane::Codec;
    let PageEncoding::Planar(layout) = &metadata.column_infos[column].page_infos[0].encoding else {
        panic!("expected a 2.3 page");
    };
    let value_compression = match layout.values.as_ref().unwrap().codec.as_ref().unwrap() {
        Codec::Chunked(values) => values.compression.as_ref().unwrap(),
        Codec::Zipped(values) => values.compression.as_ref().unwrap(),
        other => panic!("expected distinct wide text values: {other:?}"),
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
    for (name, override_codec, expected) in [
        ("default", None, Some(1)),
        ("zstd", Some("zstd"), Some(2)),
        ("none", Some("none"), None),
    ] {
        let mut options = WriteOptions {
            storage_version: "2.3".into(),
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
        let output = temp.path().join(format!("{name}.lance"));
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
        assert_eq!(text_page_codec(&dataset).await, expected, "{name}");
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
    let doc = document("images/a.png").to_string().into_bytes();
    let server = http::serve_http(move |path| match path {
        "/runs/trajectory.json" => doc.clone(),
        "/runs/images/a.png" => vec![0, 255, 42],
        other => panic!("unexpected HTTP request: {other}"),
    })
    .await;
    let batch = read(AtifSource::new(format!("{server}/runs/trajectory.json")))
        .await
        .unwrap();
    assert_eq!(path_bytes(&message(&batch, 0, 0), 1), [0, 255, 42]);
}
