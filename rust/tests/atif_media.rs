use std::{fs, path::Path, sync::Arc};

use arrow_array::{cast::AsArray, Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field};
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

async fn write_lance(path: &Path, batch: RecordBatch, options: WriteOptions) {
    let mut writer = LanceWriter::create(path, batch.schema(), options)
        .await
        .unwrap();
    writer.write_batch(batch).await.unwrap();
    writer.finish().await.unwrap();
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

fn decode_png(bytes: &[u8]) -> (png::OutputInfo, Vec<u8>) {
    let mut decoder = png::Decoder::new(std::io::Cursor::new(bytes))
        .read_info()
        .unwrap();
    let mut pixels = vec![0; decoder.output_buffer_size().unwrap()];
    let info = decoder.next_frame(&mut pixels).unwrap();
    pixels.truncate(info.buffer_size());
    decoder.finish().unwrap();
    (info, pixels)
}

#[tokio::test]
async fn real_atif_media_survives_conversion_and_source_deletion() {
    let temp = tempfile::tempdir().unwrap();
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("../test/data");
    let inputs = ["trajectory_media.json", "atif-image.png", "atif-audio.wav"];
    for name in inputs {
        fs::copy(fixtures.join(name), temp.path().join(name)).unwrap();
    }
    let image_path = temp.path().join("atif-image.png");
    let image = fs::read(&image_path).unwrap();
    let (info, pixels) = decode_png(&image);
    assert_eq!(
        (info.width, info.height, info.color_type, info.bit_depth),
        (1024, 768, png::ColorType::Rgb, png::BitDepth::Eight)
    );
    // Re-encode actual pixels without compression to exercise MB-scale Blob storage
    // while keeping the committed PNG small and decodable.
    {
        let mut encoder = png::Encoder::new(
            fs::File::create(&image_path).unwrap(),
            info.width,
            info.height,
        );
        encoder.set_color(info.color_type);
        encoder.set_depth(info.bit_depth);
        encoder.set_compression(png::Compression::NoCompression);
        let mut writer = encoder.write_header().unwrap();
        writer.write_image_data(&pixels).unwrap();
        writer.finish().unwrap();
    }
    let image = fs::read(&image_path).unwrap();
    let audio = fs::read(temp.path().join("atif-audio.wav")).unwrap();
    // Exercise the default out-of-line Blob path with actual image data.
    assert!(image.len() > 2 * 1024 * 1024);
    let path = temp.path().join("trajectory_media.json");
    let mut doc: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let mut child = document("not-fetched.png");
    child["trajectory_id"] = json!("child");
    doc["subagent_trajectories"] = json!([child]);
    let raw = doc.to_string();
    fs::write(&path, &raw).unwrap();
    let batch = read(AtifSource::new(path.to_string_lossy())).await.unwrap();
    assert_eq!(batch.num_columns(), 13);
    assert_eq!(batch["raw_json"].as_string::<i32>().value(0), raw);
    // Match COPY's wide Arrow offsets and preserved source metadata.
    let batch = duckdb_export(&batch);
    // A nonzero row offset checks recursive rewriting of sliced list/struct data.
    let batch = concat_batches(&batch.schema(), [&batch, &batch])
        .unwrap()
        .slice(1, 1);
    let output = temp.path().join("trajectory.lance");
    write_lance(&output, batch, WriteOptions::default()).await;
    for name in inputs {
        fs::remove_file(temp.path().join(name)).unwrap();
    }
    let dataset = Dataset::open(output.to_str().unwrap()).await.unwrap();
    assert_eq!(
        dataset.manifest().data_storage_format.version.to_string(),
        "2.2"
    );
    assert_eq!(
        dataset.schema().metadata["lance_conversion:source_type"],
        "atif"
    );
    for (path, policy) in [
        ("steps.item.message.item.text", "text"),
        ("steps.item.message.item.source.path", "blob"),
        (
            "steps.item.observation.results.item.content.item.source.path",
            "blob",
        ),
    ] {
        let field = dataset.schema().field(path).unwrap();
        assert_eq!(field.metadata["lance_conversion:source_type"], "atif");
        assert_eq!(field.metadata["lance_conversion:storage_policy"], policy);
        if policy == "text" {
            assert_eq!(field.data_type(), DataType::LargeBinary);
            assert_eq!(field.metadata["lance-encoding:compression"], "lz4");
            assert!(!field.metadata.contains_key("ARROW:extension:name"));
        } else {
            assert_eq!(field.metadata["ARROW:extension:name"], "lance.blob.v2");
        }
    }
    let batch = materialized(&dataset).await;
    let parts = message(&batch, 0, 0);
    let steps = list_value(batch["steps"].as_ref(), 0);
    let results = list_value(
        steps.as_struct()["observation"].as_struct()["results"].as_ref(),
        1,
    );
    let content = list_value(results.as_struct()["content"].as_ref(), 0);
    for (parts, text) in [
        (&parts, "hello 你好🙂"),
        (content.as_struct(), "RGB pattern and a 440 Hz tone."),
    ] {
        assert_eq!(parts["text"].as_binary::<i64>().value(0), text.as_bytes());
        assert!(parts["text"].is_null(1));
        assert!(parts["source"].is_null(0));
        // The shared media column retains each item's concrete ATIF variant.
        for (index, kind) in ["text", "image", "audio"].iter().enumerate() {
            assert_eq!(parts["type"].as_string::<i64>().value(index), *kind);
        }
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

    // Decode the bytes returned by Lance, not just the source file headers.
    let (_, decoded_pixels) = decode_png(&path_bytes(&parts, 1));
    assert_eq!(decoded_pixels, pixels);
    let audio = path_bytes(&parts, 2);
    let mut decoder = hound::WavReader::new(std::io::Cursor::new(&audio)).unwrap();
    let spec = decoder.spec();
    assert_eq!(
        (spec.channels, spec.bits_per_sample, spec.sample_rate),
        (1, 16, 8000)
    );
    let samples = decoder
        .samples::<i16>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(samples.len(), 800);
    assert!(samples.iter().any(|sample| *sample != 0));
}

// COPY exports wide offsets; the source metadata bridge preserves ATIF semantics.
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
        Arc::new(field.clone().with_data_type(data_type))
    }
    let mut schema = batch.schema().as_ref().clone();
    schema.fields = schema.fields.iter().map(|field| wide(field)).collect();
    let columns = batch
        .columns()
        .iter()
        .zip(&schema.fields)
        .map(|(array, field)| arrow_cast::cast(array, field.data_type()).unwrap())
        .collect();
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

#[tokio::test]
async fn default_lz4_and_nested_overrides_roundtrip() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("trajectory.json");
    let mut doc = document("data:image/png;base64,AP8=");
    let text = "long text 你好🙂 ".repeat(16384);
    doc["steps"][0]["message"][0]["text"] = json!(text);
    fs::write(&path, doc.to_string()).unwrap();
    let batch = read(AtifSource::new(path.to_string_lossy())).await.unwrap();
    let batch = duckdb_export(&batch);
    for (name, override_codec) in [
        ("lz4", None),
        ("zstd", Some("zstd")),
        ("none", Some("none")),
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
        write_lance(&output, batch.clone(), options).await;
        let dataset = Dataset::open(output.to_str().unwrap()).await.unwrap();
        let check = materialized(&dataset).await;
        assert_eq!(
            message(&check, 0, 0)["text"].as_binary::<i64>().value(0),
            text.as_bytes()
        );
        let field = dataset
            .schema()
            .field("steps.item.message.item.text")
            .unwrap();
        assert_eq!(field.metadata["lance-encoding:compression"], name);
        assert_eq!(field.data_type(), DataType::LargeBinary);
    }
}

#[tokio::test]
async fn jsonl_and_gzip_resolve_relative_to_the_document() {
    use std::io::Write;
    let temp = tempfile::tempdir().unwrap();
    fs::write(temp.path().join("a.png"), [0, 255]).unwrap();
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
        fs::write(&path, bytes).unwrap();
        let batch = read(AtifSource::new(path.to_string_lossy())).await.unwrap();
        assert_eq!(batch.num_rows(), if name.contains("jsonl") { 2 } else { 1 });
        for row in 0..batch.num_rows() {
            assert_eq!(path_bytes(&message(&batch, row, 0), 1), [0, 255]);
        }
    }
}
