use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use flate2::{write::GzEncoder, Compression};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

pub(super) struct IndexedWarcFixture {
    pub archive_path: PathBuf,
    pub index_path: PathBuf,
}

pub(super) fn write_parquet_shard(
    path: &Path,
    field_name: &str,
    values: Vec<i32>,
    max_rows_per_group: usize,
) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        field_name,
        DataType::Int32,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(max_rows_per_group))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

pub(super) fn warc_record(id: usize, body: &str) -> String {
    format!(
        "WARC/1.0\r\n\
         WARC-Type: response\r\n\
         WARC-Record-ID: <urn:uuid:{id}>\r\n\
         WARC-Date: 2024-01-02T03:04:05Z\r\n\
         WARC-Target-URI: https://example.com/{id}\r\n\
         Content-Type: text/plain\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {body}\r\n\
         \r\n",
        body.len()
    )
}

/// Writes an indexed WARC fixture with one gzip member per record.
///
/// `index_order` contains indexes into `records` and controls CDXJ line order.
/// `member_gap` is inserted between members, which can force separate range
/// workloads when testing parallel scheduling.
pub(super) async fn write_indexed_warc_fixture(
    root: &Path,
    name: &str,
    records: &[(usize, &str)],
    index_order: &[usize],
    member_gap: &[u8],
) -> IndexedWarcFixture {
    let archive_name = format!("{name}.warc.gz");
    let archive_path = root.join(&archive_name);
    let index_path = root.join(format!("{name}.cdxj"));
    let mut archive = Vec::new();
    let mut locations = Vec::with_capacity(records.len());

    for (position, &(id, body)) in records.iter().enumerate() {
        if position != 0 {
            archive.extend_from_slice(member_gap);
        }
        let offset = archive.len();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(warc_record(id, body).as_bytes()).unwrap();
        let member = encoder.finish().unwrap();
        let length = member.len();
        archive.extend_from_slice(&member);
        locations.push((id, offset, length));
    }

    let mut index = String::new();
    for &record_index in index_order {
        let (id, offset, length) = locations[record_index];
        index.push_str(&format!(
            "com,example)/{id} 20240102030405 \
             {{\"filename\":\"{archive_name}\",\"offset\":\"{offset}\",\"length\":\"{length}\"}}\n"
        ));
    }

    tokio::fs::write(&archive_path, archive).await.unwrap();
    tokio::fs::write(&index_path, index).await.unwrap();
    IndexedWarcFixture {
        archive_path,
        index_path,
    }
}
