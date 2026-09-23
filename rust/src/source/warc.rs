use std::io::{BufRead, BufReader};
use std::sync::{Arc, LazyLock};

use arrow_array::{
    ArrayRef, BinaryArray, RecordBatch, StringArray, TimestampMillisecondArray, UInt32Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::stream;
use libflate::gzip::MultiDecoder;
use tokio::sync::mpsc;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::SyncIoBridge;
use warc::{BufferedBody, Record, WarcHeader, WarcReader};

use super::{BatchSource, BatchStream};
use crate::storage::OpendalStorage;
use crate::{Error, Result, S3StorageConfig};

const DEFAULT_BATCH_SIZE: usize = 8192;
const CHANNEL_CAPACITY: usize = 2;
const READ_BUFFER_SIZE: usize = 1024 * 1024;

static WARC_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("content_length", DataType::UInt32, false),
        Field::new(
            "date",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("type", DataType::Utf8, false),
        Field::new("content_type", DataType::Utf8, true),
        Field::new("concurrent_to", DataType::Utf8, true),
        Field::new("block_digest", DataType::Utf8, true),
        Field::new("payload_digest", DataType::Utf8, true),
        Field::new("ip_address", DataType::Utf8, true),
        Field::new("refers_to", DataType::Utf8, true),
        Field::new("target_uri", DataType::Utf8, true),
        Field::new("truncated", DataType::Utf8, true),
        Field::new("warc_info_id", DataType::Utf8, true),
        Field::new("filename", DataType::Utf8, true),
        Field::new("profile", DataType::Utf8, true),
        Field::new("identified_payload_type", DataType::Utf8, true),
        Field::new("segment_number", DataType::UInt32, true),
        Field::new("segment_origin_id", DataType::Utf8, true),
        Field::new("segment_total_length", DataType::UInt32, true),
        Field::new("body", DataType::Binary, false),
    ]))
});

pub fn warc_schema() -> SchemaRef {
    WARC_SCHEMA.clone()
}

pub struct WarcSource {
    path: String,
    batch_size: usize,
    s3_config: Option<S3StorageConfig>,
}

impl WarcSource {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            batch_size: DEFAULT_BATCH_SIZE,
            s3_config: None,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_s3_config(mut self, config: S3StorageConfig) -> Self {
        self.s3_config = Some(config);
        self
    }
}

impl BatchSource for WarcSource {
    async fn open(self) -> Result<(SchemaRef, BatchStream)> {
        if self.batch_size == 0 {
            return Err(Error::message("batch_size must be positive"));
        }
        let storage = OpendalStorage::from_path(&self.path, self.s3_config.as_ref())?;
        let path = storage.object_path.to_string();
        if path.is_empty() {
            return Err(Error::message("WARC input must not be a storage root"));
        }
        let gzipped = path.to_ascii_lowercase().ends_with(".gz");
        let reader = storage
            .operator
            .reader(&path)
            .await?
            .into_futures_async_read(..)
            .await?
            .compat();
        let runtime = tokio::runtime::Handle::current();
        let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
        let batch_size = self.batch_size;
        tokio::task::spawn_blocking(move || {
            let result = (|| {
                // `warc` parses `BufRead`; OpenDAL I/O remains async through this bridge.
                let reader = SyncIoBridge::new_with_handle(reader, runtime);
                let reader: Box<dyn BufRead> = if gzipped {
                    Box::new(BufReader::new(MultiDecoder::new(reader)?))
                } else {
                    Box::new(BufReader::with_capacity(READ_BUFFER_SIZE, reader))
                };
                stream_records(reader, batch_size, &sender)
            })();
            if let Err(error) = result {
                let _ = sender.blocking_send(Err(error));
            }
        });

        let batches = stream::unfold(receiver, |mut receiver| async {
            receiver.recv().await.map(|batch| (batch, receiver))
        });
        Ok((warc_schema(), Box::pin(batches)))
    }
}

fn stream_records(
    reader: Box<dyn BufRead>,
    batch_size: usize,
    sender: &mpsc::Sender<Result<RecordBatch>>,
) -> Result<()> {
    let mut reader = WarcReader::new(reader);
    let mut records = Vec::with_capacity(batch_size);
    let mut stream = reader.stream_records();
    while let Some(record) = stream.next_item() {
        let record = record
            .map_err(|error| Error::message(format!("WARC parse error: {error}")))?
            .into_buffered()
            .map_err(|error| Error::message(format!("WARC body error: {error}")))?;
        records.push(record);
        if records.len() == batch_size && !send_batch(&mut records, sender)? {
            return Ok(());
        }
    }
    if !records.is_empty() {
        send_batch(&mut records, sender)?;
    }
    Ok(())
}

fn send_batch(
    records: &mut Vec<Record<BufferedBody>>,
    sender: &mpsc::Sender<Result<RecordBatch>>,
) -> Result<bool> {
    let batch = build_record_batch(records)?;
    records.clear();
    Ok(sender.blocking_send(Ok(batch)).is_ok())
}

fn optional_header(record: &Record<BufferedBody>, header: WarcHeader) -> Option<String> {
    record.header(header).map(|value| value.into_owned())
}

fn optional_u32(
    record: &Record<BufferedBody>,
    header: WarcHeader,
    name: &str,
) -> Result<Option<u32>> {
    optional_header(record, header)
        .map(|value| {
            value
                .parse()
                .map_err(|_| Error::message(format!("invalid WARC {name}: {value}")))
        })
        .transpose()
}

fn build_record_batch(records: &[Record<BufferedBody>]) -> Result<RecordBatch> {
    let content_lengths = records
        .iter()
        .map(|record| {
            u32::try_from(record.body().len())
                .map_err(|_| Error::message("WARC record body exceeds UInt32"))
        })
        .collect::<Result<Vec<_>>>()?;
    let segment_numbers = records
        .iter()
        .map(|record| optional_u32(record, WarcHeader::SegmentNumber, "segment number"))
        .collect::<Result<Vec<_>>>()?;
    let segment_total_lengths = records
        .iter()
        .map(|record| {
            optional_u32(
                record,
                WarcHeader::SegmentTotalLength,
                "segment total length",
            )
        })
        .collect::<Result<Vec<_>>>()?;

    macro_rules! strings {
        ($header:expr) => {
            Arc::new(StringArray::from(
                records
                    .iter()
                    .map(|record| optional_header(record, $header))
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        };
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(
            records.iter().map(|record| record.warc_id()),
        )),
        Arc::new(UInt32Array::from(content_lengths)),
        Arc::new(TimestampMillisecondArray::from_iter_values(
            records
                .iter()
                .map(|record| record.date().timestamp_millis()),
        )),
        Arc::new(StringArray::from_iter_values(
            records.iter().map(|record| record.warc_type().to_string()),
        )),
        strings!(WarcHeader::ContentType),
        strings!(WarcHeader::ConcurrentTo),
        strings!(WarcHeader::BlockDigest),
        strings!(WarcHeader::PayloadDigest),
        strings!(WarcHeader::IPAddress),
        strings!(WarcHeader::RefersTo),
        strings!(WarcHeader::TargetURI),
        strings!(WarcHeader::Truncated),
        strings!(WarcHeader::WarcInfoID),
        strings!(WarcHeader::Filename),
        strings!(WarcHeader::Profile),
        strings!(WarcHeader::IdentifiedPayloadType),
        Arc::new(UInt32Array::from(segment_numbers)),
        strings!(WarcHeader::SegmentOriginID),
        Arc::new(UInt32Array::from(segment_total_lengths)),
        Arc::new(BinaryArray::from_iter_values(
            records.iter().map(|record| record.body()),
        )),
    ];
    Ok(RecordBatch::try_new(warc_schema(), columns)?)
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    use arrow_array::BinaryArray;
    use futures::TryStreamExt;
    use libflate::gzip::Encoder;

    use super::*;

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    async fn test_directory() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "lance-warc-test-{}-{}",
            std::process::id(),
            NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        tokio::fs::create_dir(&path).await.unwrap();
        path
    }

    fn record(id: usize, body: &str) -> String {
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

    #[test]
    fn exposes_standard_warc_schema() {
        let schema = warc_schema();
        assert_eq!(schema.fields().len(), 20);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(schema.field(19).name(), "body");
        assert_eq!(schema.field(19).data_type(), &DataType::Binary);
    }

    #[tokio::test]
    async fn streams_plain_and_gzipped_warc() {
        let temp = test_directory().await;
        let input = format!("{}{}", record(1, "first"), record(2, "second"));
        let plain_path = temp.join("records.warc");
        tokio::fs::write(&plain_path, &input).await.unwrap();

        let (_, stream) = WarcSource::new(plain_path.to_string_lossy())
            .with_batch_size(1)
            .open()
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(
            batches[0]
                .column_by_name("body")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            b"first"
        );

        let gzip_path = temp.join("records.warc.gz");
        let mut encoder = Encoder::new(Vec::new()).unwrap();
        encoder.write_all(input.as_bytes()).unwrap();
        tokio::fs::write(gzip_path, encoder.finish().into_result().unwrap())
            .await
            .unwrap();
        let (_, stream) = WarcSource::new(temp.join("records.warc.gz").to_string_lossy())
            .open()
            .await
            .unwrap();
        assert_eq!(
            stream
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
        tokio::fs::remove_dir_all(temp).await.unwrap();
    }

    #[tokio::test]
    async fn reports_malformed_warc() {
        let temp = test_directory().await;
        let path = temp.join("invalid.warc");
        tokio::fs::write(
            &path,
            "WARC/1.0\r\nWARC-Type: response\r\nWARC-Record-ID: <urn:uuid:bad>\r\n\r\n",
        )
        .await
        .unwrap();
        let (_, mut stream) = WarcSource::new(path.to_string_lossy())
            .open()
            .await
            .unwrap();
        assert!(stream
            .try_next()
            .await
            .unwrap_err()
            .to_string()
            .contains("WARC parse error"));
        tokio::fs::remove_dir_all(temp).await.unwrap();
    }
}
