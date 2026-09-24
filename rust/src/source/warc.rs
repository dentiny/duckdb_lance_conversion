use std::io::{BufRead, BufReader, Read};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, LazyLock};

use arrow_array::{
    builder::{BinaryViewBuilder, StringBuilder, TimestampMillisecondBuilder, UInt64Builder},
    ArrayRef, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use flate2::read::MultiGzDecoder;
use futures::stream;
use tokio::sync::mpsc;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::SyncIoBridge;
use warc::{Record, StreamingBody, WarcHeader, WarcReader};

use super::{BatchSource, BatchStream};
use crate::storage::OpendalStorage;
use crate::{Error, Result, S3StorageConfig};

const DEFAULT_BATCH_SIZE: usize = 8192;
// A batch crosses Arrow -> DuckDB -> Arrow and may remain queued while Lance
// encodes it, so body bytes dominate peak memory much more than row count.
const MAX_BATCH_BODY_BYTES: usize = 16 * 1024 * 1024;
const CHANNEL_CAPACITY: usize = 1;
const READ_BUFFER_SIZE: usize = 1024 * 1024;
const BODY_COLUMN_INDEX: usize = 19;

static WARC_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("content_length", DataType::UInt64, false),
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
        Field::new("segment_number", DataType::UInt64, true),
        Field::new("segment_origin_id", DataType::Utf8, true),
        Field::new("segment_total_length", DataType::UInt64, true),
        Field::new("body", DataType::BinaryView, false),
    ]))
});

pub fn warc_schema() -> SchemaRef {
    WARC_SCHEMA.clone()
}

fn optional_u64(value: Option<String>, name: &str) -> Result<Option<u64>> {
    value
        .map(|value| {
            value
                .parse()
                .map_err(|_| Error::message(format!("invalid WARC {name}: {value}")))
        })
        .transpose()
}

fn optional_header<R: Read>(
    record: &Record<StreamingBody<'_, R>>,
    header: WarcHeader,
) -> Option<String> {
    record.header(header).map(|value| value.into_owned())
}

fn string_header(index: usize) -> Option<WarcHeader> {
    match index {
        4 => Some(WarcHeader::ContentType),
        5 => Some(WarcHeader::ConcurrentTo),
        6 => Some(WarcHeader::BlockDigest),
        7 => Some(WarcHeader::PayloadDigest),
        8 => Some(WarcHeader::IPAddress),
        9 => Some(WarcHeader::RefersTo),
        10 => Some(WarcHeader::TargetURI),
        11 => Some(WarcHeader::Truncated),
        12 => Some(WarcHeader::WarcInfoID),
        13 => Some(WarcHeader::Filename),
        14 => Some(WarcHeader::Profile),
        15 => Some(WarcHeader::IdentifiedPayloadType),
        17 => Some(WarcHeader::SegmentOriginID),
        _ => None,
    }
}

pub struct WarcSource {
    path: String,
    batch_size: usize,
    s3_config: Option<S3StorageConfig>,
    projection: Option<Vec<usize>>,
}

impl WarcSource {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            batch_size: DEFAULT_BATCH_SIZE,
            s3_config: None,
            projection: None,
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

    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }
}

impl BatchSource for WarcSource {
    async fn open(self) -> Result<(SchemaRef, BatchStream)> {
        if self.batch_size == 0 {
            return Err(Error::message("batch_size must be positive"));
        }
        let full_schema = warc_schema();
        let projection = match self.projection {
            Some(projection) if !projection.is_empty() => projection,
            _ => (0..full_schema.fields().len()).collect(),
        };
        let mut seen = vec![false; full_schema.fields().len()];
        for &index in &projection {
            let projected = seen
                .get_mut(index)
                .ok_or_else(|| Error::message(format!("invalid WARC column index: {index}")))?;
            if std::mem::replace(projected, true) {
                return Err(Error::message(format!(
                    "duplicate WARC column index: {index}"
                )));
            }
        }
        let fields =
            projection
                .iter()
                .map(|&index| {
                    full_schema.fields().get(index).cloned().ok_or_else(|| {
                        Error::message(format!("invalid WARC column index: {index}"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(fields));
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
        let stream_projection = projection;
        let stream_schema = schema.clone();
        tokio::task::spawn_blocking(move || {
            let result = catch_unwind(AssertUnwindSafe(|| {
                // `warc` parses `BufRead`; OpenDAL I/O remains async through this bridge.
                let reader = SyncIoBridge::new_with_handle(reader, runtime);
                let reader: Box<dyn BufRead> = if gzipped {
                    let reader = BufReader::with_capacity(READ_BUFFER_SIZE, reader);
                    Box::new(BufReader::new(MultiGzDecoder::new(reader)))
                } else {
                    Box::new(BufReader::with_capacity(READ_BUFFER_SIZE, reader))
                };
                stream_records(
                    reader,
                    batch_size,
                    stream_projection,
                    stream_schema,
                    &sender,
                )
            }));
            let error = match result {
                Ok(Ok(())) => return,
                Ok(Err(error)) => error,
                Err(payload) => {
                    let message = payload
                        .downcast_ref::<&str>()
                        .copied()
                        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                        .unwrap_or("unknown panic");
                    Error::message(format!("panic in WARC parser: {message}"))
                }
            };
            if !sender.is_closed() {
                let _ = sender.blocking_send(Err(error));
            }
        });

        let batches = stream::unfold(receiver, |mut receiver| async {
            receiver.recv().await.map(|batch| (batch, receiver))
        });
        Ok((schema, Box::pin(batches)))
    }
}

fn stream_records(
    reader: Box<dyn BufRead>,
    batch_size: usize,
    projection: Vec<usize>,
    schema: SchemaRef,
    sender: &mpsc::Sender<Result<RecordBatch>>,
) -> Result<()> {
    let include_body = projection.contains(&BODY_COLUMN_INDEX);
    let mut reader = WarcReader::new(reader);
    let mut batch = WarcBatchBuilder::new(batch_size, projection, schema)?;
    let mut stream = reader.stream_records();
    while let Some(record) = stream.next_item() {
        if sender.is_closed() {
            return Ok(());
        }
        let record =
            record.map_err(|error| Error::message(format!("WARC parse error: {error}")))?;
        let body_len = if include_body {
            u32::try_from(record.content_length())
                .map_err(|_| Error::message("WARC body exceeds Arrow BinaryView limit"))?;
            usize::try_from(record.content_length())
                .map_err(|_| Error::message("WARC record body exceeds usize"))?
        } else {
            0
        };
        if !batch.is_empty()
            && batch.would_exceed_body_limit(body_len)
            && !send_batch(batch.take_empty(batch_size)?, sender)?
        {
            return Ok(());
        }
        batch.append_metadata(&record)?;
        if include_body {
            let body = record
                .into_buffered()
                .map_err(|error| Error::message(format!("WARC body error: {error}")))?
                .into_raw_parts()
                .1;
            batch.append_body(body)?;
        }
        batch.finish_row();
        if (batch.len() == batch_size || batch.body_bytes() >= MAX_BATCH_BODY_BYTES)
            && !send_batch(batch.take_empty(batch_size)?, sender)?
        {
            return Ok(());
        }
    }
    if !batch.is_empty() {
        send_batch(batch, sender)?;
    }
    Ok(())
}

fn send_batch(batch: WarcBatchBuilder, sender: &mpsc::Sender<Result<RecordBatch>>) -> Result<bool> {
    Ok(sender.blocking_send(Ok(batch.finish()?)).is_ok())
}

enum ProjectedColumnBuilder {
    Utf8(StringBuilder),
    UInt64(UInt64Builder),
    Timestamp(TimestampMillisecondBuilder),
    Body(BinaryViewBuilder),
}

impl ProjectedColumnBuilder {
    fn new(index: usize, capacity: usize) -> Result<Self> {
        let builder = match index {
            0 | 3 => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
            index if string_header(index).is_some() => {
                Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 16))
            }
            1 | 16 | 18 => Self::UInt64(UInt64Builder::with_capacity(capacity)),
            2 => Self::Timestamp(TimestampMillisecondBuilder::with_capacity(capacity)),
            BODY_COLUMN_INDEX => Self::Body(BinaryViewBuilder::with_capacity(capacity)),
            _ => {
                return Err(Error::message(format!(
                    "invalid WARC column index: {index}"
                )))
            }
        };
        Ok(builder)
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Utf8(mut builder) => Arc::new(builder.finish()),
            Self::UInt64(mut builder) => Arc::new(builder.finish()),
            Self::Timestamp(mut builder) => Arc::new(builder.finish()),
            Self::Body(mut builder) => Arc::new(builder.finish()),
        }
    }
}

struct WarcBatchBuilder {
    len: usize,
    body_bytes: usize,
    projection: Vec<usize>,
    schema: SchemaRef,
    columns: Vec<ProjectedColumnBuilder>,
    body_position: Option<usize>,
}

impl WarcBatchBuilder {
    fn new(capacity: usize, projection: Vec<usize>, schema: SchemaRef) -> Result<Self> {
        let columns = projection
            .iter()
            .map(|&index| ProjectedColumnBuilder::new(index, capacity))
            .collect::<Result<Vec<_>>>()?;
        let body_position = projection
            .iter()
            .position(|&index| index == BODY_COLUMN_INDEX);
        Ok(Self {
            len: 0,
            body_bytes: 0,
            projection,
            schema,
            columns,
            body_position,
        })
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn body_bytes(&self) -> usize {
        self.body_bytes
    }

    fn would_exceed_body_limit(&self, body_len: usize) -> bool {
        self.body_position.is_some()
            && self.body_bytes.saturating_add(body_len) > MAX_BATCH_BODY_BYTES
    }

    fn take_empty(&mut self, capacity: usize) -> Result<Self> {
        let projection = self.projection.clone();
        let replacement = Self::new(capacity, projection, self.schema.clone())?;
        Ok(std::mem::replace(self, replacement))
    }

    fn append_metadata<R: Read>(&mut self, record: &Record<StreamingBody<'_, R>>) -> Result<()> {
        for (&index, builder) in self.projection.iter().zip(self.columns.iter_mut()) {
            match (index, builder) {
                (0, ProjectedColumnBuilder::Utf8(builder)) => {
                    builder.append_value(record.warc_id())
                }
                (1, ProjectedColumnBuilder::UInt64(builder)) => {
                    builder.append_value(record.content_length())
                }
                (2, ProjectedColumnBuilder::Timestamp(builder)) => {
                    builder.append_value(record.date().timestamp_millis())
                }
                (3, ProjectedColumnBuilder::Utf8(builder)) => {
                    builder.append_value(record.warc_type().to_string())
                }
                (16, ProjectedColumnBuilder::UInt64(builder)) => {
                    builder.append_option(optional_u64(
                        optional_header(record, WarcHeader::SegmentNumber),
                        "segment number",
                    )?)
                }
                (18, ProjectedColumnBuilder::UInt64(builder)) => {
                    builder.append_option(optional_u64(
                        optional_header(record, WarcHeader::SegmentTotalLength),
                        "segment total length",
                    )?)
                }
                (BODY_COLUMN_INDEX, ProjectedColumnBuilder::Body(_)) => {}
                (index, ProjectedColumnBuilder::Utf8(builder)) => {
                    builder.append_option(optional_header(
                        record,
                        string_header(index).ok_or_else(|| {
                            Error::message(format!("invalid WARC string column index: {index}"))
                        })?,
                    ));
                }
                _ => return Err(Error::message("WARC column builder type mismatch")),
            }
        }
        Ok(())
    }

    fn append_body(&mut self, body: Vec<u8>) -> Result<()> {
        let body_len = u32::try_from(body.len())
            .map_err(|_| Error::message("WARC body exceeds Arrow BinaryView limit"))?;
        let builder = self
            .body_position
            .and_then(|position| self.columns.get_mut(position))
            .ok_or_else(|| Error::message("WARC body column is not projected"))?;
        match builder {
            ProjectedColumnBuilder::Body(builder) => {
                let block = builder.append_block(body.into());
                builder.try_append_view(block, 0, body_len)?;
                self.body_bytes += body_len as usize;
                Ok(())
            }
            _ => Err(Error::message("WARC body column builder type mismatch")),
        }
    }

    fn finish_row(&mut self) {
        self.len += 1;
    }

    fn finish(self) -> Result<RecordBatch> {
        let columns = self
            .columns
            .into_iter()
            .map(ProjectedColumnBuilder::finish)
            .collect();
        Ok(RecordBatch::try_new(self.schema, columns)?)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    use arrow_array::BinaryViewArray;
    use flate2::{write::GzEncoder, Compression};
    use futures::TryStreamExt;

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
        assert_eq!(schema.field(1).data_type(), &DataType::UInt64);
        assert_eq!(
            schema.field(2).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(schema.field(16).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(18).data_type(), &DataType::UInt64);
        assert_eq!(schema.field(BODY_COLUMN_INDEX).name(), "body");
        assert_eq!(
            schema.field(BODY_COLUMN_INDEX).data_type(),
            &DataType::BinaryView
        );
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
                .downcast_ref::<BinaryViewArray>()
                .unwrap()
                .value(0),
            b"first"
        );

        let gzip_path = temp.join("records.warc.gz");
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(input.as_bytes()).unwrap();
        tokio::fs::write(gzip_path, encoder.finish().unwrap())
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

        let (schema, stream) = WarcSource::new(plain_path.to_string_lossy())
            .with_projection(vec![0, 2])
            .open()
            .await
            .unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "date");
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(batches[0].column_by_name("body").is_none());

        let (schema, stream) = WarcSource::new(plain_path.to_string_lossy())
            .with_projection(vec![])
            .open()
            .await
            .unwrap();
        assert_eq!(schema.fields().len(), warc_schema().fields().len());
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

    #[tokio::test]
    async fn reports_parser_panics_as_stream_errors() {
        let temp = test_directory().await;
        let path = temp.join("invalid-header.warc");
        let mut input = b"WARC/1.0\r\n\
            WARC-Type: response\r\n\
            WARC-Record-ID: <urn:uuid:bad-header>\r\n\
            WARC-Date: 2024-01-02T03:04:05Z\r\n\
            Content-Type: "
            .to_vec();
        input.extend_from_slice(
            b"\xff\r\n\
              Content-Length: 4\r\n\
              \r\n\
              body\r\n\
              \r\n",
        );
        tokio::fs::write(&path, input).await.unwrap();

        let (_, stream) = WarcSource::new(path.to_string_lossy())
            .with_projection(vec![0])
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
            1
        );

        let (_, mut stream) = WarcSource::new(path.to_string_lossy())
            .open()
            .await
            .unwrap();
        assert!(stream
            .try_next()
            .await
            .unwrap_err()
            .to_string()
            .contains("panic in WARC parser"));
        tokio::fs::remove_dir_all(temp).await.unwrap();
    }
}
