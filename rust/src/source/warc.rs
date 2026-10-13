use std::sync::{Arc, LazyLock};

use arrow_array::{
    builder::{BinaryViewBuilder, StringBuilder, TimestampMillisecondBuilder, UInt64Builder},
    ArrayRef, RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::io::AsyncBufRead;
use futures::{future, stream, StreamExt, TryStreamExt};
use opendal::Operator;
use percent_encoding::percent_decode_str;
use tokio::sync::mpsc;
use tokio_util::task::AbortOnDropHandle;
use url::Url;
use warc_rs::{GzipReader, Record, StreamingBody, WarcHeader, WarcReader};

use super::warc_index::open_indexed_warc;
use super::warc_segment::WarcSegment;
use super::{read_object, stream_object, BatchSource, BatchStream, ReadMetrics, SourceReadOptions};
use crate::error::ResultExt;
use crate::storage::{http_operator, is_http_uri, OpendalStorage};
use crate::{Error, OpendalConfig, Result, S3StorageConfig};

const DEFAULT_BATCH_SIZE: usize = 8192;
// A batch crosses Arrow -> DuckDB -> Arrow and may remain queued while Lance
// encodes it, so body bytes dominate peak memory much more than row count.
const MAX_BATCH_BODY_BYTES: usize = 16 * 1024 * 1024;
const CHANNEL_CAPACITY: usize = 1;
const BODY_COLUMN_INDEX: usize = 19;

/// Decompressed WARC bytes ready for record parsing.
pub(super) type WarcInput = Box<dyn AsyncBufRead + Unpin + Send>;
type WarcRecord<'r> = Record<StreamingBody<'r, WarcInput>>;

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

fn optional_header(record: &WarcRecord<'_>, header: WarcHeader) -> Option<String> {
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
    opendal_config: OpendalConfig,
    projection: Option<Vec<usize>>,
    read_options: SourceReadOptions,
    index_path: Option<String>,
    metrics: Arc<ReadMetrics>,
}

impl WarcSource {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            batch_size: DEFAULT_BATCH_SIZE,
            s3_config: None,
            opendal_config: OpendalConfig::default(),
            projection: None,
            read_options: SourceReadOptions::default(),
            index_path: None,
            metrics: Arc::new(ReadMetrics::default()),
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

    pub fn with_opendal_config(mut self, config: OpendalConfig) -> Self {
        self.opendal_config = config;
        self
    }

    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_max_read_parallelism(mut self, max_read_parallelism: usize) -> Self {
        self.read_options.max_read_parallelism = max_read_parallelism;
        self
    }

    pub fn with_index_path(mut self, index_path: impl Into<String>) -> Self {
        self.index_path = Some(index_path.into());
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<ReadMetrics>) -> Self {
        self.metrics = metrics;
        self
    }
}

impl BatchSource for WarcSource {
    async fn open(self) -> Result<(SchemaRef, BatchStream)> {
        if self.batch_size == 0 {
            return Err(Error::message("batch_size must be positive"));
        }
        let read_options = self.read_options.validate()?;
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
        let (operator, path) =
            warc_location(&self.path, self.s3_config.as_ref(), &self.opendal_config)?;
        if path.is_empty() {
            return Err(Error::message("WARC input must not be a storage root"));
        }
        let segment = if is_warc_file(&path) {
            None
        } else {
            WarcSegment::parse(&path)
        };
        if let Some(segment) = segment {
            if self.index_path.is_some() {
                return Err(Error::message(
                    "index_path is not supported for WARC segments",
                ));
            }
            let files = segment.files(&operator).await?;
            let batches = open_warc_files(
                operator,
                files,
                self.batch_size,
                projection,
                schema.clone(),
                read_options.max_read_parallelism,
                self.metrics,
            )
            .await?;
            return Ok((schema, batches));
        }
        let gzipped = is_gzipped(&path);
        if let Some(index_path) = self.index_path {
            return open_indexed_warc(
                operator,
                path,
                gzipped,
                &index_path,
                self.s3_config.as_ref(),
                &self.opendal_config,
                self.batch_size,
                projection,
                schema,
                read_options.max_read_parallelism,
                self.metrics,
            )
            .await;
        }
        let reader = read_object(&operator, &path, self.metrics).await?;
        let batches = spawn_records(
            warc_input(reader, gzipped),
            self.batch_size,
            projection,
            schema.clone(),
        );
        Ok((schema, batches))
    }
}

pub(super) fn is_gzipped(path: &str) -> bool {
    path.to_ascii_lowercase().ends_with(".gz")
}

fn is_warc_file(path: &str) -> bool {
    let path = path.to_ascii_lowercase();
    path.ends_with(".warc") || path.ends_with(".warc.gz")
}

/// Returns an operator and the object path of `path` within it.
fn warc_location(
    path: &str,
    s3_config: Option<&S3StorageConfig>,
    opendal_config: &OpendalConfig,
) -> Result<(Operator, String)> {
    if !is_http_uri(path) {
        let storage = OpendalStorage::from_path(path, s3_config, opendal_config)?;
        return Ok((storage.operator, storage.object_path.to_string()));
    }
    let url = Url::parse(path)?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err(Error::message(
            "WARC HTTP(S) URLs must not include a query or fragment",
        ));
    }
    let operator = http_operator(&url[..url::Position::BeforePath], None, opendal_config)?;
    let object_path = percent_decode_str(url.path())
        .decode_utf8()
        .map_err(|error| Error::message(error.to_string()))?
        .trim_start_matches('/')
        .to_owned();
    Ok((operator, object_path))
}

/// Streams up to `max_read_parallelism` files at once. Records keep their order
/// within a file, but batches from different files interleave.
async fn open_warc_files(
    operator: Operator,
    files: Vec<String>,
    batch_size: usize,
    projection: Vec<usize>,
    schema: SchemaRef,
    max_read_parallelism: usize,
    metrics: Arc<ReadMetrics>,
) -> Result<BatchStream> {
    let files = stream::iter(files)
        .map(|path| {
            let operator = operator.clone();
            async move {
                let size = operator.stat(&path).await.context(&path)?.content_length();
                Ok::<_, Error>((path, size))
            }
        })
        .buffer_unordered(max_read_parallelism)
        .try_collect::<Vec<_>>()
        .await?;
    metrics.set_total_bytes(files.iter().map(|(_, size)| size).sum());
    let batches = stream::iter(files)
        .map(move |(path, size)| {
            let operator = operator.clone();
            let projection = projection.clone();
            let schema = schema.clone();
            let metrics = metrics.clone();
            stream::once(async move {
                let reader = stream_object(&operator, &path, size, metrics)
                    .await
                    .context(&path)?;
                let records = spawn_records(
                    warc_input(reader, is_gzipped(&path)),
                    batch_size,
                    projection,
                    schema,
                );
                Ok::<_, Error>(records.map(move |batch| batch.context(&path)))
            })
            .try_flatten()
            .boxed()
        })
        .flatten_unordered(max_read_parallelism);
    Ok(Box::pin(batches))
}

pub(super) fn warc_input(
    reader: impl AsyncBufRead + Unpin + Send + 'static,
    gzipped: bool,
) -> WarcInput {
    if gzipped {
        Box::new(GzipReader::new(reader))
    } else {
        Box::new(reader)
    }
}

/// Parses `input` in its own runtime task, so parsing and decompression
/// continue while the consumer is busy. Dropping the returned stream aborts
/// the task, and a panic surfaces as an error rather than truncated output.
pub(super) fn spawn_records(
    input: WarcInput,
    batch_size: usize,
    projection: Vec<usize>,
    schema: SchemaRef,
) -> BatchStream {
    let (sender, mut receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let task = AbortOnDropHandle::new(tokio::spawn(async move {
        if let Err(error) = stream_records(input, batch_size, projection, schema, &sender).await {
            let _ = sender.send(Err(error)).await;
        }
    }));
    let batches = stream::poll_fn(move |cx| receiver.poll_recv(cx));
    let completion = stream::once(task).filter_map(|result| {
        future::ready(
            result
                .err()
                .map(|error| Err(Error::message(format!("panic in WARC parser: {error}")))),
        )
    });
    Box::pin(batches.chain(completion))
}

async fn stream_records(
    input: WarcInput,
    batch_size: usize,
    projection: Vec<usize>,
    schema: SchemaRef,
    sender: &mpsc::Sender<Result<RecordBatch>>,
) -> Result<()> {
    let include_body = projection.contains(&BODY_COLUMN_INDEX);
    let mut reader = WarcReader::new(input);
    let mut batch = WarcBatchBuilder::new(batch_size, projection, schema)?;
    let mut records = reader.stream_records();
    while let Some(record) = records.next_item().await {
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
            && !send_batch(batch.take_empty(batch_size)?, sender).await?
        {
            return Ok(());
        }
        batch.append_metadata(&record)?;
        if include_body {
            let body = record
                .into_buffered()
                .await
                .map_err(|error| Error::message(format!("WARC body error: {error}")))?
                .into_raw_parts()
                .1;
            batch.append_body(body)?;
        }
        batch.finish_row();
        if (batch.len() == batch_size || batch.body_bytes() >= MAX_BATCH_BODY_BYTES)
            && !send_batch(batch.take_empty(batch_size)?, sender).await?
        {
            return Ok(());
        }
    }
    if !batch.is_empty() {
        send_batch(batch, sender).await?;
    }
    Ok(())
}

/// Returns `false` once the consumer has dropped the stream.
async fn send_batch(
    batch: WarcBatchBuilder,
    sender: &mpsc::Sender<Result<RecordBatch>>,
) -> Result<bool> {
    Ok(sender.send(Ok(batch.finish()?)).await.is_ok())
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

pub(super) struct WarcBatchBuilder {
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

    fn append_metadata(&mut self, record: &WarcRecord<'_>) -> Result<()> {
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

    pub(super) fn finish(self) -> Result<RecordBatch> {
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
    use std::sync::atomic::{AtomicU64, Ordering};

    use arrow_array::{BinaryViewArray, StringArray};
    use futures::TryStreamExt;

    use super::super::test_util::{gzip, warc_record as record, write_indexed_warc_fixture};
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

        tokio::fs::write(temp.join("records.warc.gz"), gzip(input.as_bytes()))
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
    async fn streams_indexed_gzip_members_in_offset_order() {
        let temp = test_directory().await;
        let fixture = write_indexed_warc_fixture(
            &temp,
            "indexed",
            &[(1, "first"), (2, "second")],
            &[1, 0],
            b"index gap",
        )
        .await;

        let (_, stream) = WarcSource::new(fixture.archive_path.to_string_lossy())
            .with_index_path(fixture.index_path.to_string_lossy())
            .with_max_read_parallelism(2)
            .with_batch_size(1)
            .open()
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(ids(&batches), ["<urn:uuid:1>", "<urn:uuid:2>"]);

        tokio::fs::remove_dir_all(temp).await.unwrap();
    }

    fn ids(batches: &[RecordBatch]) -> Vec<String> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|id| id.unwrap().to_owned())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[tokio::test]
    async fn streams_common_crawl_segment_files() {
        let temp = test_directory().await;
        let crawl = temp.join("crawl-data/c");
        let warc_dir = crawl.join("segments/1/warc");
        tokio::fs::create_dir_all(&warc_dir).await.unwrap();
        let manifest = gzip(
            b"crawl-data/c/segments/1/warc/b.warc.gz\n\
              crawl-data/c/segments/1/warc/a.warc\n\
              crawl-data/c/segments/3/warc/missing.warc\n",
        );
        tokio::fs::write(crawl.join("warc.paths.gz"), manifest)
            .await
            .unwrap();
        let records = format!("{}{}", record(1, "first"), record(2, "second"));
        tokio::fs::write(warc_dir.join("b.warc.gz"), gzip(records.as_bytes()))
            .await
            .unwrap();
        tokio::fs::write(warc_dir.join("a.warc"), record(3, "third"))
            .await
            .unwrap();

        let metrics = Arc::new(ReadMetrics::default());
        let (_, stream) = WarcSource::new(crawl.join("segments/1/").to_string_lossy())
            .with_batch_size(1)
            .with_metrics(metrics.clone())
            .open()
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let mut segment_ids = ids(&batches);
        // Files interleave, but each keeps its record order.
        let first = segment_ids.iter().position(|id| id == "<urn:uuid:1>");
        let second = segment_ids.iter().position(|id| id == "<urn:uuid:2>");
        assert!(first < second, "{segment_ids:?}");
        segment_ids.sort_unstable();
        assert_eq!(
            segment_ids,
            ["<urn:uuid:1>", "<urn:uuid:2>", "<urn:uuid:3>"]
        );
        let snapshot = metrics.snapshot();
        assert!(snapshot.total_bytes > 0);
        assert_eq!(snapshot.bytes_read, snapshot.total_bytes);

        let file = crawl.join("segments/direct.warc");
        tokio::fs::write(&file, record(4, "fourth")).await.unwrap();
        let (_, stream) = WarcSource::new(file.to_string_lossy())
            .open()
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(ids(&batches), ["<urn:uuid:4>"]);

        let error = WarcSource::new(crawl.join("segments/3").to_string_lossy())
            .open()
            .await
            .err()
            .unwrap();
        assert!(error
            .to_string()
            .contains("crawl-data/c/segments/3/warc/missing.warc"));

        let error = WarcSource::new(crawl.join("segments/1").to_string_lossy())
            .with_index_path(temp.join("index.cdxj").to_string_lossy())
            .open()
            .await
            .err()
            .unwrap();
        assert!(error
            .to_string()
            .contains("not supported for WARC segments"));

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
    async fn reads_invalid_utf8_headers_lossily() {
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
            .open()
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.len(), 1);
        let content_type = batches[0]
            .column_by_name("content_type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(content_type, "\u{FFFD}");
        tokio::fs::remove_dir_all(temp).await.unwrap();
    }
}
