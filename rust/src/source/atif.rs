use std::path::Path;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use atif_io::{InputFormat, TrajectoryReader};
use futures::{StreamExt, TryStreamExt};

use super::{decompressed_input, is_gzipped, read_object, BatchSource, BatchStream, ReadMetrics};
use crate::storage::InputFile;
use crate::{Error, OpendalConfig, Result, S3StorageConfig};

// Each row holds a whole trajectory, including its raw JSON, so batches are
// bounded by row count much more tightly than for flat sources.
const BATCH_SIZE: usize = 256;
mod media;
use media::MediaResolver;

pub fn atif_schema() -> SchemaRef {
    media::embedded_schema()
}

/// `json` holds one trajectory and `jsonl` or `ndjson` one per line. Without
/// an explicit format, the file extension decides, looking past a `.gz` suffix.
fn input_format(path: &str, format: Option<&str>) -> Result<InputFormat> {
    let format = format
        .or_else(|| {
            let mut path = Path::new(path);
            if is_gzipped(path.to_str()?) {
                path = Path::new(path.file_stem()?);
            }
            path.extension()?.to_str()
        })
        .unwrap_or_default()
        .to_ascii_lowercase();
    match format.as_str() {
        "json" => Ok(InputFormat::Json),
        "jsonl" | "ndjson" => Ok(InputFormat::JsonLines),
        _ => Err(Error::message(format!(
            "unsupported ATIF format '{format}' for '{path}'; set format to 'json' or 'jsonl'"
        ))),
    }
}

pub struct AtifSource {
    path: String,
    format: Option<String>,
    s3_config: Option<S3StorageConfig>,
    opendal_config: OpendalConfig,
    projection: Option<Vec<usize>>,
    metrics: Arc<ReadMetrics>,
}

impl AtifSource {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            format: None,
            s3_config: None,
            opendal_config: OpendalConfig::default(),
            projection: None,
            metrics: Arc::new(ReadMetrics::default()),
        }
    }

    /// `json` or `jsonl`; inferred from the file extension when unset. A
    /// `.gz` file is decompressed either way.
    pub fn with_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
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

    /// Top-level column indices in output order; empty reads every column.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<ReadMetrics>) -> Self {
        self.metrics = metrics;
        self
    }
}

impl BatchSource for AtifSource {
    async fn open(self) -> Result<(SchemaRef, BatchStream)> {
        let input = InputFile::open(&self.path, self.s3_config.as_ref(), &self.opendal_config)?;
        let format = input_format(&input.opendal_path, self.format.as_deref())?;
        let full_schema = atif_schema();
        let projection = self.projection.filter(|projection| !projection.is_empty());
        let schema = match &projection {
            Some(projection) => Arc::new(full_schema.project(projection)?),
            None => full_schema.clone(),
        };
        let size = input
            .operator
            .stat(&input.opendal_path)
            .await?
            .content_length();
        self.metrics.set_total_bytes(size);
        let reader = read_object(
            &input.operator,
            &input.opendal_path,
            size,
            self.metrics.clone(),
        )
        .await?;
        let reader = decompressed_input(reader, is_gzipped(&input.opendal_path));
        let media_index = full_schema.index_of("steps")?;
        let needs_media = projection
            .as_ref()
            .is_none_or(|columns| columns.contains(&media_index));
        // One full document per batch when media is projected. Avoid buffering
        // hundreds of MB-scale trajectories in the parser's parallel queue.
        let batch_size = if needs_media { 1 } else { BATCH_SIZE };
        let reader = TrajectoryReader::new(reader, format, batch_size, Some(input.uri.clone()))
            .map_err(|error| Error::message(format!("ATIF reader: {error}")))?;
        let resolver = MediaResolver {
            document: input.uri,
            s3_config: self.s3_config,
            opendal_config: self.opendal_config,
            metrics: self.metrics,
        };
        let parallelism = if needs_media {
            1
        } else {
            std::thread::available_parallelism().map_or(1, usize::from)
        };
        let batches = reader
            .into_parallel_stream(parallelism)
            .map_err(|error| Error::message(format!("ATIF read error: {error}")))
            .then(move |batch| {
                let resolver = resolver.clone();
                let projection = projection.clone();
                let media_schema = full_schema.clone();
                async move {
                    let mut batch = batch?;
                    if needs_media {
                        batch = resolver.resolve(batch, media_schema).await?;
                    } else {
                        batch
                            .schema_metadata_mut()
                            .extend(media_schema.metadata().clone());
                    }
                    match projection {
                        Some(columns) => Ok(batch.project(&columns)?),
                        None => Ok(batch),
                    }
                }
            });
        Ok((schema, Box::pin(batches)))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{RecordBatch, StringArray, UInt64Array};

    use super::super::test_util::gzip;
    use super::*;

    const DOCUMENT: &str = r#"{"schema_version":"ATIF-v1.8","agent":{"name":"test","version":"1"},"steps":[{"step_id":1,"source":"user","message":"hello"}]}"#;

    fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<T>()
            .unwrap()
    }

    #[test]
    fn infers_format_from_extension() {
        assert!(matches!(
            input_format("a/trajectory.JSON", None).unwrap(),
            InputFormat::Json
        ));
        assert!(matches!(
            input_format("a/trajectories.jsonl", None).unwrap(),
            InputFormat::JsonLines
        ));
        assert!(matches!(
            input_format("a/trajectories.txt", Some("ndjson")).unwrap(),
            InputFormat::JsonLines
        ));
        assert!(matches!(
            input_format("a/trajectory.json.GZ", None).unwrap(),
            InputFormat::Json
        ));
        assert!(matches!(
            input_format("a/trajectories.ndjson.gz", None).unwrap(),
            InputFormat::JsonLines
        ));
        assert!(matches!(
            input_format("a/trajectories.gz", Some("jsonl")).unwrap(),
            InputFormat::JsonLines
        ));
        for (path, format) in [
            ("a/trajectories.txt", None),
            ("a/trajectory.json", Some("csv")),
            ("a/trajectories.gz", None),
            ("a/trajectories.txt.gz", None),
        ] {
            assert!(input_format(path, format)
                .unwrap_err()
                .to_string()
                .contains("unsupported ATIF format"));
        }
    }

    #[tokio::test]
    async fn reads_jsonl_with_projection_and_provenance() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("trajectories.jsonl");
        tokio::fs::write(&path, format!("{DOCUMENT}\n\n{DOCUMENT}\n"))
            .await
            .unwrap();
        let schema = atif_schema();
        let projection = vec![
            schema.index_of("source_record_index").unwrap(),
            schema.index_of("source_uri").unwrap(),
        ];
        let metrics = Arc::new(ReadMetrics::default());
        let (schema, stream) = AtifSource::new(path.to_string_lossy())
            .with_projection(projection)
            .with_metrics(metrics.clone())
            .open()
            .await
            .unwrap();
        assert_eq!(schema.field(0).name(), "source_record_index");
        assert_eq!(schema.field(1).name(), "source_uri");
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema(), schema);
        let indices = column::<UInt64Array>(&batches[0], "source_record_index");
        assert_eq!(indices.values(), &[1, 2]);
        let source_uri = column::<StringArray>(&batches[0], "source_uri").value(0);
        assert!(std::path::Path::new(source_uri).is_absolute());
        assert!(source_uri.ends_with("trajectories.jsonl"));
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.bytes_read, snapshot.total_bytes);
    }

    #[tokio::test]
    async fn reads_gzipped_jsonl_across_members() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("trajectories.jsonl.gz");
        let mut bytes = gzip(format!("{DOCUMENT}\n").as_bytes());
        bytes.extend(gzip(format!("{DOCUMENT}\n").as_bytes()));
        tokio::fs::write(&path, bytes).await.unwrap();
        let metrics = Arc::new(ReadMetrics::default());
        let (_, stream) = AtifSource::new(path.to_string_lossy())
            .with_metrics(metrics.clone())
            .open()
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let indices = batches
            .iter()
            .flat_map(|batch| {
                column::<UInt64Array>(batch, "source_record_index")
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(indices, vec![1, 2]);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.bytes_read, snapshot.total_bytes);
    }

    #[tokio::test]
    async fn reports_invalid_trajectories() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("trajectory.json");
        tokio::fs::write(&path, r#"{"schema_version":"ATIF-v1.8"}"#)
            .await
            .unwrap();
        let (_, mut stream) = AtifSource::new(path.to_string_lossy())
            .open()
            .await
            .unwrap();
        assert!(stream
            .try_next()
            .await
            .unwrap_err()
            .to_string()
            .contains("ATIF read error"));
    }
}
