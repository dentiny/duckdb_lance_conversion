use std::path::Path;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use atif_io::{InputFormat, TrajectoryReader};
use futures::{stream, Stream, StreamExt, TryStreamExt};

use super::{decompressed_input, is_gzipped, read_object, BatchSource, BatchStream, ReadMetrics};
use crate::error::ResultExt;
use crate::storage::{has_glob, InputFile};
use crate::{Error, OpendalConfig, Result, S3StorageConfig};

// Each row holds a whole trajectory, including its raw JSON, so batches are
// bounded by row count much more tightly than for flat sources.
const BATCH_SIZE: usize = 256;
pub fn atif_schema() -> SchemaRef {
    atif_arrow::trajectory_schema()
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
        let files = if has_glob(&self.path) {
            InputFile::glob(&self.path, self.s3_config.as_ref(), &self.opendal_config).await?
        } else {
            vec![InputFile::open(
                &self.path,
                self.s3_config.as_ref(),
                &self.opendal_config,
            )?]
        };
        let formats = files
            .iter()
            .map(|file| input_format(&file.opendal_path, self.format.as_deref()))
            .collect::<Result<Vec<_>>>()?;
        let full_schema = atif_schema();
        let projection = self.projection.filter(|projection| !projection.is_empty());
        let schema = match &projection {
            Some(projection) => Arc::new(full_schema.project(projection)?),
            None => full_schema,
        };
        let parallelism = std::thread::available_parallelism().map_or(1, usize::from);
        let file_count = files.len() as u64;
        let metrics = self.metrics;
        // Files are read one at a time, so trajectories keep their file order.
        let batches = stream::iter(files.into_iter().zip(formats))
            .map(move |(file, format)| {
                stream::once(read_trajectories(
                    file,
                    format,
                    file_count,
                    parallelism,
                    metrics.clone(),
                ))
                .try_flatten()
            })
            .flatten()
            // The ATIF converter always produces every column; projection
            // only trims the batches DuckDB receives.
            .map(move |batch| match &projection {
                Some(projection) => Ok(batch?.project(projection)?),
                None => batch,
            });
        Ok((schema, Box::pin(batches)))
    }
}

async fn read_trajectories(
    file: InputFile,
    format: InputFormat,
    file_count: u64,
    parallelism: usize,
    metrics: Arc<ReadMetrics>,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send> {
    let size = file
        .operator
        .stat(&file.opendal_path)
        .await
        .context(&file.uri)?
        .content_length();
    metrics.add_object(size, file_count);
    let reader = read_object(&file.operator, &file.opendal_path, size, metrics)
        .await
        .context(&file.uri)?;
    let reader = decompressed_input(reader, is_gzipped(&file.opendal_path));
    let reader = TrajectoryReader::new(reader, format, BATCH_SIZE, Some(file.uri))
        .map_err(|error| Error::message(format!("ATIF reader: {error}")))?;
    Ok(reader
        .into_parallel_stream(parallelism)
        .map_err(|error| Error::message(format!("ATIF read error: {error}"))))
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
        let indices = column::<UInt64Array>(&batches[0], "source_record_index");
        assert_eq!(indices.values(), &[1, 2]);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.bytes_read, snapshot.total_bytes);
    }

    #[tokio::test]
    async fn reads_glob_matches_in_path_order() {
        let temp = tempfile::TempDir::new().unwrap();
        tokio::fs::write(temp.path().join("b.json"), DOCUMENT)
            .await
            .unwrap();
        tokio::fs::write(
            temp.path().join("a.jsonl.gz"),
            gzip(format!("{DOCUMENT}\n{DOCUMENT}\n").as_bytes()),
        )
        .await
        .unwrap();
        tokio::fs::write(temp.path().join("c.txt"), "ignored")
            .await
            .unwrap();
        let metrics = Arc::new(ReadMetrics::default());
        let (_, stream) = AtifSource::new(format!("{}/*.json*", temp.path().display()))
            .with_metrics(metrics.clone())
            .open()
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let rows = batches
            .iter()
            .flat_map(|batch| {
                let uris = column::<StringArray>(batch, "source_uri");
                let indices = column::<UInt64Array>(batch, "source_record_index");
                (0..batch.num_rows())
                    .map(|row| {
                        let uri = uris.value(row);
                        (
                            uri[uri.rfind('/').unwrap() + 1..].to_owned(),
                            indices.value(row),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            [
                ("a.jsonl.gz".to_owned(), 1),
                ("a.jsonl.gz".to_owned(), 2),
                ("b.json".to_owned(), 1)
            ]
        );
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
