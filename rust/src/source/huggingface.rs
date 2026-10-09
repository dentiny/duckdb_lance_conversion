use std::sync::Arc;

use arrow_schema::SchemaRef;
#[cfg(test)]
use futures::TryStreamExt;
use opendal::{services::Http, Operator};
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use url::Url;

use super::parquet::open_parquet_paths;
use super::{BatchSource, BatchStream, ReadMetrics, SourceReadOptions};
use crate::error::ResultExt;
use crate::{Error, OpendalConfig, Result};

const DEFAULT_BATCH_SIZE: usize = 8192;
const PARQUET_REVISION: &str = "refs/convert/parquet";
const PARQUET_API_URL: &str = "https://datasets-server.huggingface.co/parquet";

pub struct HuggingFaceSource {
    dataset: String,
    config: String,
    split: String,
    token: Option<String>,
    batch_size: usize,
    preserve_insertion_order: bool,
    projection: Option<Vec<usize>>,
    read_options: SourceReadOptions,
    opendal_config: OpendalConfig,
    metrics: Arc<ReadMetrics>,
}

impl HuggingFaceSource {
    pub fn new(dataset: impl Into<String>) -> Self {
        Self {
            dataset: dataset.into(),
            config: "default".into(),
            split: "train".into(),
            token: None,
            batch_size: DEFAULT_BATCH_SIZE,
            preserve_insertion_order: true,
            projection: None,
            read_options: SourceReadOptions::default(),
            opendal_config: OpendalConfig::default(),
            metrics: Arc::new(ReadMetrics::default()),
        }
    }

    pub fn with_config(mut self, config: impl Into<String>) -> Self {
        self.config = config.into();
        self
    }

    pub fn with_split(mut self, split: impl Into<String>) -> Self {
        self.split = split.into();
        self
    }

    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_preserve_insertion_order(mut self, preserve: bool) -> Self {
        self.preserve_insertion_order = preserve;
        self
    }

    /// Reads only these top-level columns, in this order. An empty list reads
    /// every column.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_max_read_parallelism(mut self, max_read_parallelism: usize) -> Self {
        self.read_options.max_read_parallelism = max_read_parallelism;
        self
    }

    pub fn with_opendal_config(mut self, config: OpendalConfig) -> Self {
        self.opendal_config = config;
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<ReadMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    fn operator(&self, endpoint: &str) -> Result<Operator> {
        opendal_http_transport_reqwest::install_default();
        let mut builder = Http::default().endpoint(endpoint);
        if let Some(token) = &self.token {
            builder = builder.token(token);
        }
        self.opendal_config.apply(Operator::new(builder)?)
    }

    fn validate(&self) -> Result<()> {
        if self.dataset.is_empty() || self.config.is_empty() || self.split.is_empty() {
            return Err(Error::message(
                "dataset, config, and split must not be empty",
            ));
        }
        Ok(())
    }

    async fn parquet_files(&self) -> Result<(String, Vec<String>, u64)> {
        let client = reqwest::Client::new();
        let mut request = client.get(PARQUET_API_URL).query(&[
            ("dataset", self.dataset.as_str()),
            ("revision", PARQUET_REVISION),
            ("config", self.config.as_str()),
            ("split", self.split.as_str()),
        ]);
        if let Some(token) = &self.token {
            request = request.bearer_auth(token);
        }
        let response = request
            .send()
            .await?
            .error_for_status()?
            .json::<HuggingFaceParquetResponse>()
            .await?;
        let mut endpoint = None;
        let mut total_bytes = 0_u64;
        let mut paths = Vec::new();
        for file in response
            .parquet_files
            .into_iter()
            .filter(|file| file.config == self.config && file.split == self.split)
        {
            let (file_endpoint, path) = hugging_face_location(&file.url)?;
            if endpoint
                .as_ref()
                .is_some_and(|endpoint| endpoint != &file_endpoint)
            {
                return Err(Error::message(
                    "Hugging Face parquet files use different HTTP directories",
                ));
            }
            endpoint = Some(file_endpoint);
            total_bytes = total_bytes.saturating_add(file.size);
            paths.push(path);
        }
        paths.sort_unstable();
        let endpoint =
            endpoint.ok_or_else(|| Error::message("source contains no Parquet shards"))?;
        Ok((endpoint, paths, total_bytes))
    }
}

#[derive(Deserialize)]
struct HuggingFaceParquetResponse {
    parquet_files: Vec<HuggingFaceParquetFile>,
}

#[derive(Deserialize)]
struct HuggingFaceParquetFile {
    config: String,
    split: String,
    url: String,
    size: u64,
}

fn hugging_face_location(uri: &str) -> Result<(String, String)> {
    let url = Url::parse(uri)?;
    if url.scheme() != "https" || url.host_str() != Some("huggingface.co") {
        return Err(Error::message("invalid Hugging Face file URL"));
    }
    let (endpoint, encoded_path) = uri
        .rsplit_once('/')
        .ok_or_else(|| Error::message("invalid Hugging Face file URL"))?;
    let path = percent_decode_str(encoded_path)
        .decode_utf8()
        .map_err(|error| Error::message(error.to_string()))?
        .into_owned();
    Ok((endpoint.to_owned(), path))
}

#[cfg(test)]
fn parquet_paths(entries: impl IntoIterator<Item = (String, bool, u64)>) -> (Vec<String>, u64) {
    let mut total_bytes = 0_u64;
    let mut paths: Vec<_> = entries
        .into_iter()
        .filter_map(|(path, is_file, content_length)| {
            let is_parquet = is_file
                && path
                    .rsplit_once('.')
                    .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("parquet"));
            is_parquet.then(|| {
                total_bytes = total_bytes.saturating_add(content_length);
                path
            })
        })
        .collect();
    paths.sort_unstable();
    (paths, total_bytes)
}

impl BatchSource for HuggingFaceSource {
    async fn open(self) -> Result<(SchemaRef, BatchStream)> {
        if self.batch_size == 0 {
            return Err(Error::message("batch_size must be positive"));
        }
        self.validate()?;
        self.opendal_config.validate()?;
        let read_options = self.read_options.validate()?;
        let (endpoint, paths, total_bytes) = self.parquet_files().await?;
        self.metrics.set_total_bytes(total_bytes);
        let operator = self.operator(&endpoint)?;
        open_parquet_paths(
            operator,
            paths,
            self.batch_size,
            self.preserve_insertion_order,
            read_options.max_read_parallelism,
            self.projection,
            self.metrics,
        )
        .await
        .context(format!(
            "reading Hugging Face dataset '{}' config '{}' split '{}'",
            self.dataset, self.config, self.split
        ))
    }
}

#[cfg(test)]
async fn open_operator(
    operator: Operator,
    prefix: &str,
    batch_size: usize,
    preserve_insertion_order: bool,
    max_read_parallelism: usize,
    projection: Option<Vec<usize>>,
    metrics: Arc<ReadMetrics>,
) -> Result<(SchemaRef, BatchStream)> {
    let mut lister = operator.lister_with(prefix).recursive(true).await?;
    let mut entries = Vec::new();
    while let Some(entry) = lister.try_next().await? {
        entries.push((
            entry.path().to_owned(),
            entry.metadata().is_file(),
            entry.metadata().content_length(),
        ));
    }
    let (paths, total_bytes) = parquet_paths(entries);
    if paths.is_empty() {
        return Err(Error::message("source contains no Parquet shards"));
    }
    metrics.set_total_bytes(total_bytes);
    open_parquet_paths(
        operator,
        paths,
        batch_size,
        preserve_insertion_order,
        max_read_parallelism,
        projection,
        metrics,
    )
    .await
}

#[cfg(test)]
mod tests {
    use arrow_array::{FixedSizeBinaryArray, Int32Array, RecordBatch, StringArray};
    use arrow_schema::extension::{ExtensionType, Json, Uuid};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use opendal::services::Fs;
    use parquet::arrow::{arrow_writer::ArrowWriterOptions, AsyncArrowWriter};
    use tempfile::TempDir;
    use tokio::fs::{self, File};

    use super::super::test_util::write_parquet_shard;
    use super::*;

    fn fs_operator(root: &TempDir) -> Operator {
        Operator::new(Fs::default().root(root.path().to_str().unwrap())).unwrap()
    }

    #[test]
    fn defaults_match_sql_api() {
        let source = HuggingFaceSource::new("owner/dataset");
        assert_eq!(source.config, "default");
        assert_eq!(source.split, "train");
        assert!(source.token.is_none());
        assert!(source.preserve_insertion_order);
        assert_eq!(
            source.read_options.max_read_parallelism,
            super::super::DEFAULT_MAX_READ_PARALLELISM
        );
    }

    #[test]
    fn filters_and_sorts_parquet_shards() {
        let (paths, total_bytes) = parquet_paths([
            (
                /*path=*/ "default/train/2.parquet".into(),
                /*is_file=*/ true,
                /*content_length=*/ 20,
            ),
            (
                /*path=*/ "default/train/_metadata".into(),
                /*is_file=*/ true,
                /*content_length=*/ 40,
            ),
            (
                /*path=*/ "default/train/1.PARQUET".into(),
                /*is_file=*/ true,
                /*content_length=*/ 10,
            ),
            (
                /*path=*/ "default/train/directory.parquet".into(),
                /*is_file=*/ false,
                /*content_length=*/ 80,
            ),
        ]);
        assert_eq!(
            paths,
            vec!["default/train/1.PARQUET", "default/train/2.parquet"]
        );
        assert_eq!(total_bytes, 30);
    }

    #[tokio::test]
    async fn streams_multiple_shards() {
        let root = TempDir::new().unwrap();
        write_parquet_shard(
            &root.path().join("default/train/2.parquet"),
            "id",
            vec![3, 4],
            1,
        );
        write_parquet_shard(
            &root.path().join("default/train/1.parquet"),
            "id",
            vec![1, 2],
            1,
        );
        let metrics = Arc::new(ReadMetrics::default());
        let (_, stream) = open_operator(
            fs_operator(&root),
            "default/train/",
            1024,
            true,
            8,
            None,
            metrics.clone(),
        )
        .await
        .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
        let snapshot = metrics.snapshot();
        assert!(snapshot.bytes_read > 0);
        assert!(snapshot.total_bytes > 0);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );

        let (_, stream) = open_operator(
            fs_operator(&root),
            "default/train/",
            1024,
            false,
            8,
            None,
            Arc::new(ReadMetrics::default()),
        )
        .await
        .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let mut values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, [1, 2, 3, 4]);
    }

    async fn write_wide_shard(path: &std::path::Path, ids: Vec<i32>) {
        fs::create_dir_all(path.parent().unwrap()).await.unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Int32, false),
        ]));
        let names = ids.iter().map(|id| format!("row-{id}")).collect::<Vec<_>>();
        let scores = ids.iter().map(|id| id * 10).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int32Array::from(scores)),
            ],
        )
        .unwrap();
        let file = File::create(path).await.unwrap();
        let mut writer = AsyncArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn projection_reads_requested_columns_in_requested_order() {
        let root = TempDir::new().unwrap();
        write_wide_shard(&root.path().join("default/train/1.parquet"), vec![1, 2]).await;
        write_wide_shard(&root.path().join("default/train/2.parquet"), vec![3, 4]).await;

        for preserve_insertion_order in [true, false] {
            let (schema, stream) = open_operator(
                fs_operator(&root),
                "default/train/",
                1024,
                preserve_insertion_order,
                8,
                Some(vec![2, 0]),
                Arc::new(ReadMetrics::default()),
            )
            .await
            .unwrap();
            let names = |schema: &Schema| {
                schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>()
            };
            // DuckDB maps Arrow children to projected columns by position.
            assert_eq!(names(&*schema), ["score", "id"]);
            let batches = stream.try_collect::<Vec<_>>().await.unwrap();
            let mut rows = Vec::new();
            for batch in &batches {
                assert_eq!(names(&*batch.schema()), ["score", "id"]);
                let scores = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let ids = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                rows.extend(
                    scores
                        .values()
                        .iter()
                        .copied()
                        .zip(ids.values().iter().copied()),
                );
            }
            rows.sort_unstable();
            assert_eq!(rows, [(10, 1), (20, 2), (30, 3), (40, 4)]);
        }

        // Unselected column chunks are never fetched.
        let mut bytes_read = Vec::new();
        for projection in [None, Some(vec![0])] {
            let metrics = Arc::new(ReadMetrics::default());
            let (_, stream) = open_operator(
                fs_operator(&root),
                "default/train/",
                1024,
                true,
                8,
                projection,
                metrics.clone(),
            )
            .await
            .unwrap();
            stream.try_collect::<Vec<_>>().await.unwrap();
            bytes_read.push(metrics.snapshot().bytes_read);
        }
        assert!(
            bytes_read[1] < bytes_read[0],
            "projected read {} bytes, full read {} bytes",
            bytes_read[1],
            bytes_read[0]
        );
    }

    #[tokio::test]
    async fn projection_rejects_repeated_and_unknown_columns() {
        let root = TempDir::new().unwrap();
        write_wide_shard(&root.path().join("default/train/1.parquet"), vec![1]).await;
        for (projection, message) in [
            (vec![0, 0], "duplicate Parquet column index: 0"),
            (vec![3], "invalid Parquet column index: 3"),
        ] {
            for preserve_insertion_order in [true, false] {
                let error = match open_operator(
                    fs_operator(&root),
                    "default/train/",
                    1024,
                    preserve_insertion_order,
                    8,
                    Some(projection.clone()),
                    Arc::new(ReadMetrics::default()),
                )
                .await
                {
                    Ok(_) => panic!("projection {projection:?} should fail"),
                    Err(error) => error,
                };
                assert!(error.to_string().contains(message), "{error}");
            }
        }
    }

    #[tokio::test]
    async fn parquet_uuid_and_json_keep_canonical_extension_types() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("default/train/1.parquet");
        fs::create_dir_all(path.parent().unwrap()).await.unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::FixedSizeBinary(16), true).with_extension_type(Uuid),
            Field::new("doc", DataType::Utf8, true).with_extension_type(Json::default()),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(FixedSizeBinaryArray::try_from_iter([[0xAB_u8; 16]].into_iter()).unwrap()),
                Arc::new(StringArray::from(vec![r#"{"a":1}"#])),
            ],
        )
        .unwrap();
        // Without the embedded Arrow schema, the reader must derive the
        // extension types from the Parquet UUID and JSON logical types.
        let options = ArrowWriterOptions::new().with_skip_arrow_metadata(true);
        let file = File::create(&path).await.unwrap();
        let mut writer = AsyncArrowWriter::try_new_with_options(file, schema, options).unwrap();
        writer.write(&batch).await.unwrap();
        writer.close().await.unwrap();

        let (schema, _) = open_operator(
            fs_operator(&root),
            "default/train/",
            1024,
            true,
            8,
            None,
            Arc::new(ReadMetrics::default()),
        )
        .await
        .unwrap();
        assert_eq!(schema.field(0).extension_type_name(), Some(Uuid::NAME));
        assert_eq!(schema.field(1).extension_type_name(), Some(Json::NAME));
    }

    #[tokio::test]
    async fn rejects_empty_and_mismatched_shards() {
        let root = TempDir::new().unwrap();
        let error = match open_operator(
            fs_operator(&root),
            "default/train/",
            1024,
            true,
            8,
            None,
            Arc::new(ReadMetrics::default()),
        )
        .await
        {
            Ok(_) => panic!("empty source should fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("no Parquet shards"));

        write_parquet_shard(
            &root.path().join("default/train/1.parquet"),
            "id",
            vec![1],
            1,
        );
        write_parquet_shard(
            &root.path().join("default/train/2.parquet"),
            "other",
            vec![2],
            1,
        );
        let error = match open_operator(
            fs_operator(&root),
            "default/train/",
            1024,
            false,
            8,
            None,
            Arc::new(ReadMetrics::default()),
        )
        .await
        {
            Ok(_) => panic!("mismatched schemas should fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("schema does not match"));
    }
}
