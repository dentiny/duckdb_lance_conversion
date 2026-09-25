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
use crate::{Error, Result};

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
    read_options: SourceReadOptions,
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
            read_options: SourceReadOptions::default(),
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

    pub fn with_max_read_parallelism(mut self, max_read_parallelism: usize) -> Self {
        self.read_options.max_read_parallelism = max_read_parallelism;
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
        Ok(Operator::new(builder)?)
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
        metrics,
    )
    .await
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, RecordBatch};
    use futures::TryStreamExt;
    use opendal::services::Fs;
    use tempfile::TempDir;

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

    #[tokio::test]
    async fn rejects_empty_and_mismatched_shards() {
        let root = TempDir::new().unwrap();
        let error = match open_operator(
            fs_operator(&root),
            "default/train/",
            1024,
            true,
            8,
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
