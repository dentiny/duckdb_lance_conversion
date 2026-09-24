use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use opendal::{services::Hf, Operator};

use super::parquet::open_parquet_paths;
use super::{BatchSource, BatchStream, SourceReadOptions};
use crate::error::ResultExt;
use crate::{Error, Result};

const DEFAULT_BATCH_SIZE: usize = 8192;
const PARQUET_REVISION: &str = "refs/convert/parquet";

pub struct HuggingFaceSource {
    dataset: String,
    config: String,
    split: String,
    token: Option<String>,
    batch_size: usize,
    preserve_insertion_order: bool,
    read_options: SourceReadOptions,
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

    fn operator(&self) -> Result<Operator> {
        opendal_http_transport_reqwest::install_default();
        let mut builder = Hf::default()
            .repo_type("dataset")
            .repo_id(&self.dataset)
            .revision(PARQUET_REVISION)
            .download_mode("http");
        if let Some(token) = &self.token {
            builder = builder.token(token);
        }
        Ok(Operator::new(builder)?)
    }

    fn prefix(&self) -> Result<String> {
        if self.dataset.is_empty() || self.config.is_empty() || self.split.is_empty() {
            return Err(Error::message(
                "dataset, config, and split must not be empty",
            ));
        }
        Ok(format!("{}/{}/", self.config, self.split))
    }
}

fn parquet_paths(entries: impl IntoIterator<Item = (String, bool)>) -> Vec<String> {
    let mut paths: Vec<_> = entries
        .into_iter()
        .filter_map(|(path, is_file)| {
            (is_file
                && path
                    .rsplit_once('.')
                    .is_some_and(|(_, extension)| extension.eq_ignore_ascii_case("parquet")))
            .then_some(path)
        })
        .collect();
    paths.sort_unstable();
    paths
}

impl BatchSource for HuggingFaceSource {
    async fn open(self) -> Result<(SchemaRef, BatchStream)> {
        if self.batch_size == 0 {
            return Err(Error::message("batch_size must be positive"));
        }
        let read_options = self.read_options.validate()?;
        let prefix = self.prefix()?;
        let operator = self.operator()?;
        open_operator(
            operator,
            &prefix,
            self.batch_size,
            self.preserve_insertion_order,
            read_options.max_read_parallelism,
        )
        .await
        .context(format!(
            "reading Hugging Face dataset '{}' config '{}' split '{}'",
            self.dataset, self.config, self.split
        ))
    }
}

async fn open_operator(
    operator: Operator,
    prefix: &str,
    batch_size: usize,
    preserve_insertion_order: bool,
    max_read_parallelism: usize,
) -> Result<(SchemaRef, BatchStream)> {
    let mut lister = operator.lister_with(prefix).recursive(true).await?;
    let mut entries = Vec::new();
    while let Some(entry) = lister.try_next().await? {
        entries.push((entry.path().to_owned(), entry.metadata().is_file()));
    }
    let paths = parquet_paths(entries);
    if paths.is_empty() {
        return Err(Error::message("source contains no Parquet shards"));
    }
    open_parquet_paths(
        operator,
        paths,
        batch_size,
        preserve_insertion_order,
        max_read_parallelism,
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
        let paths = parquet_paths([
            ("default/train/2.parquet".into(), true),
            ("default/train/_metadata".into(), true),
            ("default/train/1.PARQUET".into(), true),
            ("default/train/directory.parquet".into(), false),
        ]);
        assert_eq!(
            paths,
            vec!["default/train/1.PARQUET", "default/train/2.parquet"]
        );
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
        let (_, stream) = open_operator(fs_operator(&root), "default/train/", 1024, true, 8)
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );

        let (_, stream) = open_operator(fs_operator(&root), "default/train/", 1024, false, 8)
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
        let error = match open_operator(fs_operator(&root), "default/train/", 1024, true, 8).await {
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
        let error = match open_operator(fs_operator(&root), "default/train/", 1024, false, 8).await
        {
            Ok(_) => panic!("mismatched schemas should fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("schema does not match"));
    }
}
