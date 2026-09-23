use arrow_schema::SchemaRef;
use futures::TryStreamExt;
use opendal::{services::Hf, Operator};

use super::parquet::open_parquet_paths;
use super::{BatchSource, BatchStream};
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
}

impl HuggingFaceSource {
    pub fn new(dataset: impl Into<String>) -> Self {
        Self {
            dataset: dataset.into(),
            config: "default".into(),
            split: "train".into(),
            token: None,
            batch_size: DEFAULT_BATCH_SIZE,
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
        let prefix = self.prefix()?;
        let operator = self.operator()?;
        open_operator(operator, &prefix, self.batch_size)
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
    open_parquet_paths(operator, paths, batch_size).await
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use opendal::services::Fs;
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    use super::*;

    fn write_shard(path: &std::path::Path, field_name: &str, values: Vec<i32>) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            field_name,
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap();
        let mut writer = ArrowWriter::try_new(File::create(path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn fs_operator(root: &TempDir) -> Operator {
        Operator::new(Fs::default().root(root.path().to_str().unwrap())).unwrap()
    }

    #[test]
    fn defaults_match_sql_api() {
        let source = HuggingFaceSource::new("owner/dataset");
        assert_eq!(source.config, "default");
        assert_eq!(source.split, "train");
        assert!(source.token.is_none());
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
        write_shard(&root.path().join("default/train/2.parquet"), "id", vec![2]);
        write_shard(&root.path().join("default/train/1.parquet"), "id", vec![1]);
        let (_, stream) = open_operator(fs_operator(&root), "default/train/", 1024)
            .await
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
    }

    #[tokio::test]
    async fn rejects_empty_and_mismatched_shards() {
        let root = TempDir::new().unwrap();
        let error = match open_operator(fs_operator(&root), "default/train/", 1024).await {
            Ok(_) => panic!("empty source should fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("no Parquet shards"));

        write_shard(&root.path().join("default/train/1.parquet"), "id", vec![1]);
        write_shard(
            &root.path().join("default/train/2.parquet"),
            "other",
            vec![2],
        );
        let (_, mut stream) = open_operator(fs_operator(&root), "default/train/", 1024)
            .await
            .unwrap();
        assert!(stream.try_next().await.unwrap().is_some());
        let error = stream.try_next().await.unwrap_err();
        assert!(error.to_string().contains("schema does not match"));
    }

    #[tokio::test]
    async fn reads_small_public_dataset() {
        let (_, mut stream) = HuggingFaceSource::new("lhoestq/demo1")
            .open()
            .await
            .unwrap();
        assert!(stream.try_next().await.unwrap().is_some());
    }
}
