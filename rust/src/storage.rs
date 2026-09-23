use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreParams, ObjectStoreProvider,
    DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_DOWNLOAD_RETRY_COUNT, DEFAULT_LOCAL_IO_PARALLELISM,
};
use object_store::ObjectStore as ArrowObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{
    services::{Fs, S3},
    Operator,
};
use url::Url;

#[derive(Clone, Debug)]
pub struct S3StorageConfig {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub key_id: Option<String>,
    pub secret: Option<String>,
    pub session_token: Option<String>,
    pub use_ssl: bool,
    pub virtual_host_style: bool,
}

impl Default for S3StorageConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            region: None,
            key_id: None,
            secret: None,
            session_token: None,
            use_ssl: true,
            virtual_host_style: false,
        }
    }
}

#[derive(Clone)]
pub(crate) struct OpendalStorage {
    pub operator: Operator,
    pub object_store: Arc<dyn ArrowObjectStore>,
    pub object_path: object_store::path::Path,
    pub location: Url,
}

#[derive(Debug)]
pub(crate) struct OpendalStoreProvider {
    object_store: Arc<dyn ArrowObjectStore>,
}

impl OpendalStoreProvider {
    pub fn new(object_store: Arc<dyn ArrowObjectStore>) -> Self {
        Self { object_store }
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for OpendalStoreProvider {
    async fn new_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> lance::Result<LanceObjectStore> {
        let io_parallelism = if base_path.scheme() == "file" {
            DEFAULT_LOCAL_IO_PARALLELISM
        } else {
            DEFAULT_CLOUD_IO_PARALLELISM
        };
        Ok(LanceObjectStore::new(
            self.object_store.clone(),
            base_path,
            params.resolved_block_size()?,
            params.object_store_wrapper.clone(),
            params.use_constant_size_upload_parts,
            params.list_is_lexically_ordered.unwrap_or(false),
            io_parallelism,
            DEFAULT_DOWNLOAD_RETRY_COUNT,
            params.storage_options(),
        ))
    }
}

impl OpendalStorage {
    pub fn from_path(path: &str, s3_config: Option<&S3StorageConfig>) -> Result<Self> {
        if is_s3_uri(path) {
            let config = s3_config.context("S3 path requires S3 storage configuration")?;
            Self::from_s3_uri(path, config)
        } else {
            ensure!(
                s3_config.is_none(),
                "S3 storage configuration can only be used with an s3:// path"
            );
            Self::from_local_path(path)
        }
    }

    fn from_local_path(path: &str) -> Result<Self> {
        opendal::install_default();
        let absolute = if std::path::Path::new(path).is_absolute() {
            std::path::PathBuf::from(path)
        } else {
            std::env::current_dir()?.join(path)
        };
        let location = Url::from_file_path(&absolute)
            .map_err(|_| anyhow::anyhow!("invalid local storage path: {}", absolute.display()))?;
        let operator =
            Operator::new(Fs::default().root("/")).context("initializing OpenDAL local storage")?;
        let object_store = Arc::new(OpendalStore::new(operator.clone()));
        let object_path =
            object_store::path::Path::from(absolute.to_string_lossy().trim_start_matches('/'));
        Ok(Self {
            operator,
            object_store,
            object_path,
            location,
        })
    }

    fn from_s3_uri(uri: &str, config: &S3StorageConfig) -> Result<Self> {
        let location = Url::parse(uri).context("invalid S3 URI")?;
        ensure!(location.scheme() == "s3", "expected an s3:// URI");
        let bucket = location
            .host_str()
            .filter(|bucket| !bucket.is_empty())
            .context("S3 URI must include a bucket")?;
        ensure!(
            config.key_id.is_some() == config.secret.is_some(),
            "S3 key ID and secret must be provided together"
        );
        ensure!(
            config.session_token.is_none() || config.key_id.is_some(),
            "S3 session token requires a key ID and secret"
        );

        // Static libraries do not reliably run OpenDAL's process constructor.
        opendal::install_default();
        let mut builder = S3::default()
            .bucket(bucket)
            .region(config.region.as_deref().unwrap_or("us-east-1"))
            .disable_config_load()
            .disable_ec2_metadata();
        if let Some(endpoint) = config.endpoint.as_deref() {
            let endpoint = if endpoint.contains("://") {
                endpoint.to_owned()
            } else if config.use_ssl {
                format!("https://{endpoint}")
            } else {
                format!("http://{endpoint}")
            };
            builder = builder.endpoint(&endpoint);
        }
        if let (Some(key_id), Some(secret)) = (&config.key_id, &config.secret) {
            builder = builder.access_key_id(key_id).secret_access_key(secret);
            if let Some(token) = config.session_token.as_deref() {
                builder = builder.session_token(token);
            }
        } else {
            builder = builder.skip_signature();
        }
        if config.virtual_host_style {
            builder = builder.enable_virtual_host_style();
        }

        let operator = Operator::new(builder).context("initializing OpenDAL S3 storage")?;
        let object_store = Arc::new(OpendalStore::new(operator.clone()));
        let object_path = object_store::path::Path::from(location.path().trim_start_matches('/'));
        Ok(Self {
            operator,
            object_store,
            object_path,
            location,
        })
    }
}

pub(crate) fn is_s3_uri(path: &str) -> bool {
    path.get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("s3://"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_s3_bucket_and_object_path() {
        let storage = OpendalStorage::from_path(
            "s3://example-bucket/path/data.parquet",
            Some(&Default::default()),
        )
        .unwrap();
        assert_eq!(storage.object_path.as_ref(), "path/data.parquet");
        assert_eq!(storage.location.host_str(), Some("example-bucket"));
    }

    #[test]
    fn rejects_partial_static_credentials() {
        let error = OpendalStorage::from_path(
            "s3://example-bucket/path",
            Some(&S3StorageConfig {
                key_id: Some("key".into()),
                ..Default::default()
            }),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("provided together"));
    }
}
