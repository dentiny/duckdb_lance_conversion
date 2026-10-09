use std::sync::Arc;
use std::time::Duration;

use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreParams, ObjectStoreProvider,
    DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_DOWNLOAD_RETRY_COUNT, DEFAULT_LOCAL_IO_PARALLELISM,
};
use object_store::ObjectStore as ArrowObjectStore;
use object_store_opendal::OpendalStore;
use opendal::{
    layers::{RetryLayer, TimeoutLayer},
    services::{Fs, S3},
    Operator,
};
use url::Url;

use crate::error::ResultExt;
use crate::{Error, Result};

/// Per-query OpenDAL policy, copied across the C ABI and into each remote operator.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct OpendalConfig {
    pub timeout_ms: u64,
    pub io_timeout_ms: u64,
    pub retry_max_times: u64,
    pub retry_min_delay_ms: u64,
    pub retry_max_delay_ms: u64,
    pub retry_factor: f64,
}

impl Default for OpendalConfig {
    fn default() -> Self {
        Self {
            timeout_ms: 60_000,
            io_timeout_ms: 10_000,
            retry_max_times: 3,
            retry_min_delay_ms: 1_000,
            retry_max_delay_ms: 60_000,
            retry_factor: 2.0,
        }
    }
}

impl OpendalConfig {
    pub(crate) fn validate(self) -> Result<Self> {
        for (name, value) in [
            ("timeout_ms", self.timeout_ms),
            ("io_timeout_ms", self.io_timeout_ms),
            ("retry_min_delay_ms", self.retry_min_delay_ms),
            ("retry_max_delay_ms", self.retry_max_delay_ms),
        ] {
            if value == 0 {
                return Err(Error::invalid_argument(format!(
                    "lance_conversion_opendal_{name} must be positive"
                )));
            }
            if std::time::Instant::now()
                .checked_add(Duration::from_millis(value))
                .is_none()
            {
                return Err(Error::invalid_argument(format!(
                    "lance_conversion_opendal_{name} is too large"
                )));
            }
        }
        if self.retry_max_delay_ms < self.retry_min_delay_ms {
            return Err(Error::invalid_argument(
                "lance_conversion_opendal_retry_max_delay_ms must be at least retry_min_delay_ms",
            ));
        }
        if !self.retry_factor.is_finite()
            || self.retry_factor < 1.0
            || self.retry_factor > f32::MAX as f64
        {
            return Err(Error::invalid_argument(
                "lance_conversion_opendal_retry_factor must be finite and between 1 and f32::MAX",
            ));
        }
        usize::try_from(self.retry_max_times).map_err(|_| {
            Error::invalid_argument("lance_conversion_opendal_retry_max_times is too large")
        })?;
        Ok(self)
    }

    pub(crate) fn apply(self, operator: Operator) -> Result<Operator> {
        let config = self.validate()?;
        let timeout = TimeoutLayer::new()
            .with_timeout(Duration::from_millis(config.timeout_ms))
            .with_io_timeout(Duration::from_millis(config.io_timeout_ms));
        let retry = RetryLayer::new()
            .with_max_times(config.retry_max_times as usize)
            .with_min_delay(Duration::from_millis(config.retry_min_delay_ms))
            .with_max_delay(Duration::from_millis(config.retry_max_delay_ms))
            .with_factor(config.retry_factor as f32);
        // Timeout must be inside retry: each attempt has its own deadline, and
        // cancellation never drops RetryLayer while it holds IO body state.
        Ok(operator.layer(timeout).layer(retry))
    }
}

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
    pub fn from_path(
        path: &str,
        s3_config: Option<&S3StorageConfig>,
        opendal_config: &OpendalConfig,
    ) -> Result<Self> {
        opendal_config.validate()?;
        if is_s3_uri(path) {
            let config = s3_config
                .ok_or_else(|| Error::message("S3 path requires S3 storage configuration"))?;
            Self::from_s3_uri(path, config, opendal_config)
        } else {
            if s3_config.is_some() {
                return Err(Error::message(
                    "S3 storage configuration can only be used with an s3:// path",
                ));
            }
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
        let location = Url::from_file_path(&absolute).map_err(|_| {
            Error::message(format!(
                "invalid local storage path: {}",
                absolute.display()
            ))
        })?;
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

    fn from_s3_uri(
        uri: &str,
        config: &S3StorageConfig,
        opendal_config: &OpendalConfig,
    ) -> Result<Self> {
        let location = Url::parse(uri).context("invalid S3 URI")?;
        if location.scheme() != "s3" {
            return Err(Error::message("expected an s3:// URI"));
        }
        let bucket = location
            .host_str()
            .filter(|bucket| !bucket.is_empty())
            .ok_or_else(|| Error::message("S3 URI must include a bucket"))?;

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
        if let Some(key_id) = config.key_id.as_deref() {
            builder = builder.access_key_id(key_id);
        }
        if let Some(secret) = config.secret.as_deref() {
            builder = builder.secret_access_key(secret);
        }
        if let Some(token) = config.session_token.as_deref() {
            builder = builder.session_token(token);
        }
        if config.key_id.is_none() && config.secret.is_none() && config.session_token.is_none() {
            builder = builder.skip_signature();
        }
        if config.virtual_host_style {
            builder = builder.enable_virtual_host_style();
        }

        let operator = opendal_config
            .apply(Operator::new(builder).context("initializing OpenDAL S3 storage")?)?;
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
