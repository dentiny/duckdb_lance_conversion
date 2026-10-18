use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreParams, ObjectStoreProvider, WrappingObjectStore,
    DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_DOWNLOAD_RETRY_COUNT, DEFAULT_LOCAL_IO_PARALLELISM,
};
use lance_io::utils::tracking_store::IOTracker;
use object_store::{path::Path, ObjectStore as ArrowObjectStore};
use object_store_opendal::OpendalStore;
use opendal::{
    layers::{RetryLayer, TimeoutLayer},
    services::{Fs, Hf, Http, S3},
    Operator,
};
use percent_encoding::percent_decode_str;
use url::Url;

use crate::error::ResultExt;
use crate::{Error, Result};

const DEFAULT_STORAGE_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_STORAGE_IO_TIMEOUT_MS: u64 = 10_000;
const DEFAULT_STORAGE_RETRY_MAX_TIMES: u64 = 3;
const DEFAULT_STORAGE_RETRY_MIN_DELAY_MS: u64 = 1_000;
const DEFAULT_STORAGE_RETRY_MAX_DELAY_MS: u64 = 60_000;
const DEFAULT_STORAGE_RETRY_FACTOR: f64 = 2.0;

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
            timeout_ms: DEFAULT_STORAGE_TIMEOUT_MS,
            io_timeout_ms: DEFAULT_STORAGE_IO_TIMEOUT_MS,
            retry_max_times: DEFAULT_STORAGE_RETRY_MAX_TIMES,
            retry_min_delay_ms: DEFAULT_STORAGE_RETRY_MIN_DELAY_MS,
            retry_max_delay_ms: DEFAULT_STORAGE_RETRY_MAX_DELAY_MS,
            retry_factor: DEFAULT_STORAGE_RETRY_FACTOR,
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
                return Err(Error::message(format!(
                    "lance_conversion_storage_{name} must be positive"
                )));
            }
            if std::time::Instant::now()
                .checked_add(Duration::from_millis(value))
                .is_none()
            {
                return Err(Error::message(format!(
                    "lance_conversion_storage_{name} is too large"
                )));
            }
        }
        if self.retry_max_delay_ms < self.retry_min_delay_ms {
            return Err(Error::message(
                "lance_conversion_storage_retry_max_delay_ms must be at least retry_min_delay_ms",
            ));
        }
        if !self.retry_factor.is_finite()
            || self.retry_factor < 1.0
            || self.retry_factor > f32::MAX as f64
        {
            return Err(Error::message(
                "lance_conversion_storage_retry_factor must be finite and between 1 and f32::MAX",
            ));
        }
        usize::try_from(self.retry_max_times)
            .map_err(|_| Error::message("lance_conversion_storage_retry_max_times is too large"))?;
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
            .with_factor(config.retry_factor as f32)
            .with_jitter();
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

/// Serves the Lance destination and Blob v2 source URIs through OpenDAL, with
/// one store per bucket or host, shared for the whole write.
pub(crate) struct OpendalStoreProvider {
    s3_config: S3StorageConfig,
    opendal_config: OpendalConfig,
    io_tracker: Arc<IOTracker>,
    stores: Mutex<HashMap<String, Arc<dyn ArrowObjectStore>>>,
}

impl std::fmt::Debug for OpendalStoreProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpendalStoreProvider")
            .finish_non_exhaustive()
    }
}

impl OpendalStoreProvider {
    pub const SCHEMES: [&str; 4] = ["file", "s3", "http", "https"];

    pub fn new(
        s3_config: Option<S3StorageConfig>,
        opendal_config: OpendalConfig,
        io_tracker: Arc<IOTracker>,
    ) -> Self {
        Self {
            s3_config: s3_config.unwrap_or_default(),
            opendal_config,
            io_tracker,
            stores: Mutex::default(),
        }
    }

    fn object_store(&self, url: &Url) -> Result<Arc<dyn ArrowObjectStore>> {
        let key = format!("{}${}", url.scheme(), url.authority());
        let mut stores = self.stores.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(store) = stores.get(&key) {
            return Ok(store.clone());
        }
        let store = match url.scheme() {
            "file" => OpendalStorage::from_local_path("/")?.object_store,
            "s3" => {
                OpendalStorage::from_s3_uri(url.as_str(), &self.s3_config, &self.opendal_config)?
                    .object_store
            }
            "http" | "https" => Arc::new(OpendalStore::new(http_operator(
                &url[..url::Position::BeforePath],
                &self.opendal_config,
            )?)),
            scheme => {
                return Err(Error::message(format!(
                    "unsupported storage scheme: {scheme}"
                )))
            }
        };
        let store = self.io_tracker.wrap(&key, store);
        stores.insert(key, store.clone());
        Ok(store)
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
        let object_store = self
            .object_store(&base_path)
            .map_err(|error| lance::Error::invalid_input(error.to_string()))?;
        Ok(LanceObjectStore::new(
            object_store,
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

    fn extract_path(&self, url: &Url) -> lance::Result<Path> {
        // Object paths cannot carry a query, so it would be silently dropped.
        if url.query().is_some() || url.fragment().is_some() {
            return Err(lance::Error::invalid_input(format!(
                "storage URI must not have a query or fragment: {url}"
            )));
        }
        Path::from_url_path(url.path()).map_err(|error| {
            lance::Error::invalid_input(format!("invalid path in URI {url}: {error}"))
        })
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

/// A file read from a local path, an `s3://` URI, or an HTTP(S) URL.
pub(crate) struct InputFile {
    pub operator: Operator,
    /// Path within `operator`, without a leading slash.
    pub opendal_path: String,
    /// The absolute local path, or the URL for remote files.
    pub uri: String,
}

impl InputFile {
    pub fn open(
        path: &str,
        s3_config: Option<&S3StorageConfig>,
        opendal_config: &OpendalConfig,
    ) -> Result<Self> {
        let input = match Url::parse(path) {
            Ok(url) if matches!(url.scheme(), "http" | "https") => {
                if url.query().is_some() || url.fragment().is_some() {
                    return Err(Error::message(format!(
                        "HTTP(S) URL must not include a query or fragment: {path}"
                    )));
                }
                // OpenDAL percent-encodes the path again.
                let object_path = percent_decode_str(url.path()).decode_utf8()?;
                Self {
                    operator: http_operator(&url[..url::Position::BeforePath], opendal_config)?,
                    opendal_path: object_path.trim_start_matches('/').to_owned(),
                    uri: url.to_string(),
                }
            }
            _ => {
                let storage = OpendalStorage::from_path(path, s3_config, opendal_config)?;
                let uri = match storage.location.to_file_path() {
                    Ok(local_path) => local_path.display().to_string(),
                    Err(()) => storage.location.to_string(),
                };
                Self {
                    operator: storage.operator,
                    opendal_path: storage.object_path.to_string(),
                    uri,
                }
            }
        };
        if input.opendal_path.is_empty() {
            return Err(Error::message(format!(
                "input must not be a storage root: {path}"
            )));
        }
        Ok(input)
    }

    /// Lists the files matching a local or `s3://` glob `pattern`, sorted by
    /// path. `*`, `?`, and `[...]` match within one path component, and a
    /// `**` component matches any number of directories.
    pub async fn glob(
        pattern: &str,
        s3_config: Option<&S3StorageConfig>,
        opendal_config: &OpendalConfig,
    ) -> Result<Vec<Self>> {
        if is_http_uri(pattern) {
            return Err(Error::message(format!(
                "HTTP(S) URL must not include a glob pattern: {pattern}"
            )));
        }
        // Only the literal directory before the first wildcard is listed.
        let glob_start = pattern.find(GLOB_CHARS).unwrap_or(pattern.len());
        let dir_end = pattern[..glob_start]
            .rfind('/')
            .map_or(0, |index| index + 1);
        let (dir, relative_pattern) = pattern.split_at(dir_end);
        let matcher = glob::Pattern::new(relative_pattern).map_err(|error| {
            Error::message(format!("invalid glob pattern '{pattern}': {error}"))
        })?;
        let options = glob::MatchOptions {
            case_sensitive: true,
            require_literal_separator: true,
            require_literal_leading_dot: false,
        };

        let storage = OpendalStorage::from_path(dir, s3_config, opendal_config)?;
        let root = storage.object_path.to_string();
        let (list_path, prefix) = if root.is_empty() {
            ("/".to_owned(), String::new())
        } else {
            (format!("{root}/"), format!("{root}/"))
        };
        let uri_prefix = match storage.location.host_str() {
            Some(bucket) if is_s3_uri(pattern) => format!("s3://{bucket}/"),
            _ => "/".to_owned(),
        };
        let entries = match storage
            .operator
            .list_with(&list_path)
            .recursive(relative_pattern.contains('/'))
            .await
        {
            Ok(entries) => entries,
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error).context(format!("listing {dir}")),
        };
        let mut paths = entries
            .iter()
            .map(opendal::Entry::path)
            .filter(|path| {
                path.strip_prefix(&prefix).is_some_and(|relative| {
                    !relative.is_empty()
                        && !relative.ends_with('/')
                        && matcher.matches_with(relative, options)
                })
            })
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if paths.is_empty() {
            return Err(Error::message(format!("no files match {pattern}")));
        }
        paths.sort_unstable();
        Ok(paths
            .into_iter()
            .map(|opendal_path| Self {
                operator: storage.operator.clone(),
                uri: format!("{uri_prefix}{opendal_path}"),
                opendal_path,
            })
            .collect())
    }
}

const GLOB_CHARS: [char; 3] = ['*', '?', '['];

/// Returns true when `path` should be expanded by [`InputFile::glob`].
pub(crate) fn has_glob(path: &str) -> bool {
    if is_http_uri(path) {
        path.contains(['*', '['])
    } else {
        path.contains(GLOB_CHARS)
    }
}

fn is_http_uri(path: &str) -> bool {
    let lowercase = path.get(..8).unwrap_or(path).to_ascii_lowercase();
    lowercase.starts_with("http://") || lowercase.starts_with("https://")
}

/// Builds a read operator for files under the HTTP(S) `endpoint`.
fn http_operator(endpoint: &str, opendal_config: &OpendalConfig) -> Result<Operator> {
    opendal_http_transport_reqwest::install_default();
    opendal_config.apply(Operator::new(Http::default().endpoint(endpoint))?)
}

/// Builds a read operator over the files of a Hugging Face dataset at `revision`.
pub(crate) fn huggingface_operator(
    dataset: &str,
    revision: &str,
    token: Option<&str>,
    opendal_config: &OpendalConfig,
) -> Result<Operator> {
    opendal_http_transport_reqwest::install_default();
    // Plain HTTP downloads follow the Hub redirect, without the XET client.
    let mut builder = Hf::default()
        .repo_type("dataset")
        .repo_id(dataset)
        .revision(revision)
        .download_mode("http");
    if let Some(token) = token {
        builder = builder.token(token);
    }
    opendal_config.apply(Operator::new(builder)?)
}

pub(crate) fn is_s3_uri(path: &str) -> bool {
    path.get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("s3://"))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn glob_names(root: &std::path::Path, pattern: &str) -> Result<Vec<String>> {
        let pattern = format!("{}/{pattern}", root.display());
        let files = InputFile::glob(&pattern, None, &OpendalConfig::default()).await?;
        Ok(files
            .into_iter()
            .map(|file| {
                assert_eq!(file.uri, format!("/{}", file.opendal_path));
                file.uri
                    .strip_prefix(&format!("{}/", root.display()))
                    .unwrap()
                    .to_owned()
            })
            .collect())
    }

    #[tokio::test]
    async fn globs_local_files() {
        let temp = tempfile::TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap();
        for file in ["b.jsonl", "a.jsonl", "c.json", "x/d.jsonl", "x/y/e.jsonl"] {
            let path = root.join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "").unwrap();
        }

        assert_eq!(
            glob_names(&root, "*.jsonl").await.unwrap(),
            ["a.jsonl", "b.jsonl"]
        );
        assert_eq!(
            glob_names(&root, "**/*.jsonl").await.unwrap(),
            ["a.jsonl", "b.jsonl", "x/d.jsonl", "x/y/e.jsonl"]
        );
        assert_eq!(glob_names(&root, "x/*/*").await.unwrap(), ["x/y/e.jsonl"]);
        assert_eq!(glob_names(&root, "[ac].json?").await.unwrap(), ["a.jsonl"]);
        assert!(glob_names(&root, "*.csv")
            .await
            .unwrap_err()
            .to_string()
            .contains("no files match"));
        assert!(glob_names(&root, "missing/*")
            .await
            .unwrap_err()
            .to_string()
            .contains("no files match"));
    }

    #[tokio::test]
    async fn rejects_http_globs() {
        let error = InputFile::glob(
            "https://example.com/*.jsonl",
            None,
            &OpendalConfig::default(),
        )
        .await
        .err()
        .unwrap();
        assert!(error
            .to_string()
            .contains("must not include a glob pattern"));
        assert!(has_glob("s3://bucket/a?.json"));
        assert!(!has_glob("s3://bucket/a.json"));
        assert!(has_glob("https://example.com/*.json"));
        // `?` starts an HTTP query, which `InputFile::open` rejects.
        assert!(!has_glob("https://example.com/a.json?x=1"));
    }
}
