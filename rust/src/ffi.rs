use std::collections::{btree_map::Entry, BTreeMap};
use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::{Arc, LazyLock, Mutex};

use arrow_array::{
    ffi::{from_ffi_and_data_type, FFI_ArrowArray},
    ffi_stream::FFI_ArrowArrayStream,
    RecordBatch, RecordBatchReader, StructArray,
};
use arrow_schema::{ffi::FFI_ArrowSchema, ArrowError, DataType, Schema, SchemaRef};
use futures::TryStreamExt;

use crate::source::ReadMetrics;
use crate::{
    atif_schema, warc_schema, AtifSource, BatchSource, BatchStream, ColumnEncoding, Error,
    HuggingFaceSource, IndexKind, LanceDatasetWriter, LanceFragmentWriter, OpendalConfig, Result,
    S3StorageConfig, StructuralEncoding, WarcSource, WriteMode, WriteOptions, WriteSummary,
    DEFAULT_MAX_READ_PARALLELISM, DEFAULT_READ_AHEAD_BYTES,
};

static SOURCE_RUNTIME: LazyLock<std::result::Result<tokio::runtime::Runtime, String>> =
    LazyLock::new(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to create source runtime: {error}"))
    });

fn source_runtime() -> Result<&'static tokio::runtime::Runtime> {
    SOURCE_RUNTIME
        .as_ref()
        .map_err(|error| Error::message(error.clone()))
}

// Values of the C enums in lance_conversion.h.
const LANCE_WRITE_MODE_CREATE: i32 = 0;
const LANCE_WRITE_MODE_APPEND: i32 = 1;
const LANCE_WRITE_MODE_OVERWRITE: i32 = 2;
const LANCE_INDEX_SCALAR: i32 = 0;
const LANCE_INDEX_VECTOR: i32 = 1;
const LANCE_INDEX_TEXT: i32 = 2;
const LANCE_INDEX_BLOOM_FILTER: i32 = 3;
const LANCE_STRUCTURAL_ENCODING_DEFAULT: i32 = 0;
const LANCE_STRUCTURAL_ENCODING_MINIBLOCK: i32 = 1;
const LANCE_STRUCTURAL_ENCODING_FULLZIP: i32 = 2;

#[repr(C)]
pub struct LanceS3Config {
    endpoint: *const c_char,
    region: *const c_char,
    key_id: *const c_char,
    secret: *const c_char,
    session_token: *const c_char,
    use_ssl: i32,
    virtual_host_style: i32,
}

#[repr(C)]
pub struct LanceColumnEncoding {
    column: *const c_char,
    compression: *const c_char,
    has_compression_level: i32,
    compression_level: i32,
    structural_encoding: i32,
}

#[repr(C)]
pub struct LanceWriteConfig {
    mode: i32,
    blob_inline_size_threshold: i64,
    blob_dedicated_size_threshold: i64,
    target_file_size: i64,
    storage_version: *const c_char,
    column_encodings: *const LanceColumnEncoding,
    column_encoding_count: usize,
    blob_columns: *const *const c_char,
    blob_column_count: usize,
    index_columns: *const *const c_char,
    index_kinds: *const i32,
    index_column_count: usize,
}

#[repr(C)]
pub struct LanceSourceDefaults {
    max_read_parallelism: u64,
    read_ahead_bytes: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
/// Cumulative source bytes and the expected total used for scan progress.
pub struct LanceReadMetrics {
    bytes_read: u64,
    total_bytes: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
/// Final destination I/O totals, available after the Lance writer finishes.
pub struct LanceWriteMetrics {
    bytes_read: u64,
    bytes_written: u64,
}

/// Shared by concurrent DuckDB threads: only `lance_conversion_finish` and
/// `lance_conversion_destroy` require exclusive access.
pub struct LanceConversionDataset {
    writer: Option<LanceDatasetWriter>,
    runtime: Arc<tokio::runtime::Runtime>,
    summary: Option<WriteSummary>,
}

pub struct LanceConversionWriter {
    writer: LanceFragmentWriter,
    runtime: Arc<tokio::runtime::Runtime>,
}

struct ArrowBatchReader {
    schema: SchemaRef,
    stream: BatchStream,
    runtime: &'static tokio::runtime::Runtime,
}

impl Iterator for ArrowBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.runtime
            .block_on(self.stream.try_next())
            .transpose()
            .map(|result| result.map_err(|error| ArrowError::ExternalError(Box::new(error))))
    }
}

impl RecordBatchReader for ArrowBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

type MakeReader =
    Box<dyn Fn(Vec<usize>, Arc<ReadMetrics>) -> Result<ArrowBatchReader> + Send + Sync>;

/// A fixed-schema source that each DuckDB scan reopens with its own
/// projection. Metrics accumulate across scans.
pub struct SourceStreamFactory {
    schema: SchemaRef,
    metrics: Arc<ReadMetrics>,
    make_reader: MakeReader,
    /// Opened at bind time to learn the schema; reused by the first scan that
    /// reads every column in schema order.
    bound: Mutex<Option<ArrowBatchReader>>,
}

impl SourceStreamFactory {
    fn new<S: BatchSource + 'static>(
        schema: SchemaRef,
        make_source: impl Fn(Vec<usize>, Arc<ReadMetrics>) -> S + Send + Sync + 'static,
    ) -> Self {
        Self {
            schema,
            metrics: Arc::new(ReadMetrics::default()),
            make_reader: Box::new(move |projection, metrics| {
                open_batch_reader(make_source(projection, metrics))
            }),
            bound: Mutex::new(None),
        }
    }

    /// Opens a reader now, for sources whose schema is only known once open.
    fn bind(mut self) -> Result<Self> {
        let reader = self.open_reader(Vec::new())?;
        self.schema = reader.schema();
        self.bound = Mutex::new(Some(reader));
        Ok(self)
    }

    fn open_reader(&self, projection: Vec<usize>) -> Result<ArrowBatchReader> {
        let reads_every_column =
            projection.is_empty() || projection.iter().copied().eq(0..self.schema.fields().len());
        if reads_every_column {
            if let Some(reader) = self.bound.lock().unwrap().take() {
                return Ok(reader);
            }
        }
        (self.make_reader)(projection, self.metrics.clone())
    }
}

fn open_batch_reader(source: impl BatchSource) -> Result<ArrowBatchReader> {
    let runtime = source_runtime()?;
    let (schema, stream) = runtime.block_on(source.open())?;
    Ok(ArrowBatchReader {
        schema,
        stream,
        runtime,
    })
}

unsafe fn optional_string(value: *const c_char) -> Result<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    let value = CStr::from_ptr(value).to_str()?.to_owned();
    Ok((!value.is_empty()).then_some(value))
}

unsafe fn string_list(values: *const *const c_char, count: usize) -> Result<Vec<String>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if values.is_null() {
        return Err(Error::message("null string list"));
    }
    std::slice::from_raw_parts(values, count)
        .iter()
        .map(|value| {
            if value.is_null() {
                return Err(Error::message("null string list value"));
            }
            Ok(CStr::from_ptr(*value).to_str()?.to_owned())
        })
        .collect()
}
unsafe fn i32_list(values: *const i32, count: usize) -> Result<Vec<i32>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if values.is_null() {
        return Err(Error::message("null integer list"));
    }
    Ok(std::slice::from_raw_parts(values, count).to_vec())
}

unsafe fn column_encoding(config: &LanceWriteConfig) -> Result<BTreeMap<String, ColumnEncoding>> {
    if config.column_encoding_count == 0 {
        return Ok(BTreeMap::new());
    }
    if config.column_encodings.is_null() {
        return Err(Error::message("null column encoding list"));
    }
    let mut encodings = BTreeMap::new();
    for encoding in
        std::slice::from_raw_parts(config.column_encodings, config.column_encoding_count)
    {
        let column = optional_string(encoding.column)?
            .ok_or_else(|| Error::message("column encoding has no column"))?;
        let settings = ColumnEncoding {
            compression: optional_string(encoding.compression)?,
            compression_level: (encoding.has_compression_level != 0)
                .then_some(encoding.compression_level),
            structural_encoding: structural_encoding(encoding.structural_encoding)?,
        };
        match encodings.entry(column) {
            Entry::Occupied(entry) => {
                return Err(Error::message(format!(
                    "duplicate column encoding column: {}",
                    entry.key()
                )))
            }
            Entry::Vacant(entry) => {
                entry.insert(settings);
            }
        }
    }
    Ok(encodings)
}

unsafe fn parse_opendal_config(config: *const OpendalConfig) -> Result<OpendalConfig> {
    config.as_ref().copied().unwrap_or_default().validate()
}

unsafe fn s3_config(config: *const LanceS3Config) -> Result<Option<S3StorageConfig>> {
    if config.is_null() {
        return Ok(None);
    }
    let config = &*config;
    Ok(Some(S3StorageConfig {
        endpoint: optional_string(config.endpoint)?,
        region: optional_string(config.region)?,
        key_id: optional_string(config.key_id)?,
        secret: optional_string(config.secret)?,
        session_token: optional_string(config.session_token)?,
        use_ssl: config.use_ssl != 0,
        virtual_host_style: config.virtual_host_style != 0,
    }))
}

fn optional_threshold(value: i64, name: &str, allow_zero: bool) -> Result<Option<usize>> {
    if value < 0 {
        return Ok(None);
    }
    if value == 0 && !allow_zero {
        return Err(Error::message(format!("{name} must be greater than zero")));
    }
    Ok(Some(usize::try_from(value).map_err(|_| {
        Error::message(format!("{name} exceeds the platform size limit"))
    })?))
}

unsafe fn indexes(config: &LanceWriteConfig) -> Result<Vec<(String, IndexKind)>> {
    string_list(config.index_columns, config.index_column_count)?
        .into_iter()
        .zip(i32_list(config.index_kinds, config.index_column_count)?)
        .map(|(column, kind)| Ok((column, index_kind(kind)?)))
        .collect()
}

fn index_kind(kind: i32) -> Result<IndexKind> {
    match kind {
        LANCE_INDEX_SCALAR => Ok(IndexKind::Scalar),
        LANCE_INDEX_VECTOR => Ok(IndexKind::Vector),
        LANCE_INDEX_TEXT => Ok(IndexKind::Text),
        LANCE_INDEX_BLOOM_FILTER => Ok(IndexKind::BloomFilter),
        _ => Err(Error::message(format!("invalid Lance index kind: {kind}"))),
    }
}

fn structural_encoding(encoding: i32) -> Result<Option<StructuralEncoding>> {
    match encoding {
        LANCE_STRUCTURAL_ENCODING_DEFAULT => Ok(None),
        LANCE_STRUCTURAL_ENCODING_MINIBLOCK => Ok(Some(StructuralEncoding::MiniBlock)),
        LANCE_STRUCTURAL_ENCODING_FULLZIP => Ok(Some(StructuralEncoding::FullZip)),
        _ => Err(Error::message(format!(
            "invalid Lance structural encoding: {encoding}"
        ))),
    }
}

fn write_mode(mode: i32) -> Result<WriteMode> {
    match mode {
        LANCE_WRITE_MODE_CREATE => Ok(WriteMode::Create),
        LANCE_WRITE_MODE_APPEND => Ok(WriteMode::Append),
        LANCE_WRITE_MODE_OVERWRITE => Ok(WriteMode::Overwrite),
        _ => Err(Error::message(format!("invalid Lance write mode: {mode}"))),
    }
}

fn call(operation: impl FnOnce() -> Result<()>) -> *mut c_char {
    let error = match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(())) => return ptr::null_mut(),
        Ok(Err(error)) => format!("{error:#}"),
        Err(_) => "panic in Lance conversion FFI".into(),
    };
    CString::new(error.replace('\0', "\\0")).unwrap().into_raw()
}

/// Return the Rust-defined defaults to C++ without duplicating their values.
#[no_mangle]
pub extern "C" fn lance_opendal_default_config() -> OpendalConfig {
    OpendalConfig::default()
}

#[no_mangle]
pub extern "C" fn lance_write_default_config() -> LanceWriteConfig {
    // A negative threshold keeps Lance's own default.
    fn threshold(value: Option<usize>) -> i64 {
        value.map_or(-1, |value| value as i64)
    }
    let defaults = WriteOptions::default();
    LanceWriteConfig {
        mode: LANCE_WRITE_MODE_CREATE,
        blob_inline_size_threshold: threshold(defaults.blob_inline_size_threshold),
        blob_dedicated_size_threshold: threshold(defaults.blob_dedicated_size_threshold),
        target_file_size: threshold(Some(defaults.target_file_size)),
        storage_version: ptr::null(),
        column_encodings: ptr::null(),
        column_encoding_count: 0,
        blob_columns: ptr::null(),
        blob_column_count: 0,
        index_columns: ptr::null(),
        index_kinds: ptr::null(),
        index_column_count: 0,
    }
}

#[no_mangle]
pub extern "C" fn lance_source_defaults() -> LanceSourceDefaults {
    LanceSourceDefaults {
        max_read_parallelism: DEFAULT_MAX_READ_PARALLELISM as u64,
        read_ahead_bytes: DEFAULT_READ_AHEAD_BYTES as u64,
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_error_free(error: *mut c_char) {
    if !error.is_null() {
        drop(CString::from_raw(error));
    }
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_open(
    path: *const c_char,
    schema: *const FFI_ArrowSchema,
    config: *const LanceWriteConfig,
    s3: *const LanceS3Config,
    opendal_config: *const OpendalConfig,
    output: *mut *mut LanceConversionDataset,
) -> *mut c_char {
    call(|| {
        if path.is_null() || schema.is_null() || config.is_null() || output.is_null() {
            return Err(Error::message("null writer argument"));
        }
        *output = ptr::null_mut();
        let path = CStr::from_ptr(path).to_str()?;
        let schema = Arc::new(Schema::try_from(&*schema)?);
        let config = &*config;
        let options = WriteOptions {
            mode: write_mode(config.mode)?,
            s3_config: s3_config(s3)?,
            opendal_config: parse_opendal_config(opendal_config)?,
            blob_inline_size_threshold: optional_threshold(
                config.blob_inline_size_threshold,
                "blob inline size threshold",
                true,
            )?,
            blob_dedicated_size_threshold: optional_threshold(
                config.blob_dedicated_size_threshold,
                "blob dedicated size threshold",
                false,
            )?,
            target_file_size: optional_threshold(
                config.target_file_size,
                "target file size",
                false,
            )?
            .unwrap_or_else(|| WriteOptions::default().target_file_size),
            storage_version: optional_string(config.storage_version)?
                .unwrap_or_else(|| WriteOptions::default().storage_version),
            column_encoding: column_encoding(config)?,
            blob_columns: string_list(config.blob_columns, config.blob_column_count)?,
            indexes: indexes(config)?,
        };
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?,
        );
        let writer = runtime.block_on(LanceDatasetWriter::create(path, schema, options))?;
        *output = Box::into_raw(Box::new(LanceConversionDataset {
            writer: Some(writer),
            runtime,
            summary: None,
        }));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_writer_open(
    dataset: *const LanceConversionDataset,
    output: *mut *mut LanceConversionWriter,
) -> *mut c_char {
    call(|| {
        if dataset.is_null() || output.is_null() {
            return Err(Error::message("null fragment writer argument"));
        }
        *output = ptr::null_mut();
        let dataset = &*dataset;
        let writer = dataset
            .writer
            .as_ref()
            .ok_or_else(|| Error::message("Lance dataset writer is already finished"))?;
        let _runtime = dataset.runtime.enter();
        *output = Box::into_raw(Box::new(LanceConversionWriter {
            writer: writer.fragment_writer(),
            runtime: dataset.runtime.clone(),
        }));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_push(
    writer: *mut LanceConversionWriter,
    array: *mut FFI_ArrowArray,
) -> *mut c_char {
    call(|| {
        if writer.is_null() || array.is_null() {
            return Err(Error::message("null batch argument"));
        }
        let writer = &mut *writer;
        // Move Arrow ownership; the C++ wrapper sees an empty release callback.
        let array = ptr::replace(array, FFI_ArrowArray::empty());
        let data_type = DataType::Struct(writer.writer.schema().fields().clone());
        let data = from_ffi_and_data_type(array, data_type)?;
        let array = StructArray::from(data);
        let batch = RecordBatch::try_new(writer.writer.schema().clone(), array.columns().to_vec())?;
        writer.runtime.block_on(writer.writer.write_batch(batch))
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_writer_finish(
    writer: *mut LanceConversionWriter,
) -> *mut c_char {
    call(|| {
        if writer.is_null() {
            return Err(Error::message("null fragment writer"));
        }
        let writer = &mut *writer;
        writer.runtime.block_on(writer.writer.finish())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_writer_destroy(writer: *mut LanceConversionWriter) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !writer.is_null() {
            let mut writer = Box::from_raw(writer);
            let LanceConversionWriter { writer, runtime } = &mut *writer;
            runtime.block_on(writer.abort());
        }
    }));
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_finish(
    dataset: *mut LanceConversionDataset,
) -> *mut c_char {
    call(|| {
        if dataset.is_null() {
            return Err(Error::message("null writer"));
        }
        let dataset = &mut *dataset;
        let writer = dataset
            .writer
            .take()
            .ok_or_else(|| Error::message("Lance dataset writer is already finished"))?;
        dataset.summary = Some(dataset.runtime.block_on(writer.commit())?);
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_get_metrics(
    dataset: *const LanceConversionDataset,
    output: *mut LanceWriteMetrics,
) {
    if dataset.is_null() || output.is_null() {
        return;
    }
    let metrics = match &(*dataset).summary {
        Some(summary) => LanceWriteMetrics {
            bytes_read: summary.bytes_read,
            bytes_written: summary.bytes_written,
        },
        None => LanceWriteMetrics::default(),
    };
    ptr::write(output, metrics);
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_destroy(dataset: *mut LanceConversionDataset) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !dataset.is_null() {
            drop(Box::from_raw(dataset));
        }
    }));
}

#[no_mangle]
pub unsafe extern "C" fn lance_huggingface_open(
    dataset: *const c_char,
    config: *const c_char,
    split: *const c_char,
    token: *const c_char,
    opendal_config: *const OpendalConfig,
    preserve_insertion_order: i32,
    max_read_parallelism: u64,
    read_ahead_bytes: u64,
    output: *mut *mut SourceStreamFactory,
) -> *mut c_char {
    call(|| {
        if dataset.is_null() || config.is_null() || split.is_null() || output.is_null() {
            return Err(Error::message("null Hugging Face reader argument"));
        }
        *output = ptr::null_mut();
        let dataset = CStr::from_ptr(dataset).to_str()?.to_owned();
        let config = CStr::from_ptr(config).to_str()?.to_owned();
        let split = CStr::from_ptr(split).to_str()?.to_owned();
        let token = optional_string(token)?;
        let opendal_config = parse_opendal_config(opendal_config)?;
        let max_read_parallelism = usize::try_from(max_read_parallelism)
            .map_err(|_| Error::message("max_read_parallelism is too large"))?;
        // A budget beyond the address space cannot be reached anyway.
        let read_ahead_bytes = usize::try_from(read_ahead_bytes).unwrap_or(usize::MAX);
        let schema = Arc::new(Schema::empty());
        let factory = SourceStreamFactory::new(schema, move |projection, metrics| {
            let mut source = HuggingFaceSource::new(&dataset)
                .with_config(&config)
                .with_split(&split)
                .with_preserve_insertion_order(preserve_insertion_order != 0)
                .with_projection(projection)
                .with_max_read_parallelism(max_read_parallelism)
                .with_read_ahead_bytes(read_ahead_bytes)
                .with_opendal_config(opendal_config)
                .with_metrics(metrics);
            if let Some(token) = &token {
                source = source.with_token(token);
            }
            source
        });
        *output = Box::into_raw(Box::new(factory.bind()?));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_warc_open(
    path: *const c_char,
    index_path: *const c_char,
    s3: *const LanceS3Config,
    opendal_config: *const OpendalConfig,
    max_read_parallelism: u64,
    output: *mut *mut SourceStreamFactory,
) -> *mut c_char {
    call(|| {
        if path.is_null() || output.is_null() {
            return Err(Error::message("null WARC reader argument"));
        }
        *output = ptr::null_mut();
        let path = CStr::from_ptr(path).to_str()?.to_owned();
        let index_path = optional_string(index_path)?;
        let max_read_parallelism = usize::try_from(max_read_parallelism)
            .map_err(|_| Error::message("max_read_parallelism is too large"))?;
        if max_read_parallelism == 0 {
            return Err(Error::message("max_read_parallelism must be positive"));
        }
        let s3_config = s3_config(s3)?;
        let opendal_config = parse_opendal_config(opendal_config)?;
        let factory = SourceStreamFactory::new(warc_schema(), move |projection, metrics| {
            let mut source = WarcSource::new(&path)
                .with_projection(projection)
                .with_max_read_parallelism(max_read_parallelism)
                .with_opendal_config(opendal_config)
                .with_metrics(metrics);
            if let Some(index_path) = &index_path {
                source = source.with_index_path(index_path);
            }
            if let Some(config) = &s3_config {
                source = source.with_s3_config(config.clone());
            }
            source
        });
        *output = Box::into_raw(Box::new(factory));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_atif_open(
    path: *const c_char,
    format: *const c_char,
    s3: *const LanceS3Config,
    opendal_config: *const OpendalConfig,
    output: *mut *mut SourceStreamFactory,
) -> *mut c_char {
    call(|| {
        if path.is_null() || output.is_null() {
            return Err(Error::message("null ATIF reader argument"));
        }
        *output = ptr::null_mut();
        let path = CStr::from_ptr(path).to_str()?.to_owned();
        let format = optional_string(format)?;
        let s3_config = s3_config(s3)?;
        let opendal_config = parse_opendal_config(opendal_config)?;
        let factory = SourceStreamFactory::new(atif_schema(), move |projection, metrics| {
            let mut source = AtifSource::new(&path)
                .with_projection(projection)
                .with_opendal_config(opendal_config)
                .with_metrics(metrics);
            if let Some(format) = &format {
                source = source.with_format(format);
            }
            if let Some(config) = &s3_config {
                source = source.with_s3_config(config.clone());
            }
            source
        });
        *output = Box::into_raw(Box::new(factory));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_source_get_metrics(
    factory: *const SourceStreamFactory,
    output: *mut LanceReadMetrics,
) {
    if factory.is_null() || output.is_null() {
        return;
    }
    let snapshot = (*factory).metrics.snapshot();
    ptr::write(
        output,
        LanceReadMetrics {
            bytes_read: snapshot.bytes_read,
            total_bytes: snapshot.total_bytes,
        },
    );
}

#[no_mangle]
pub unsafe extern "C" fn lance_source_get_schema(
    factory: *const SourceStreamFactory,
    output: *mut FFI_ArrowSchema,
) -> *mut c_char {
    call(|| {
        if factory.is_null() || output.is_null() {
            return Err(Error::message("null source schema argument"));
        }
        ptr::write(
            output,
            FFI_ArrowSchema::try_from((*factory).schema.as_ref())?,
        );
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_source_get_stream(
    factory: *const SourceStreamFactory,
    columns: *const *const c_char,
    column_count: usize,
    output: *mut FFI_ArrowArrayStream,
) -> *mut c_char {
    call(|| {
        if factory.is_null() || output.is_null() {
            return Err(Error::message("null source stream argument"));
        }
        let factory = &*factory;
        let projection = string_list(columns, column_count)?
            .iter()
            .map(|name| {
                factory
                    .schema
                    .index_of(name)
                    .map_err(|_| Error::message(format!("unknown source column: {name}")))
            })
            .collect::<Result<_>>()?;
        let reader = factory.open_reader(projection)?;
        ptr::write(output, FFI_ArrowArrayStream::new(Box::new(reader)));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_source_destroy(factory: *mut SourceStreamFactory) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !factory.is_null() {
            drop(Box::from_raw(factory));
        }
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ffi_errors_remain_valid_across_calls() {
        unsafe {
            let first = lance_conversion_finish(ptr::null_mut());
            let second = lance_conversion_push(ptr::null_mut(), ptr::null_mut());
            assert!(!first.is_null());
            assert!(!second.is_null());
            assert!(CStr::from_ptr(first)
                .to_str()
                .unwrap()
                .starts_with("null writer (permanent) at "));
            lance_conversion_error_free(first);
            assert!(CStr::from_ptr(second)
                .to_str()
                .unwrap()
                .starts_with("null batch argument (permanent) at "));
            lance_conversion_error_free(second);
            lance_conversion_error_free(ptr::null_mut());
            assert!(call(|| Ok(())).is_null());
        }
    }
}
