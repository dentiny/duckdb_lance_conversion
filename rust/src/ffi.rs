use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::{Arc, LazyLock};

use arrow_array::{
    ffi::{from_ffi_and_data_type, FFI_ArrowArray},
    ffi_stream::FFI_ArrowArrayStream,
    RecordBatch, RecordBatchReader, StructArray,
};
use arrow_schema::{ffi::FFI_ArrowSchema, ArrowError, DataType, Schema, SchemaRef};
use futures::TryStreamExt;

use crate::{
    warc_schema, BatchSource, BatchStream, Error, HuggingFaceSource, LanceWriter, Result,
    S3StorageConfig, WarcSource, WriteMode, WriteOptions,
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
pub struct LanceWriteConfig {
    mode: i32,
    blob_inline_size_threshold: i64,
    blob_dedicated_size_threshold: i64,
    target_file_size: i64,
    blob_columns: *const *const c_char,
    blob_column_count: usize,
}

pub struct LanceConversionWriter {
    sink: LanceWriter,
    runtime: tokio::runtime::Runtime,
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

pub struct HuggingFaceStreamFactory {
    dataset: String,
    config: String,
    split: String,
    token: Option<String>,
    schema: SchemaRef,
    reader: Option<ArrowBatchReader>,
}

impl HuggingFaceStreamFactory {
    fn open_reader(&self) -> Result<ArrowBatchReader> {
        let mut source = HuggingFaceSource::new(&self.dataset)
            .with_config(&self.config)
            .with_split(&self.split);
        if let Some(token) = &self.token {
            source = source.with_token(token);
        }
        open_batch_reader(source)
    }
}

pub struct WarcStreamFactory {
    path: String,
    s3_config: Option<S3StorageConfig>,
}

impl WarcStreamFactory {
    fn open_reader(&self, projection: Vec<usize>) -> Result<ArrowBatchReader> {
        let mut source = WarcSource::new(&self.path).with_projection(projection);
        if let Some(config) = &self.s3_config {
            source = source.with_s3_config(config.clone());
        }
        open_batch_reader(source)
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
        return Err(Error::invalid_argument(format!(
            "{name} must be greater than zero"
        )));
    }
    Ok(Some(usize::try_from(value).map_err(|_| {
        Error::invalid_argument(format!("{name} exceeds the platform size limit"))
    })?))
}

fn write_mode(mode: i32) -> Result<WriteMode> {
    match mode {
        0 => Ok(WriteMode::Create),
        1 => Ok(WriteMode::Append),
        2 => Ok(WriteMode::Overwrite),
        _ => Err(Error::invalid_argument(format!(
            "invalid Lance write mode: {mode}"
        ))),
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
    output: *mut *mut LanceConversionWriter,
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
            blob_columns: string_list(config.blob_columns, config.blob_column_count)?,
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let sink = runtime.block_on(LanceWriter::create(path, schema, options))?;
        *output = Box::into_raw(Box::new(LanceConversionWriter { sink, runtime }));
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
        let data_type = DataType::Struct(writer.sink.schema().fields().clone());
        let data = from_ffi_and_data_type(array, data_type)?;
        let array = StructArray::from(data);
        let batch = RecordBatch::try_new(writer.sink.schema().clone(), array.columns().to_vec())?;
        writer.runtime.block_on(writer.sink.write_batch(batch))
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_finish(
    writer: *mut LanceConversionWriter,
) -> *mut c_char {
    call(|| {
        if writer.is_null() {
            return Err(Error::message("null writer"));
        }
        let writer = &mut *writer;
        writer.runtime.block_on(writer.sink.finish())?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_destroy(writer: *mut LanceConversionWriter) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !writer.is_null() {
            let mut writer = Box::from_raw(writer);
            let LanceConversionWriter { sink, runtime } = &mut *writer;
            runtime.block_on(sink.abort());
        }
    }));
}

#[no_mangle]
pub unsafe extern "C" fn lance_huggingface_open(
    dataset: *const c_char,
    config: *const c_char,
    split: *const c_char,
    token: *const c_char,
    output: *mut *mut HuggingFaceStreamFactory,
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
        let mut source = HuggingFaceSource::new(&dataset)
            .with_config(&config)
            .with_split(&split);
        if let Some(token) = &token {
            source = source.with_token(token);
        }
        let reader = open_batch_reader(source)?;
        let schema = reader.schema();
        *output = Box::into_raw(Box::new(HuggingFaceStreamFactory {
            dataset,
            config,
            split,
            token,
            schema,
            reader: Some(reader),
        }));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_huggingface_get_schema(
    factory: *const HuggingFaceStreamFactory,
    output: *mut FFI_ArrowSchema,
) -> *mut c_char {
    call(|| {
        if factory.is_null() || output.is_null() {
            return Err(Error::message("null Hugging Face schema argument"));
        }
        ptr::write(
            output,
            FFI_ArrowSchema::try_from((&*factory).schema.as_ref())?,
        );
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_huggingface_get_stream(
    factory: *mut HuggingFaceStreamFactory,
    output: *mut FFI_ArrowArrayStream,
) -> *mut c_char {
    call(|| {
        if factory.is_null() || output.is_null() {
            return Err(Error::message("null Hugging Face stream argument"));
        }
        let factory = &mut *factory;
        let reader = factory.reader.take();
        let reader = match reader {
            Some(reader) => reader,
            None => factory.open_reader()?,
        };
        ptr::write(output, FFI_ArrowArrayStream::new(Box::new(reader)));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_huggingface_destroy(factory: *mut HuggingFaceStreamFactory) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !factory.is_null() {
            drop(Box::from_raw(factory));
        }
    }));
}

#[no_mangle]
pub unsafe extern "C" fn lance_warc_open(
    path: *const c_char,
    s3: *const LanceS3Config,
    output: *mut *mut WarcStreamFactory,
) -> *mut c_char {
    call(|| {
        if path.is_null() || output.is_null() {
            return Err(Error::message("null WARC reader argument"));
        }
        *output = ptr::null_mut();
        let path = CStr::from_ptr(path).to_str()?.to_owned();
        *output = Box::into_raw(Box::new(WarcStreamFactory {
            path,
            s3_config: s3_config(s3)?,
        }));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_warc_get_schema(
    factory: *const WarcStreamFactory,
    output: *mut FFI_ArrowSchema,
) -> *mut c_char {
    call(|| {
        if factory.is_null() || output.is_null() {
            return Err(Error::message("null WARC schema argument"));
        }
        ptr::write(output, FFI_ArrowSchema::try_from(warc_schema().as_ref())?);
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_warc_get_stream(
    factory: *const WarcStreamFactory,
    columns: *const *const c_char,
    column_count: usize,
    output: *mut FFI_ArrowArrayStream,
) -> *mut c_char {
    call(|| {
        if factory.is_null() || output.is_null() {
            return Err(Error::message("null WARC stream argument"));
        }
        if column_count != 0 && columns.is_null() {
            return Err(Error::message("null WARC projection"));
        }
        let schema = warc_schema();
        let columns = if column_count == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(columns, column_count)
        };
        let projection = columns
            .iter()
            .map(|&column| {
                if column.is_null() {
                    return Err(Error::message("null WARC column name"));
                }
                let name = CStr::from_ptr(column).to_str()?;
                schema
                    .index_of(name)
                    .map_err(|_| Error::message(format!("unknown WARC column: {name}")))
            })
            .collect::<Result<Vec<_>>>()?;
        let reader = (&*factory).open_reader(projection)?;
        ptr::write(output, FFI_ArrowArrayStream::new(Box::new(reader)));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_warc_destroy(factory: *mut WarcStreamFactory) {
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
