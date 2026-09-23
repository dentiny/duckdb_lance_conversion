use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;

use arrow_array::{
    ffi::{from_ffi_and_data_type, FFI_ArrowArray},
    RecordBatch, StructArray,
};
use arrow_schema::{ffi::FFI_ArrowSchema, DataType, Schema};

use crate::{Error, LanceWriter, Result, S3StorageConfig, WriteOptions};

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

pub struct LanceConversionWriter {
    sink: LanceWriter,
    runtime: tokio::runtime::Runtime,
}

unsafe fn optional_string(value: *const c_char) -> Result<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    let value = CStr::from_ptr(value).to_str()?.to_owned();
    Ok((!value.is_empty()).then_some(value))
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
    overwrite: i32,
    s3: *const LanceS3Config,
    output: *mut *mut LanceConversionWriter,
) -> *mut c_char {
    call(|| {
        if path.is_null() || schema.is_null() || output.is_null() {
            return Err(Error::message("null writer argument"));
        }
        *output = ptr::null_mut();
        let path = CStr::from_ptr(path).to_str()?;
        let schema = Arc::new(Schema::try_from(&*schema)?);
        let options = WriteOptions {
            overwrite: overwrite != 0,
            s3_config: s3_config(s3)?,
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
