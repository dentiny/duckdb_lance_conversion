use std::cell::RefCell;
use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;

use anyhow::{ensure, Result};
use arrow_array::{
    ffi::{from_ffi_and_data_type, FFI_ArrowArray},
    RecordBatch, StructArray,
};
use arrow_schema::{ffi::FFI_ArrowSchema, DataType, Schema};

use crate::{LanceSink, WriteOptions};

pub struct LanceConversionWriter {
    sink: LanceSink,
    runtime: tokio::runtime::Runtime,
}

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn call(operation: impl FnOnce() -> Result<()>) -> i32 {
    let error = match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(Ok(())) => return 0,
        Ok(Err(error)) => format!("{error:#}"),
        Err(_) => "panic in Lance conversion FFI".into(),
    };
    LAST_ERROR.with(|slot| *slot.borrow_mut() = CString::new(error.replace('\0', "\\0")).unwrap());
    1
}

#[no_mangle]
pub extern "C" fn lance_conversion_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| slot.borrow().as_ptr())
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_open(
    path: *const c_char,
    schema: *const FFI_ArrowSchema,
    overwrite: i32,
    output: *mut *mut LanceConversionWriter,
) -> i32 {
    call(|| {
        ensure!(
            !path.is_null() && !schema.is_null() && !output.is_null(),
            "null writer argument"
        );
        *output = ptr::null_mut();
        let path = CStr::from_ptr(path).to_str()?;
        let schema = Arc::new(Schema::try_from(&*schema)?);
        let options = WriteOptions {
            overwrite: overwrite != 0,
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let sink = runtime.block_on(LanceSink::create(path, schema, options))?;
        *output = Box::into_raw(Box::new(LanceConversionWriter { sink, runtime }));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_push(
    writer: *mut LanceConversionWriter,
    array: *mut FFI_ArrowArray,
) -> i32 {
    call(|| {
        ensure!(!writer.is_null() && !array.is_null(), "null batch argument");
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
pub unsafe extern "C" fn lance_conversion_finish(writer: *mut LanceConversionWriter) -> i32 {
    call(|| {
        ensure!(!writer.is_null(), "null writer");
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
