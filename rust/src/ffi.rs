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
    output: *mut *mut LanceSink,
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
        *output = Box::into_raw(Box::new(LanceSink::create(path, schema, options)?));
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_push(
    writer: *mut LanceSink,
    array: *mut FFI_ArrowArray,
) -> i32 {
    call(|| {
        ensure!(!writer.is_null() && !array.is_null(), "null batch argument");
        let writer = &mut *writer;
        // Move Arrow ownership; the C++ wrapper sees an empty release callback.
        let array = ptr::replace(array, FFI_ArrowArray::empty());
        let data_type = DataType::Struct(writer.schema().fields().clone());
        let data = from_ffi_and_data_type(array, data_type)?;
        let array = StructArray::from(data);
        let batch = RecordBatch::try_new(writer.schema().clone(), array.columns().to_vec())?;
        writer.write_batch(batch)
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_finish(writer: *mut LanceSink) -> i32 {
    call(|| {
        ensure!(!writer.is_null(), "null writer");
        (&mut *writer).finish()?;
        Ok(())
    })
}

#[no_mangle]
pub unsafe extern "C" fn lance_conversion_destroy(writer: *mut LanceSink) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !writer.is_null() {
            drop(Box::from_raw(writer));
        }
    }));
}
