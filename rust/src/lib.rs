//! Async Arrow conversion with pluggable sources and sinks.
//!
//! DuckDB feeds `LanceWriter` through FFI. Native callers use `convert`.
pub mod converter;
mod error;
mod error_struct;
mod ffi;
mod schema;
pub mod sink;
pub mod source;
mod storage;

pub use converter::convert;
pub use error::{Error, Result};
pub use error_struct::{ErrorStatus, ErrorStruct};
pub use sink::{BatchSink, LanceSink, LanceWriter, WriteMode, WriteOptions, WriteSummary};
pub use source::{warc_schema, BatchSource, BatchStream, HuggingFaceSource, WarcSource};
pub use storage::S3StorageConfig;
