//! Async Arrow sources and Lance writers for the DuckDB extension, which
//! drives them through FFI.
mod error;
mod ffi;
mod schema;
pub mod sink;
pub mod source;
mod storage;

pub use error::{Error, ErrorStatus, Result};
pub use sink::{
    ColumnEncoding, IndexKind, LanceDatasetWriter, LanceFragmentWriter, LanceWriter, WriteMode,
    WriteOptions, WriteSummary,
};
pub use source::{
    atif_schema, warc_schema, AtifSource, BatchSource, BatchStream, HuggingFaceSource,
    SourceReadOptions, WarcSource, DEFAULT_MAX_READ_PARALLELISM,
};
pub use storage::{OpendalConfig, S3StorageConfig};
