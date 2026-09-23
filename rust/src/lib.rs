//! Async Arrow conversion with pluggable sources and sinks.
//!
//! DuckDB feeds `LanceWriter` through FFI. Native callers use `convert`.
pub mod converter;
mod ffi;
mod schema;
pub mod sink;
pub mod source;
mod storage;

pub use converter::convert;
pub use sink::{BatchSink, LanceSink, LanceWriter, WriteOptions, WriteSummary};
pub use source::{BatchSource, BatchStream, ParquetFileSource};
pub use storage::S3StorageConfig;
