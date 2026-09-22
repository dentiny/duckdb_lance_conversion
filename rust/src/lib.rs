//! Source-independent, streaming Arrow-to-Lance conversion.
//!
//! DuckDB feeds `LanceSink` directly. Native adapters implement `BatchSource`.
mod ffi;
pub mod source;
pub mod writer;

pub use source::{convert, BatchSource, ParquetFileSource};
pub use writer::{write_stream, LanceSink, WriteOptions, WriteSummary};
