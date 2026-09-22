# Conversion interfaces

## Responsibilities

```text
DuckDB read_parquet + SELECT -> COPY adapter -> Arrow C Data -> LanceSink
                                                                    |
Native BatchSource -> RecordBatchReader -> convert ------------------+
                                                                    |
                                                              Lance dataset
```

- `src/lance_conversion_extension.cpp`: registers `FORMAT LANCE`, validates
  DuckDB types and the optional `OVERWRITE` flag, and exports schema/chunks through Arrow C Data. A custom
  COPY plan disables DuckDB's generic single-file overwrite/rotation behavior,
  which does not match a Lance dataset directory. COPY uses a serial sink.
- `rust/src/ffi.rs`: imports Arrow buffers, owns opaque Rust writer handles, and
  translates Rust errors/panics into C status codes with thread-local messages.
  C++ raises DuckDB exceptions. The C header defines the ownership contract.
- `rust/src/source.rs`: the `BatchSource` interface, the single-file
  `ParquetFileSource`, and a `convert` orchestration function.
- `rust/src/writer.rs`: source-independent validation, destination ownership,
  backpressure, the writer thread/runtime, Lance write options, and cleanup.

This follows the CMake + Rust staticlib structure in `duckdb-object-storage`.
The C++ shim uses the pinned v1.5.4 API; Rust does not link a second DuckDB.
The Rust crate also builds as an rlib so native callers can bypass the SQL adapter.

## Source contract

```rust
pub trait BatchSource {
    fn open(self) -> anyhow::Result<Box<dyn arrow_array::RecordBatchReader + Send>>;
}
```

A source resolves its input before writing and supplies a fixed Arrow schema.
It yields batches incrementally and reports read errors through the iterator.
Every batch must match the schema, including field nullability and metadata.
`convert` drops its sink on source error, preventing a partial successful commit.

Future adapters can use the same contract:

- Parquet directory: discover files and establish schema compatibility before
  yielding batches. Define file order and schema-union policy explicitly.
- Hugging Face: resolve revision/config/split, credentials, and Parquet shards
  before reusing the Parquet reading layer. Do not embed Hub behavior in the sink.
- WARC: stream all records and gzip members into a fixed schema, batching records
  without reading an entire archive into a scalar value.

These adapters are not implemented in this milestone. The SQL path can instead
use a DuckDB reader/table function and feed the same sink.

## Sink lifecycle

1. `LanceSink::create(path, schema, options)` validates the contract and atomically
   reserves a new local directory with `create_dir`. The only write option is
   `overwrite`. When enabled for an existing directory, the worker opens it as a
   valid Lance dataset and uses Lance's overwrite mode. It never deletes the old
   dataset to begin a write. Ordinary files and non-dataset directories are rejected.
2. A dedicated worker owns a Tokio runtime and calls Lance `Dataset::write` once.
3. `write_batch` moves each batch through a zero-capacity channel. The worker's
   `RecordBatchReader` turns messages into Lance's streaming input.
4. Only `finish` sends an explicit end-of-stream message, waits for the Lance
   commit, and marks the output successful.
5. Dropping the sender without `finish` yields an Arrow error, not EOF. Dropping
   an unfinished sink waits for the worker to stop before attempting cleanup.
   Cleanup can remove only a directory this write created. Failed overwrites
   preserve the prior committed version; uncommitted files may remain.

The distinction between error and EOF is essential: losing the producer must not
make a truncated input look like a complete dataset. Buffer ownership extends
through Rust's Arrow arrays until Lance releases them; the C++ chunk can then be
reused. Arrow's release callback is cleared on transfer to avoid double release.

The writer is single-owner; its methods must not be called concurrently through
FFI. Runtime/memory tuning, parallel fragment writes, append semantics,
cloud output, crash recovery, and immediate cancellation of a blocked I/O operation
are separate future work. They do not require changing the source interface.
