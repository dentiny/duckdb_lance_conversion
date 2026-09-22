# DuckDB Lance Conversion

Convert data into Lance datasets through DuckDB `COPY TO`, with the conversion
core implemented in Rust. The first supported workflow converts a single local
Parquet file into a local Lance dataset, preserving supported column values
without special blob or media processing.

## Usage

Use a compatible DuckDB v1.5.4 shell with the extension loaded. For an unsigned
local extension, start DuckDB with `-unsigned` and load it:

```sql
LOAD '/absolute/path/lance_conversion.duckdb_extension';

COPY (
    SELECT * FROM read_parquet('/absolute/path/input.parquet')
)
TO '/absolute/path/output.lance' (FORMAT LANCE);
```

The COPY query can select columns, filter rows, and cast types before conversion.
The statement returns the number of rows written. Empty input creates a dataset
with the query's schema and zero rows.

The output is a **dataset directory**. Its parent must exist, and by default the
output path must not exist, even as an empty directory.

### Overwrite

`OVERWRITE` is the only optional COPY parameter:

```sql
COPY (SELECT * FROM read_parquet('input.parquet'))
TO 'output.lance' (FORMAT LANCE, OVERWRITE);
```

It creates a new dataset or publishes a new version of an existing valid Lance
dataset after successful conversion. Previous versions remain in Lance storage.
Ordinary files and non-dataset directories are rejected. `OVERWRITE false`
retains the default existing-path protection.

Other COPY options, including `APPEND`, `PARTITION_BY`, `PER_THREAD_OUTPUT`, and
`USE_TMP_FILE`, are rejected. Lance's default writer settings are used.

### Types and conversion limits

Supported DuckDB types are booleans, signed/unsigned integers through 64 bits,
float/double, decimal, varchar, blob, date/time/timestamp (including
`TIMESTAMPTZ`), and recursively supported `LIST`, fixed-size `ARRAY`, and `STRUCT`.
Nested arrays are accepted when every child type is supported.

Types outside this list are rejected, including:

- `HUGEINT` and `UHUGEINT` (128-bit integers)
- `UUID`
- `MAP`, `ENUM`, and `UNION`
- `INTERVAL`
- `TIME WITH TIME ZONE` (`TIMETZ`)
- Untyped `NULL` (for example, `SELECT NULL AS value`)

Explicitly cast unsupported columns to a supported type before exporting,
for example `uuid_column::VARCHAR` or `NULL::INTEGER`. NULL values within
supported typed columns are allowed.

Binary values remain ordinary binary columns. Conversion does not preserve
original Parquet encodings, field IDs, key/value metadata, or Hugging Face feature
metadata. Only local output paths are supported. Rust allocations are not
accounted for by DuckDB's `memory_limit`.

Failed overwrites preserve the previous dataset version but may leave
uncommitted files. Process termination or cleanup failures can leave partial
output. A subsequent DuckDB transaction rollback does not roll back Lance writes.

### Rust usage

The standalone core can convert Parquet without DuckDB:

```rust
use lance_conversion::{convert, LanceSink, ParquetFileSource, WriteOptions};

let result = convert(
    ParquetFileSource::new("input.parquet").with_batch_size(8192),
    LanceSink::new("output.lance", WriteOptions::default()),
).await?;
println!("{} rows written", result.rows_written);
```

`convert(source, sink)` connects any `BatchSource` to any `BatchSink`. A source
returns `(SchemaRef, BatchStream)`, where each stream item is an
`anyhow::Result<RecordBatch>`. A sink consumes the schema and stream through its
async `write` method. Public interfaces use Arrow and `futures::Stream`;
DataFusion adaptation stays inside the Lance implementation.

Callers with an existing Arrow stream can write it directly:

```rust
use lance_conversion::{BatchSink, LanceSink, WriteOptions};

let result = LanceSink::new("output.lance", WriteOptions::default())
    .write(schema, batches)
    .await?;
```

Run these APIs inside a Tokio runtime. DuckDB uses the incremental `LanceWriter`
adapter (`create`, `write_batch`, and `finish`) with a bounded channel. Dropping an
unfinished writer closes its input; its task asynchronously removes newly created
output. Keep the runtime alive for cleanup to complete. The synchronous FFI layer
owns the runtime and waits for async operations and cleanup on destruction.

The Lance sink rejects unsupported Arrow types, including `Map`,
`Dictionary`, `Union`, `Null`, `Duration`, `Interval`, and decimal types other
than `Decimal128`. It does not automatically cast them.

## TODO

- Support directories of Parquet files in the native input adapter.
- Add Hugging Face input resolution and authentication.
- Add WARC input conversion.
- Expand round-trip coverage for decimal, temporal, and nested types, including
  nested arrays.
- Add crash recovery and resumable conversions.
