# DuckDB Lance Conversion

Convert data into Lance datasets through DuckDB `COPY TO`, with the conversion
core implemented in Rust. The native adapter converts a Parquet file or a
directory of Parquet files into a Lance dataset, preserving supported column
values without special blob or media processing.

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
metadata. Rust allocations are not accounted for by DuckDB's `memory_limit`.

### S3-compatible storage

An `s3://bucket/prefix.lance` destination uses the best matching DuckDB S3
secret. The extension passes its credentials and connection settings directly
to OpenDAL:

```sql
CREATE SECRET lance_s3 (
    TYPE S3,
    KEY_ID 'access-key',
    SECRET 'secret-key',
    REGION 'us-east-1',
    ENDPOINT 'localhost:9000',
    USE_SSL false,
    URL_STYLE 'path',
    SCOPE 's3://bucket'
);

COPY (SELECT * FROM read_parquet('s3://bucket/input.parquet'))
TO 's3://bucket/output.lance' (FORMAT LANCE);
```

Supported secret fields are `key_id`, `secret`, `session_token`, `endpoint`,
`region`, `use_ssl`, and `url_style` (`path` or `vhost`). The source query is
still executed by DuckDB; load `httpfs` when `read_parquet` itself reads S3.

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

For standalone S3 input/output, attach the same `S3StorageConfig` to
`ParquetFileSource::with_s3_config` and `WriteOptions::s3_config`. This API does
not read DuckDB secrets; the DuckDB extension resolves those before crossing
the FFI boundary. Directory inputs are scanned recursively for `.parquet` files
and require all files to have the same Arrow schema.

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
unfinished writer closes its input and waits for the writer task to stop. An
aborted write may leave uncommitted output. The synchronous FFI layer owns the
runtime and waits for the task on destruction.

The Lance sink rejects unsupported Arrow types, including `Map`,
`Dictionary`, `Union`, `Null`, `Duration`, `Interval`, and decimal types other
than `Decimal128`. It does not automatically cast them.

## TODO

- Add Hugging Face input resolution and authentication.
- Add WARC input conversion.
- Expand round-trip coverage for decimal, temporal, and nested types, including
  nested arrays.
- Add crash recovery and resumable conversions.
