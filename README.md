# DuckDB Lance Conversion

DuckDB Lance Conversion adds Lance as a native DuckDB `COPY` destination:

```sql
COPY (SELECT ...) TO 'dataset.lance' (FORMAT LANCE);
```

This is a DuckDB `CopyFunction`, not a standalone import command. DuckDB reads,
filters, joins, casts, and projects the source data using its normal execution
engine, then streams the result into the Rust Lance writer. There is no
intermediate Parquet export, and the complete input is not materialized in
memory.

Because the source is an ordinary DuckDB query, the extension works with any
format or data source that DuckDB can query, including:

- Parquet files, file lists, globs, and partitioned directories
- CSV and JSON
- DuckDB tables and views
- Results of joins, filters, aggregations, and expressions
- S3-compatible object storage through DuckDB filesystem extensions
- Hugging Face Parquet datasets through `read_huggingface`
- Local and S3 WARC/WARC.GZ files through `read_warc`

The Lance sink supports local and S3-compatible destinations, Create/Append/
Overwrite modes, Blob v2 storage policies, target file sizing, and scalar,
vector, full-text, and Bloom filter indexes.

## Usage

Install the extension from the DuckDB Community Extensions repository, then
load it:

```sql
FORCE INSTALL lance_conversion FROM community;
LOAD lance_conversion;

COPY (
    SELECT * FROM read_parquet('/absolute/path/input.parquet')
)
TO '/absolute/path/output.lance' (FORMAT LANCE);
```

### Source examples

`COPY` can consume a table, a view, or any DuckDB query. The source does not
need to be Parquet:

```sql
-- DuckDB table or view
COPY events TO 'events.lance' (FORMAT LANCE);
COPY active_users TO 'active_users.lance' (FORMAT LANCE);

-- One Parquet file
COPY (SELECT * FROM read_parquet('events.parquet'))
TO 'events.lance' (FORMAT LANCE);

-- A partitioned directory or glob of Parquet files
COPY (SELECT * FROM read_parquet('events/**/*.parquet', hive_partitioning = true))
TO 'partitioned_events.lance' (FORMAT LANCE);

-- An explicit list of Parquet files
COPY (SELECT * FROM read_parquet(['part-0.parquet', 'part-1.parquet']))
TO 'combined.lance' (FORMAT LANCE);

-- CSV
COPY (SELECT * FROM read_csv('events.csv'))
TO 'events_from_csv.lance' (FORMAT LANCE);

-- JSON
COPY (SELECT * FROM read_json_auto('events.json'))
TO 'events_from_json.lance' (FORMAT LANCE);

-- JSONL / NDJSON
COPY (SELECT * FROM read_ndjson_auto('events.jsonl'))
TO 'events_from_jsonl.lance' (FORMAT LANCE);

-- S3 through DuckDB's filesystem support
COPY (SELECT * FROM read_parquet('s3://bucket/events/*.parquet'))
TO 'events_from_s3.lance' (FORMAT LANCE);

-- Multiple sources and relational operations can participate in one query
COPY (
    SELECT e.id, e.timestamp, u.name
    FROM read_parquet('events/*.parquet') e
    JOIN users u USING (user_id)
    WHERE e.timestamp >= DATE '2026-01-01'
)
TO 'recent_events.lance' (FORMAT LANCE);
```

The same pattern applies to other DuckDB readers and extensions: if a source
can appear in a DuckDB `SELECT`, its supported result types can be written to
Lance.

The statement returns the number of rows written. Empty input creates a dataset
with the query's schema and zero rows.

The output is a **dataset directory**. Its parent must exist, and by default the
output path must not exist, even as an empty directory.

### Extension readers

`read_huggingface` reads the Parquet representation published for a Hugging
Face dataset. `config` and `split` default to `default` and `train`.
`preserve_insertion_order` defaults to `true`, which reads sorted Parquet
shards and their row groups sequentially. Set it to `false` to read files and
row groups concurrently, emitting batches as they become ready.
`max_read_parallelism` defaults to `8` and is capped by the total row-group
count:

```sql
COPY (
    SELECT * FROM read_huggingface(
        'lhoestq/demo1',
        config = 'default',
        split = 'train',
        preserve_insertion_order = false,
        max_read_parallelism = 8
    )
)
TO 'demo1.lance' (FORMAT LANCE);
```

If a matching DuckDB `huggingface` secret is available, its token is passed to
the reader.

`read_warc` streams records from an uncompressed or gzip-compressed WARC file.
It exposes WARC metadata fields, parsed date and length values, and the record
body:

```sql
COPY (
    SELECT id, type, date, content_type, body
    FROM read_warc('archive.warc.gz')
)
TO 'archive.lance' (FORMAT LANCE);
```

Local and `s3://` paths are supported. Projection pushdown avoids constructing
and copying WARC bodies when the query does not select `body`.
For WARC files with a JSON or CDXJ index containing `offset` and `length`,
`index_path` enables parallel range reads. `max_read_parallelism` defaults to
`8`; results remain in WARC offset order.

```sql
COPY (
    SELECT id, date, body
    FROM read_warc(
        's3://bucket/crawl/archive.warc.gz',
        index_path = 's3://bucket/crawl/archive.cdxj.gz',
        max_read_parallelism = 16
    )
)
TO 's3://bucket/datasets/archive.lance' (FORMAT LANCE);
```

### Profiling

`COPY` returns the number of rows written. DuckDB's JSON profiler also reports
rows scanned by `read_huggingface` and `read_warc`, bytes transferred by those
readers, and bytes read or written by the Lance destination:

```sql
SET profiling_coverage = 'ALL';
PRAGMA enable_profiling = 'json';
PRAGMA profiling_output = '/tmp/lance_profile.json';
PRAGMA custom_profiling_settings = '{
    "OPERATOR_ROWS_SCANNED": "true",
    "TOTAL_BYTES_READ": "true",
    "TOTAL_BYTES_WRITTEN": "true",
    "OPERATOR_TIMING": "true"
}';

COPY (
    SELECT * FROM read_warc('archive.warc.gz')
) TO 'archive.lance' (FORMAT LANCE);

PRAGMA disable_profiling;
```

Source readers also provide byte-based progress to DuckDB's progress bar when
the total source size is known. Byte counters measure compressed/encoded
storage traffic rather than decoded Arrow memory. Destination reads, such as
existing Lance metadata loaded by `APPEND`, contribute to
`TOTAL_BYTES_READ`.

### Write modes

The default mode is `Create`: the destination must not already exist. Use
`APPEND` to add rows to an existing compatible dataset:

```sql
COPY (SELECT * FROM read_parquet('next.parquet'))
TO 'output.lance' (FORMAT LANCE, APPEND);
```

Use `OVERWRITE` to replace the current contents:

```sql
COPY (SELECT * FROM read_parquet('input.parquet'))
TO 'output.lance' (FORMAT LANCE, OVERWRITE);
```

`APPEND` and `OVERWRITE` publish a new Lance dataset version after a successful
write. `OVERWRITE` also creates the dataset when it does not exist. Previous
versions remain in Lance storage. `APPEND false` and `OVERWRITE false` select
the default `Create` mode. The two options cannot be specified together.

Other DuckDB file-layout options, including `PARTITION_BY`,
`PER_THREAD_OUTPUT`, and `USE_TMP_FILE`, are rejected.

### Parallelism and row order

`FORMAT LANCE` supports three write modes. DuckDB chooses one when it plans
the COPY, using the same rules as its Parquet writer.

| Mode | Used when | Query execution | Lance writers | Row order |
| --- | --- | --- | --- | --- |
| Parallel | Row order need not be preserved | Parallel | One per thread | Not preserved |
| Batch | Row order must be preserved, and batch indexes are available | Parallel | One | Preserved |
| Regular | Row order must be preserved, but batch indexes are unavailable | Single thread | One | Preserved |

Row order need not be preserved when any of the following holds:

- The COPY sets `PRESERVE_ORDER false`.
- The query ends in `GROUP BY` or a join, whose output has no defined order.
- `SET preserve_insertion_order = false`, and the query has no `ORDER BY`,
  `LIMIT`, or streaming window function.

`PRESERVE_ORDER true` forces row order to be preserved. Batch indexes are
available when DuckDB runs with more than one thread and every source in the
query supports them. Table scans, sorted results, and DuckDB's file readers
such as `read_parquet`, `read_csv`, and `read_json` support batch indexes;
`read_huggingface` and `read_warc` do not.

For example, with the default settings:

```sql
-- Batch: an ordered table scan.
COPY source TO 'a.lance' (FORMAT LANCE);
-- Parallel: GROUP BY output has no order to preserve.
COPY (SELECT bucket, count(*) FROM source GROUP BY bucket) TO 'b.lance' (FORMAT LANCE);
-- Regular: read_huggingface does not support batch indexes.
COPY (SELECT * FROM read_huggingface('lhoestq/demo1')) TO 'c.lance' (FORMAT LANCE);
-- Parallel: order is explicitly not required.
COPY (SELECT * FROM read_huggingface('lhoestq/demo1'))
TO 'd.lance' (FORMAT LANCE, PRESERVE_ORDER false);
```

`EXPLAIN COPY ...` shows `BATCH_COPY_TO_FILE` for the batch mode. In every
mode, a COPY publishes one Lance dataset version for its data, committed after
every thread finishes, plus one version per requested index.

### Sampling

`SAMPLE_PERCENT` writes a random percentage of the source rows; each row is
kept independently with that probability (Bernoulli sampling), so the written
row count is approximate. `SAMPLE_ROWS` writes exactly that many uniformly
random rows (reservoir sampling), or every row when the source is smaller:

```sql
COPY (SELECT * FROM read_parquet('events/*.parquet'))
TO 'events_10pct.lance' (FORMAT LANCE, SAMPLE_PERCENT 10);

COPY (SELECT * FROM read_huggingface('lhoestq/demo1'))
TO 'demo1_sample.lance' (FORMAT LANCE, SAMPLE_ROWS 1000);
```

Sampling is applied to the query result before it is written, so the whole
source is still read. `SAMPLE_PERCENT` accepts values from 0 to 100,
`SAMPLE_ROWS` accepts non-negative integers, and the two options cannot be
specified together. The returned row count is the number of sampled rows
written.

### File and Blob configuration

Size options are specified in bytes:

- `BLOB_INLINE_SIZE_THRESHOLD` defaults to 2 MiB.
- `BLOB_DEDICATED_SIZE_THRESHOLD` defaults to 16 MiB. Values above it use
  dedicated storage; values between the two thresholds use packed storage.
- `TARGET_FILE_SIZE` defaults to 512 MiB and is a soft maximum data-file size.
  It applies to each writer: in a parallel write, every thread that receives
  rows writes at least one data file.

For example:

```sql
COPY (SELECT id, body FROM source)
TO 'output.lance' (
    FORMAT LANCE,
    BLOB_INLINE_SIZE_THRESHOLD 1048576,
    BLOB_DEDICATED_SIZE_THRESHOLD 8388608,
    TARGET_FILE_SIZE 268435456
);
```

`STORAGE_VERSION` selects the Lance file format version, such as `2.1`, `2.2`,
`stable`, or `next`, and is passed to Lance as-is. It defaults to `stable`.

```sql
COPY (SELECT * FROM source)
TO 'output.lance' (FORMAT LANCE, STORAGE_VERSION '2.1');
```

`COLUMN_COMPRESSION` selects a general compression algorithm for individual
top-level columns, such as `zstd`, `lz4`, or `none`. Each value is passed to
Lance as-is through the `lance-encoding:compression` field metadata. Columns
not listed use Lance's default encoding, which does not apply general
compression. See Lance's
[compression configuration](https://lance.org/format/file/encoding/#compression-configuration).

```sql
COPY (SELECT * FROM source)
TO 'output.lance' (FORMAT LANCE, COLUMN_COMPRESSION {'body': 'zstd', 'id': 'lz4'});
```

`BLOB_COLUMNS` converts selected top-level `VARCHAR` columns containing local
paths or URIs into Lance Blob v2 columns. Lance asynchronously reads each
referenced object during the write and stores its bytes using the thresholds
above; it does not retain the source URI as an external reference. NULL values
remain NULL.

```sql
COPY (
    SELECT id, image_url, audio_url
    FROM read_parquet('metadata.parquet')
)
TO 'media.lance' (
    FORMAT LANCE,
    BLOB_COLUMNS (image_url, audio_url)
);
```

Every named column must exist and have type `VARCHAR`. A failed object read
fails the COPY.

### Indexes

Index options create indexes after writing the dataset. Users select the
purpose; the Rust writer chooses the corresponding Lance implementation:

- `SCALAR_INDEX_COLUMNS` creates BTree indexes.
- `VECTOR_INDEX_COLUMNS` creates IVF_FLAT indexes with L2 distance.
- `TEXT_INDEX_COLUMNS` creates Inverted full-text indexes.
- `BLOOM_FILTER_INDEX_COLUMNS` creates Bloom filter indexes for equality and
  membership tests.

```sql
COPY (
    SELECT id, event_id, category, description, embedding
    FROM read_parquet('vectors.parquet')
)
TO 'vectors.lance' (
    FORMAT LANCE,
    SCALAR_INDEX_COLUMNS (id, category),
    VECTOR_INDEX_COLUMNS (embedding),
    TEXT_INDEX_COLUMNS (description),
    BLOOM_FILTER_INDEX_COLUMNS (event_id)
);
```

Scalar and Bloom filter options accept supported scalar values, text indexes
require `VARCHAR`, and vector indexes require fixed-size numeric arrays. A
column may appear in only one index option per COPY. Unsupported combinations
are rejected before data is written. Index creation publishes additional Lance
dataset versions and can fail after the data version has committed.

### Complete write configuration

The write options can be combined in one native `COPY` statement:

```sql
COPY (
    SELECT
        id,
        event_id,
        category,
        description,
        embedding,
        asset_uri
    FROM read_parquet('catalog/**/*.parquet')
)
TO 'catalog.lance' (
    FORMAT LANCE,

    -- Omit both flags for Create mode; use APPEND instead to add rows.
    OVERWRITE,

    -- Optional: write a sample instead of every row (or SAMPLE_ROWS n).
    SAMPLE_PERCENT 10,

    -- Lance data and Blob v2 layout.
    TARGET_FILE_SIZE 536870912,
    BLOB_INLINE_SIZE_THRESHOLD 2097152,
    BLOB_DEDICATED_SIZE_THRESHOLD 16777216,
    BLOB_COLUMNS (asset_uri),

    -- Lance indexes.
    SCALAR_INDEX_COLUMNS (id, category),
    VECTOR_INDEX_COLUMNS (embedding),
    TEXT_INDEX_COLUMNS (description),
    BLOOM_FILTER_INDEX_COLUMNS (event_id)
);
```

`APPEND` and `OVERWRITE` are mutually exclusive. Blob columns cannot also be
index columns. Indexes are created after the data write succeeds.

### Types and conversion limits

Supported DuckDB types are booleans, signed/unsigned integers through 64 bits,
float/double, decimal, varchar, blob, `UUID`, `ENUM`, date/time/timestamp
(including `TIMESTAMPTZ`), and recursively supported `LIST`, fixed-size `ARRAY`,
`STRUCT`, and `MAP`. Nested types are accepted when every child type is supported.
`UUID` is stored as its canonical 36-character string. `ENUM` is stored as a
dictionary-encoded string; DuckDB uses an 8-bit dictionary key for up to 255
values and wider keys beyond that, and Lance rejects `APPEND` when the key
width differs from the existing dataset.

Types outside this list are rejected, including:

- `HUGEINT` and `UHUGEINT` (128-bit integers)
- `UNION`
- `INTERVAL`
- `TIME WITH TIME ZONE` (`TIMETZ`)
- Untyped `NULL` (for example, `SELECT NULL AS value`)

Explicitly cast unsupported columns to a supported type before exporting,
for example `interval_column::VARCHAR` or `NULL::INTEGER`. NULL values within
supported typed columns are allowed.

Top-level `BLOB` columns use Lance Blob v2 storage. Depending on the reader,
they may be exposed as a logical struct containing `data` and `uri`. Nested
binary fields remain ordinary binary columns. Conversion does not preserve
original Parquet encodings, field IDs, key/value metadata, or Hugging Face
feature metadata. The extension reserves bounded memory budgets for its Rust
readers and Lance writer against DuckDB's `memory_limit`; the writer also
accounts exported Arrow batches for their full Rust-side lifetime. Allocations
inside Rust dependencies are represented by conservative reservations rather
than exact per-allocation tracking.

### S3-compatible storage

An `s3://bucket/prefix.lance` destination uses the best matching scoped DuckDB
S3 secret. The extension passes its credentials and connection settings to
OpenDAL, which backs the Lance object store:

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

Run these APIs inside a Tokio runtime. DuckDB uses `LanceDatasetWriter`: each
thread pushes batches into its own `LanceFragmentWriter` (`write_batch`, then
`finish`) through a bounded channel, and `LanceDatasetWriter::commit` publishes
the fragments of every finished writer as one dataset version. `LanceWriter`
wraps one dataset writer with one fragment writer. Dropping an unfinished
fragment writer closes its input; its fragments are never committed but may be
left as uncommitted output. The synchronous FFI layer owns the runtime and
waits for writer tasks on destruction.

The Lance sink rejects unsupported Arrow types, including `Union`, `Null`,
`Duration`, `Interval`, and decimal types other than `Decimal128`. It does not
automatically cast them.

## TODO

- Add per-stage timing for download, decode, Lance write, and index creation.
- Integrate cancellation with DuckDB.
- Add crash recovery, resumable conversions, and cleanup of uncommitted files.
