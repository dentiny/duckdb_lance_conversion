# DuckDB Lance Conversion

Stream a single Parquet file into a local Lance dataset using DuckDB `COPY TO`.
The conversion core is Rust; a C++ adapter registers the COPY format and exports
DuckDB chunks through the Arrow C Data Interface.

## Use

The extension targets the pinned DuckDB **v1.5.4** submodule. In the shell built
by this repository it is linked in automatically:

```sh
./build/reldebug/duckdb
```

```sql
COPY (
    SELECT * FROM read_parquet('/absolute/path/input.parquet')
)
TO '/absolute/path/output.lance' (FORMAT LANCE);
```

`output.lance` is a **dataset directory**, not a single file. Its parent must
exist. By default the destination itself must not exist, even as an empty directory.
`OVERWRITE` permits replacing an existing valid Lance dataset; ordinary files and
non-dataset directories are rejected. The statement returns the number of rows written. Empty input creates a dataset
with the query's schema and zero rows.

For a separate v1.5.4 DuckDB shell, start it with `-unsigned` and load the binary:

```sql
LOAD '/absolute/path/duckdb_lance_conversion/build/reldebug/extension/lance_conversion/lance_conversion.duckdb_extension';
```

The only optional COPY parameter is `OVERWRITE` (equivalently, `OVERWRITE true`):

```sql
COPY (SELECT * FROM read_parquet('input.parquet'))
TO 'output.lance' (FORMAT LANCE, OVERWRITE);
```

`OVERWRITE false` retains the default existing-path protection. Overwrite can
also create a dataset at a new path. It publishes a new Lance version after the
input has completed successfully; it does not delete the existing dataset first.
Previous versions remain in Lance storage. Lance's default writer settings are
used. Other COPY options, including `APPEND`, `PARTITION_BY`, `PER_THREAD_OUTPUT`,
and `USE_TMP_FILE`, are rejected.

## Rust interface

The standalone core does not require DuckDB:

```rust
use lance_conversion::{convert, ParquetFileSource, WriteOptions};

let result = convert(
    ParquetFileSource::new("input.parquet").with_batch_size(8192),
    "output.lance",
    WriteOptions::default(),
)?;
println!("{} rows written", result.rows_written);
```

New native readers implement `BatchSource::open`, returning a
`RecordBatchReader + Send` with a fixed schema. Callers already producing Arrow
can use `LanceSink::create`, `write_batch`, and `finish` directly.

## Versions

These are separate version numbers:

| Version | Value | Source |
| --- | --- | --- |
| This extension | `0.1.0` | `rust/Cargo.toml` package version |
| Compatible DuckDB | `v1.5.4` | pinned `duckdb` submodule |
| Lance Rust dependency | `f3dc9364c07d6e84706e1f07a9a0654b4ec480f1` | upstream Git revision and Cargo lockfile |

`extension_config.cmake` reads the Cargo package version and passes it as
`EXTENSION_VERSION`, so DuckDB's extension metadata and `Version()` report the
same release version instead of the repository commit hash. After changing it,
reconfigure/rebuild; an already-built binary retains its previous version.

```sql
SELECT extension_name, extension_version, loaded
FROM duckdb_extensions()
WHERE extension_name = 'lance_conversion';
```

## Build

Prerequisites: CMake, a C++17 compiler, Cargo/Rust, and `protoc`. The Rust lockfile
pins the Lance upstream revision above and resolves the Arrow 58 dependency family.
Cargo uses the committed lockfile for reproducible builds. No OpenSSL/vcpkg
dependency is needed by the extension template anymore.

```sh
git submodule update --init --recursive
CMAKE_BUILD_PARALLEL_LEVEL=8 CARGO_BUILD_JOBS=8 make reldebug
```

CMake builds the Rust static library automatically. Rust defaults to the release
profile independently of DuckDB's build type. For a faster development build:

```sh
CMAKE_BUILD_PARALLEL_LEVEL=8 CARGO_BUILD_JOBS=8   make reldebug EXT_FLAGS='-DLANCE_CARGO_PROFILE=dev'
```

The Cargo output lives under `build/reldebug/rust-lance-conversion`.
The C++ extension is tied to the DuckDB version it was built against.

## Test

Run the SQL regression using the already-built runner (no compilation):

```sh
./build/reldebug/test/unittest "test/sql/*.test"
```

Run the standalone Rust tests (all run by default):

```sh
cargo test --locked --manifest-path rust/Cargo.toml --target-dir build/reldebug/rust-lance-conversion
```

The Rust tests cover single-file Parquet conversion, exact values across 10,001
rows, empty input, existing outputs, writer failure, schema mismatch, aborted
writes, and overwrite behavior. The SQL tests exercise COPY and extension version
reporting through DuckDB's test runner.

## Scope and limits

- Implemented input adapter: one local Parquet file. Directory discovery,
  Hugging Face resolution/authentication, and WARC parsing are future adapters.
  DuckDB COPY can consume other SQL relations, but this milestone validates only
  the single-file Parquet workflow and local output.
- Supported DuckDB types: booleans; signed/unsigned integers through 64 bits;
  float/double; decimal; varchar; blob; date/time/timestamp; and recursively
  supported list, fixed-size array, and struct columns. See the unsupported
  types below for conversion restrictions.
- The mapping preserves supported query values, not original Parquet encodings,
  field IDs, key/value metadata, or Hugging Face feature metadata. Binary values
  stay binary; there is no media downloading, embedding, or special blob storage.
- A zero-capacity handoff channel supplies backpressure. Lance has its own buffers;
  Rust allocations are not accounted for by DuckDB's `memory_limit`. Arrow export
  may copy data; this is not advertised as an end-to-end zero-copy implementation.
- A successful `finish` commits one dataset version. Normal errors or dropping an
  unfinished writer join the worker and attempt to remove a newly created output
  directory. Failed overwrites preserve the previous dataset version and may leave
  uncommitted data files for later Lance cleanup. Process termination or cleanup
  I/O failure can also leave partial files; there is no crash recovery or resume yet. External writes are not rolled back
  by a subsequent DuckDB SQL transaction rollback.

### Unsupported types

`COPY ... TO ... (FORMAT LANCE)` rejects types outside the supported list above,
including:

- `HUGEINT` and `UHUGEINT` (128-bit integers)
- `UUID`
- `MAP`, `ENUM`, and `UNION`
- `INTERVAL`
- `TIME WITH TIME ZONE` (`TIMETZ`)
- Untyped `NULL` (for example, `SELECT NULL AS value`)

Explicitly cast unsupported columns to a supported type before exporting.
For example, use `uuid_column::VARCHAR` or `NULL::INTEGER`. NULL values within
supported typed columns are allowed.

Type validation is recursive: nested `LIST`, fixed-size `ARRAY`, and `STRUCT`
columns are accepted only when every child type is supported. Nested arrays are
allowed by the implementation but do not yet have end-to-end round-trip tests.

The standalone Rust Parquet converter also rejects unsupported Arrow types,
including `Map`, `Dictionary`, `Union`, `Null`, `Duration`, `Interval`, and decimal
types other than `Decimal128`. It does not automatically cast these types.
