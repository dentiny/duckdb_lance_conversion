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

There is also an executable example:

```sh
cargo run --locked --manifest-path rust/Cargo.toml --example convert_parquet --   input.parquet output.lance
```

New native readers implement `BatchSource::open`, returning a
`RecordBatchReader + Send` with a fixed schema. Callers already producing Arrow
can use `LanceSink::create`, `write_batch`, and `finish` directly.
See [the interface and lifecycle design](docs/architecture.md).

## Versions

These are separate version numbers:

| Version | Value | Source |
| --- | --- | --- |
| This extension | `0.1.0` | `rust/Cargo.toml` package version |
| Compatible DuckDB | `v1.5.4` | pinned `duckdb` submodule |
| Lance Rust dependency | `11.0.0` | pinned Cargo dependency and lockfile |

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
pins Lance 11.0.0 and Arrow 58.4.0; local verification used Rust 1.97.1 and macOS
arm64. No OpenSSL/vcpkg dependency is needed by the extension template anymore.

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
./build/reldebug/test/unittest test/sql/lance_conversion.test
```

Build/run Rust core tests and the end-to-end test against the extension:

```sh
DUCKDB_BINARY="$PWD/build/reldebug/duckdb" LANCE_EXTENSION="$PWD/build/reldebug/extension/lance_conversion/lance_conversion.duckdb_extension" cargo test --locked --manifest-path rust/Cargo.toml   --target-dir build/reldebug/rust-lance-conversion -- --include-ignored
```

Without `--include-ignored`, the integration test is skipped because it requires
the separately built DuckDB shell and extension. Core tests still run.
The integration test generates one Parquet file, exports it via COPY, reads the
Lance dataset with Rust, and compares values across all 10,001 rows. It also
checks empty input, existing outputs, upstream failure, and unsupported options
and types. The native tests cover writer failure, schema mismatch, aborted writes,
and multiple input batches.

## Scope and limits

- Implemented input adapter: one local Parquet file. Directory discovery,
  Hugging Face resolution/authentication, and WARC parsing are future adapters.
  DuckDB COPY can consume other SQL relations, but this milestone validates only
  the single-file Parquet workflow and local output.
- Supported DuckDB types: booleans; signed/unsigned integers through 64 bits;
  float/double; decimal; varchar; blob; date/time/timestamp; and recursively
  supported list, fixed-size array, and struct columns. Unsupported types such
  as HUGEINT, UHUGEINT, UUID, MAP, ENUM, UNION, INTERVAL, and TIME WITH TIME ZONE
  require an explicit cast. Untyped NULL also requires a cast.
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
