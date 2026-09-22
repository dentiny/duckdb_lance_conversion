# Tests

See the [project README](../README.md#test) for exact commands.

- `sql/lance_conversion.test`: SQL COPY counts, manifest creation, empty input,
  existing output protection, overwrite, invalid options, and unsupported types.
- `../rust/tests/conversion.rs`: native Parquet conversion, streaming batches,
  exact values, empty schema-preserving output, write errors, and abort cleanup.
  Overwrite tests cover replacement, empty replacement, preserving the previous
  version on abort, and rejecting non-dataset targets.
- `../rust/tests/duckdb_copy.rs`: actual DuckDB COPY output read back with Lance
  and compared against the source Parquet across 10,001 rows and nested columns.

The last test is ignored by default because it needs a built DuckDB and extension.
Set `DUCKDB_BINARY` and `LANCE_EXTENSION` and pass `--include-ignored` to run it.

All Rust test entry points use `#[tokio::test]`. Lance reads and DuckDB subprocesses
are awaited directly. Synchronous conversion APIs, Parquet I/O, and writer-drop
cleanup run through `tokio::task::spawn_blocking`; test helpers do not create their
own runtimes. SQLLogicTests remain SQL files for DuckDB's test runner.
