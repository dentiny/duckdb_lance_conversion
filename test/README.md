# Tests

See the [project README](../README.md#test) for exact commands.

- `sql/extension_version.test`: loaded extension release version.
- `sql/lance_conversion.test`: SQL COPY counts, manifest creation, empty input,
  existing output protection, overwrite, invalid options, and unsupported types.
- `../rust/tests/conversion.rs`: native Parquet conversion, streaming batches,
  exact values, empty schema-preserving output, write errors, and abort cleanup.
  Overwrite tests cover replacement, empty replacement, preserving the previous
  version on abort, and rejecting non-dataset targets.

All Rust test entry points use `#[tokio::test]`. Lance reads are awaited directly.
Synchronous conversion APIs, Parquet I/O, and writer-drop
cleanup run through `tokio::task::spawn_blocking`; test helpers do not create their
own runtimes. SQLLogicTests remain SQL files for DuckDB's test runner.
