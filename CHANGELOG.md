# 0.2.0

## Fixed

- Remove the obsolete DONT_LINK option from the optional Lance test dependency so DuckDB v2 configuration succeeds with unit tests enabled.

## Changed

- Upgrade DuckDB to `v2.0.0`, extension-ci-tools to current main, and the Lance reader used by round-trip SQL tests to its current DuckDB v2-compatible revision.
- Use DuckDB's owned Arrow scan factory API and SQL identifiers for source and COPY column names, with string conversion only at Arrow and Rust FFI boundaries.
- Register optional reader parameters through DuckDB v2 typed keyword options, preserving their existing defaults and NULL validation.
- Migrate profiling counters and the profiling SQL test to DuckDB v2 metric APIs and names. Reuse DuckDB's Arrow partition callback instead of duplicating it.
- Use C++17 and string views for static setting and option names.
- Align Cargo, extension metadata, and the fallback extension version at `0.2.0`.

# 0.1.5

## Added

- Add `read_atif` for ATIF v1.0–v1.8 agent trajectories in local and S3 JSON/JSONL files. Each trajectory becomes one row with nested `agent` and `steps` fields, the original document in `raw_json`, and provenance in `source_uri` and `source_record_index`; the input format is inferred from the file extension or set through `format` ([#44]).

## Changed

- Switch `read_warc` to the `warc-rs` fork, with asynchronous WARC parsing and gzip decompression for sequential and indexed reads. Indexed reads now stream batches instead of collecting all decoded batches for each range before returning them ([#43]).
- Update the pinned `warc-rs` revision to use OpenDAL `0.59` and reduce transitive dependencies.
- Add jitter to storage retries so concurrent operations do not retry in lockstep, while retaining the existing `lance_conversion_storage_*` timeout and backoff settings ([#45]).

## Fixed

- Read invalid UTF-8 WARC header values with replacement characters instead of failing the scan with a parser panic ([#43]).

[#43]: https://github.com/dentiny/duckdb_lance_conversion/pull/43
[#44]: https://github.com/dentiny/duckdb_lance_conversion/pull/44
[#45]: https://github.com/dentiny/duckdb_lance_conversion/pull/45

# 0.1.4

## Added

- Add per-attempt timeouts and exponential-backoff retries for S3 WARC reads, Hugging Face Parquet reads, and S3 Lance writes, configured through the `lance_conversion_storage_*` settings ([#40]).
- Push column projection down into `read_huggingface`, so only the selected Parquet column chunks are fetched and decoded, and progress counts only those bytes ([#41]).
- Decode `read_huggingface` row groups in the background, up to `max_read_parallelism` at a time, so reading continues while DuckDB processes earlier batches. The new `read_ahead_bytes` option (default 32 MiB, `0` to disable) caps the decoded data each row group may queue ([#42]).

## Changed

- `read_huggingface` now loads every shard's Parquet footer concurrently before the first batch, in both ordered and unordered modes ([#41]).
- With `preserve_insertion_order = true` (the default), `read_huggingface` now reads later row groups ahead while still emitting them in order, instead of reading one row group at a time ([#42]).

[#40]: https://github.com/dentiny/duckdb_lance_conversion/pull/40
[#41]: https://github.com/dentiny/duckdb_lance_conversion/pull/41
[#42]: https://github.com/dentiny/duckdb_lance_conversion/pull/42

# 0.1.3

## Added

- Write Lance datasets in parallel: every DuckDB thread runs its own Lance writer when row order need not be preserved, and order-preserving COPY reads in parallel through DuckDB's batch COPY. All writers commit as one Lance dataset version ([#37]).
- Support batch indexes in `read_huggingface` and `read_warc`, so order-preserving COPY from these readers uses the parallel batch mode ([#38]).
- Support `UUID` (stored as a canonical string), `ENUM` (stored as a dictionary-encoded string), and `MAP` columns ([#36]).
- Keep the canonical Arrow `UUID` and `JSON` extension types when reading Hugging Face Parquet shards ([#36]).
- Add [performance notes](docs/performance_notes.md) ([#39]).

## Fixed

- Use 64-bit Arrow offsets so a chunk whose string or blob column exceeds 2 GiB no longer overflows ([#35]).
- Work around an undefined-symbol link error in `SAMPLE_ROWS` validation ([#34]).

[#34]: https://github.com/dentiny/duckdb_lance_conversion/pull/34
[#35]: https://github.com/dentiny/duckdb_lance_conversion/pull/35
[#36]: https://github.com/dentiny/duckdb_lance_conversion/pull/36
[#37]: https://github.com/dentiny/duckdb_lance_conversion/pull/37
[#38]: https://github.com/dentiny/duckdb_lance_conversion/pull/38
[#39]: https://github.com/dentiny/duckdb_lance_conversion/pull/39

# 0.1.2

## Added

- Add `SAMPLE_PERCENT` (Bernoulli) and `SAMPLE_ROWS` (reservoir) COPY options to write a random sample of the source rows; the source is still read in full ([#32]).

# 0.1.1

## Added

- Add concurrent Hugging Face Parquet reads with configurable insertion-order preservation and maximum read parallelism ([#19]).
- Add parallel range reads for indexed WARC and WARC.GZ files using JSON or CDXJ indexes, while preserving archive offset order ([#19]).
- Report source rows and bytes, Lance destination I/O, and byte-based progress through DuckDB's profiling and progress APIs ([#20]).

## Fixed

- Verify Blob v2 round trips through the Lance Blob API instead of the SQL reader, which does not materialize Blob v2 payloads ([#29]).

## Changed

- Update DuckDB and extension-ci-tools to `v1.5.6`, upgrade Lance to latest main (`b94b20c785ca`), and refresh the Lance reader used by SQL tests ([#26]).
- Discover Hugging Face Parquet shards through the Hugging Face datasets API and use their reported sizes for accurate progress metrics ([#21]).
- Use `flate2` with zlib-ng for WARC gzip decompression ([#17]).
- Reduce the Rust dependency footprint and remove unused standalone Parquet source code ([#18], [#21]).

[#17]: https://github.com/dentiny/duckdb_lance_conversion/pull/17
[#18]: https://github.com/dentiny/duckdb_lance_conversion/pull/18
[#19]: https://github.com/dentiny/duckdb_lance_conversion/pull/19
[#20]: https://github.com/dentiny/duckdb_lance_conversion/pull/20
[#21]: https://github.com/dentiny/duckdb_lance_conversion/pull/21
[#26]: https://github.com/dentiny/duckdb_lance_conversion/pull/26
[#29]: https://github.com/dentiny/duckdb_lance_conversion/pull/29
[#32]: https://github.com/dentiny/duckdb_lance_conversion/pull/32
