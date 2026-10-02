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
