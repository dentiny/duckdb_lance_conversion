# Performance Notes

## Parallelism and row order

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
query supports them. Table scans, sorted results, `read_huggingface`,
`read_warc`, and DuckDB's file readers such as `read_parquet`, `read_csv`, and
`read_json` support batch indexes.

For example:

```sql
-- Batch: an ordered read.
COPY (SELECT * FROM read_huggingface('lhoestq/demo1')) TO 'a.lance' (FORMAT LANCE);
-- Parallel: GROUP BY output has no order to preserve.
COPY (SELECT bucket, count(*) FROM source GROUP BY bucket) TO 'b.lance' (FORMAT LANCE);
-- Parallel: order is explicitly not required.
COPY (SELECT * FROM read_huggingface('lhoestq/demo1'))
TO 'c.lance' (FORMAT LANCE, PRESERVE_ORDER false);
-- Regular: batch indexes are never used with a single thread.
SET threads = 1;
COPY source TO 'd.lance' (FORMAT LANCE);
```

`EXPLAIN COPY ...` shows `BATCH_COPY_TO_FILE` for the batch mode. In every
mode, a COPY publishes one Lance dataset version for its data, committed after
every thread finishes, plus one version per requested index. Indexes are built
one after another once the data is committed.

## Benchmark

The table compares COPY times before and after parallel and batch writes were
added. Each case writes 20M rows (`BIGINT`, `INTEGER`, `VARCHAR`, `DOUBLE`,
`INTEGER[]`, and an MD5 `VARCHAR`) to a local Lance dataset with 18 threads,
and reports the median wall time of 3 runs.

| Source | `preserve_insertion_order` | Mode | Before | After | Speedup |
| --- | --- | --- | --- | --- | --- |
| DuckDB table | false | Parallel | 2.82s | 0.44s | 6.5× |
| `read_parquet` | false | Parallel | 3.30s | 0.43s | 7.7× |
| DuckDB table | true | Batch | 3.92s | 3.76s | 1.04× |
| `read_parquet` | true | Batch | 3.36s | 2.61s | 1.29× |

Parallel mode scales with the thread count because every thread runs its own
Lance writer. Batch mode reads in parallel, but a single Lance writer encodes
and writes every row, so it stays close to single-threaded write speed. When
row order does not matter, `PRESERVE_ORDER false` is the fastest option.
