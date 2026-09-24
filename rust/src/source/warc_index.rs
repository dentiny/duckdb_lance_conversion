use std::io::{BufRead, BufReader, Cursor};
use std::ops::Range;
use std::panic::{catch_unwind, AssertUnwindSafe};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use flate2::read::MultiGzDecoder;
use futures::{stream, StreamExt, TryStreamExt};
use serde_json::Value;

use super::warc::stream_records;
use super::BatchStream;
use crate::storage::OpendalStorage;
use crate::{Error, Result, S3StorageConfig};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WarcIndexEntry {
    offset: u64,
    length: u64,
}

fn json_u64(object: &Value, name: &str, line_number: usize) -> Result<u64> {
    let value = object
        .get(name)
        .ok_or_else(|| Error::message(format!("WARC index line {line_number} has no {name}")))?;
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .ok_or_else(|| Error::message(format!("WARC index line {line_number} has invalid {name}")))
}

fn index_filename_matches(source_path: &str, filename: &str) -> bool {
    source_path.ends_with(filename)
        || std::path::Path::new(source_path).file_name()
            == std::path::Path::new(filename).file_name()
}

fn parse_warc_index(
    bytes: bytes::Bytes,
    gzipped: bool,
    source_path: &str,
) -> Result<Vec<WarcIndexEntry>> {
    let reader: Box<dyn BufRead> = if gzipped {
        Box::new(BufReader::new(MultiGzDecoder::new(Cursor::new(bytes))))
    } else {
        Box::new(BufReader::new(Cursor::new(bytes)))
    };
    let mut entries = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line.map_err(|error| {
            Error::message(format!("reading WARC index line {line_number}: {error}"))
        })?;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let json_start = line.find('{').ok_or_else(|| {
            Error::message(format!("WARC index line {line_number} is not JSON or CDXJ"))
        })?;
        let object: Value = serde_json::from_str(&line[json_start..]).map_err(|error| {
            Error::message(format!("invalid WARC index line {line_number}: {error}"))
        })?;
        if let Some(filename) = object.get("filename").and_then(Value::as_str) {
            if !index_filename_matches(source_path, filename) {
                continue;
            }
        }
        let offset = json_u64(&object, "offset", line_number)?;
        let length = json_u64(&object, "length", line_number)?;
        if length == 0 {
            return Err(Error::message(format!(
                "WARC index line {line_number} has zero length"
            )));
        }
        entries.push(WarcIndexEntry { offset, length });
    }
    if entries.is_empty() {
        return Err(Error::message(
            "WARC index contains no entries for the source file",
        ));
    }
    Ok(entries)
}

/// Sorts index entries by archive offset and converts them into range-read
/// workloads.
///
/// The target compressed size is the total indexed length divided by
/// `max_read_parallelism`, rounded up. Exactly adjacent entries are greedily
/// coalesced until a workload reaches that target. Record boundaries and
/// unindexed gaps can prevent exact balancing, and gaps always start a new
/// workload. Overlapping entries are rejected because they would decode the
/// same compressed bytes more than once. Returned ranges remain in physical
/// archive order.
///
/// For example, entries `(offset, length)` of `(20, 5)`, `(0, 8)`, `(8, 4)`
/// with parallelism `2` have a target of 9 bytes and become `0..12` and
/// `20..25`: the first two sorted entries are adjacent, while the gap from 12
/// to 20 starts a new workload.
fn build_index_work_units(
    mut entries: Vec<WarcIndexEntry>,
    max_read_parallelism: usize,
) -> Result<Vec<Range<u64>>> {
    if max_read_parallelism == 0 {
        return Err(Error::message("max_read_parallelism must be positive"));
    }
    let total_length = entries.iter().try_fold(0_u64, |total, entry| {
        total
            .checked_add(entry.length)
            .ok_or_else(|| Error::message("WARC index total length overflows u64"))
    })?;
    let parallelism = u64::try_from(max_read_parallelism).unwrap_or(u64::MAX);
    let target_length = total_length.div_ceil(parallelism);

    entries.sort_unstable_by_key(|entry| entry.offset);
    let mut units: Vec<Range<u64>> = Vec::new();
    for entry in entries {
        let end = entry
            .offset
            .checked_add(entry.length)
            .ok_or_else(|| Error::message("WARC index offset plus length overflows u64"))?;
        if let Some(unit) = units.last_mut() {
            if entry.offset < unit.end {
                return Err(Error::message("WARC index entries overlap"));
            }
            if entry.offset == unit.end && unit.end - unit.start < target_length {
                unit.end = end;
                continue;
            }
        }
        units.push(entry.offset..end);
    }
    Ok(units)
}

fn parse_indexed_warc_bytes(
    bytes: bytes::Bytes,
    gzipped: bool,
    batch_size: usize,
    projection: Vec<usize>,
    schema: SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let reader: Box<dyn BufRead> = if gzipped {
            Box::new(BufReader::new(MultiGzDecoder::new(Cursor::new(bytes))))
        } else {
            Box::new(BufReader::new(Cursor::new(bytes)))
        };
        let mut batches = Vec::new();
        stream_records(reader, batch_size, projection, schema, |batch| {
            batches.push(batch.finish()?);
            Ok(true)
        })?;
        Ok(batches)
    }));
    match result {
        Ok(result) => result,
        Err(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("unknown panic");
            Err(Error::message(format!("panic in WARC parser: {message}")))
        }
    }
}

/// Opens a WARC file through an external JSON or CDXJ index.
///
/// The index is loaded from `index_path`, optionally decompressed when its
/// filename ends in `.gz`, and filtered to entries matching `path`. Indexed
/// offsets are converted into ordered range-read workloads. Up to
/// `max_read_parallelism` workloads are fetched and parsed concurrently, while
/// `buffered` preserves physical archive order in the returned batch stream.
/// `gzipped` controls whether each WARC range is decoded as concatenated gzip
/// members before record parsing.
///
/// Returns an error for unreadable or malformed indexes, invalid or
/// overlapping ranges, storage failures, and WARC decode failures.
pub(super) async fn open_indexed_warc(
    operator: opendal::Operator,
    path: String,
    gzipped: bool,
    index_path: &str,
    s3_config: Option<&S3StorageConfig>,
    batch_size: usize,
    projection: Vec<usize>,
    schema: SchemaRef,
    max_read_parallelism: usize,
) -> Result<(SchemaRef, BatchStream)> {
    let index_storage = OpendalStorage::from_path(index_path, s3_config)?;
    let index_object_path = index_storage.object_path.to_string();
    if index_object_path.is_empty() {
        return Err(Error::message("WARC index must not be a storage root"));
    }
    let index_bytes = index_storage
        .operator
        .read(&index_object_path)
        .await?
        .to_bytes();
    let index_gzipped = index_object_path.to_ascii_lowercase().ends_with(".gz");
    let source_path = path.clone();
    let entries = tokio::task::spawn_blocking(move || {
        parse_warc_index(index_bytes, index_gzipped, &source_path)
    })
    .await
    .map_err(|error| Error::message(format!("WARC index parser task failed: {error}")))??;
    let work_units = build_index_work_units(entries, max_read_parallelism)?;
    let parallelism = work_units.len().min(max_read_parallelism).max(1);
    let stream_schema = schema.clone();
    let batches = stream::iter(work_units)
        .map(move |range| {
            let operator = operator.clone();
            let path = path.clone();
            let projection = projection.clone();
            let schema = stream_schema.clone();
            async move {
                let bytes = operator.read_with(&path).range(range).await?.to_bytes();
                tokio::task::spawn_blocking(move || {
                    parse_indexed_warc_bytes(bytes, gzipped, batch_size, projection, schema)
                })
                .await
                .map_err(|error| {
                    Error::message(format!("indexed WARC parser task failed: {error}"))
                })?
            }
        })
        .buffered(parallelism)
        .map_ok(|batches| stream::iter(batches.into_iter().map(Ok::<_, Error>)))
        .try_flatten();
    Ok((schema, Box::pin(batches)))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use bytes::Bytes;
    use flate2::{write::GzEncoder, Compression};

    use super::*;

    #[test]
    fn balances_contiguous_entries_across_parallelism() {
        let entries = vec![
            WarcIndexEntry {
                offset: 30,
                length: 10,
            },
            WarcIndexEntry {
                offset: 0,
                length: 10,
            },
            WarcIndexEntry {
                offset: 20,
                length: 10,
            },
            WarcIndexEntry {
                offset: 10,
                length: 10,
            },
        ];

        assert_eq!(build_index_work_units(entries, 2).unwrap(), [0..20, 20..40]);
    }

    #[test]
    fn gaps_start_new_workloads() {
        let entries = vec![
            WarcIndexEntry {
                offset: 20,
                length: 5,
            },
            WarcIndexEntry {
                offset: 0,
                length: 8,
            },
            WarcIndexEntry {
                offset: 8,
                length: 4,
            },
        ];

        assert_eq!(build_index_work_units(entries, 2).unwrap(), [0..12, 20..25]);
    }

    #[test]
    fn parses_json_and_cdxj_entries_for_the_source_file() {
        let index = concat!(
            "# comment\n",
            "{\"filename\":\"archive.warc.gz\",\"offset\":10,\"length\":\"20\"}\n",
            "com,example)/ 20240102030405 ",
            "{\"filename\":\"other.warc.gz\",\"offset\":\"30\",\"length\":40}\n",
            "com,example)/ 20240102030406 ",
            "{\"filename\":\"archive.warc.gz\",\"offset\":\"50\",\"length\":60}\n",
        );

        let entries = parse_warc_index(
            Bytes::from_static(index.as_bytes()),
            false,
            "crawl-data/archive.warc.gz",
        )
        .unwrap();

        assert_eq!(
            entries,
            [
                WarcIndexEntry {
                    offset: 10,
                    length: 20
                },
                WarcIndexEntry {
                    offset: 50,
                    length: 60
                }
            ]
        );
    }

    #[test]
    fn parses_gzipped_index() {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(b"{\"offset\":\"7\",\"length\":\"11\"}\n")
            .unwrap();

        let entries = parse_warc_index(
            Bytes::from(encoder.finish().unwrap()),
            true,
            "archive.warc.gz",
        )
        .unwrap();

        assert_eq!(
            entries,
            [WarcIndexEntry {
                offset: 7,
                length: 11
            }]
        );
    }
}
