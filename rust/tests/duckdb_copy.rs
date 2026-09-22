#[allow(dead_code)]
mod common;

use std::path::Path;
use std::process::Output;
use tokio::process::Command;
use tokio::task::spawn_blocking;

use arrow_array::{Array, ListArray, RecordBatch, StructArray};
use arrow_select::concat::concat_batches;
use lance_conversion::{BatchSource, ParquetFileSource};
use tempfile::tempdir;

fn literal(path: &Path) -> String {
    format!("'{}'", path.to_str().unwrap().replace('\'', "''"))
}

async fn sql(sql: &str) -> Output {
    let binary = std::env::var("DUCKDB_BINARY").expect("set DUCKDB_BINARY to the v1.5.4 shell");
    let extension =
        std::env::var("LANCE_EXTENSION").expect("set LANCE_EXTENSION to the compiled extension");
    Command::new(binary)
        .kill_on_drop(true)
        .args([
            "-unsigned",
            "-batch",
            "-csv",
            "-noheader",
            "-c",
            &format!(
                "LOAD {}; SET threads=4; {sql}",
                literal(Path::new(&extension))
            ),
        ])
        .output()
        .await
        .unwrap()
}

async fn ok(sql_text: &str) -> String {
    let output = sql(sql_text).await;
    assert!(
        output.status.success(),
        "SQL: {sql_text}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn assert_array_values(actual: &dyn Array, expected: &dyn Array) {
    assert_eq!(actual.len(), expected.len());
    assert_eq!(actual.nulls(), expected.nulls());
    if let (Some(a), Some(b)) = (
        actual.as_any().downcast_ref::<ListArray>(),
        expected.as_any().downcast_ref::<ListArray>(),
    ) {
        for row in 0..a.len() {
            if a.is_valid(row) {
                assert_array_values(a.value(row).as_ref(), b.value(row).as_ref());
            }
        }
    } else if let (Some(a), Some(b)) = (
        actual.as_any().downcast_ref::<StructArray>(),
        expected.as_any().downcast_ref::<StructArray>(),
    ) {
        for col in 0..a.num_columns() {
            for row in 0..a.len() {
                if a.is_valid(row) {
                    assert_array_values(
                        a.column(col).slice(row, 1).as_ref(),
                        b.column(col).slice(row, 1).as_ref(),
                    );
                }
            }
        }
    } else {
        assert_eq!(actual.to_data(), expected.to_data());
    }
}

#[tokio::test]
#[ignore = "requires built DuckDB and extension; see README"]
async fn parquet_copy_roundtrip_and_failures() {
    let temp = tempdir().unwrap();
    let input = temp.path().join("input.parquet");
    let output = temp.path().join("output.lance");
    ok(&format!(
        r#"
        COPY (
            SELECT i::BIGINT AS id,
                (i % 2 = 0)::BOOLEAN AS flag,
                (i % 100)::TINYINT AS tiny,
                (i % 1000)::SMALLINT AS small,
                i::INTEGER AS integer_value,
                (i % 100)::UTINYINT AS utiny,
                (i % 1000)::USMALLINT AS usmall,
                i::UINTEGER AS uinteger,
                (18446744073709500000::UBIGINT + i::UBIGINT) AS unsigned_value,
                (i * 0.5)::FLOAT AS float_value,
                CASE WHEN i % 17 = 0 THEN NULL ELSE i * 0.25 END::DOUBLE AS double_value,
                (i * 1.25)::DECIMAL(18, 2) AS decimal_value,
                DATE '2024-01-01' + i::INTEGER AS date_value,
                TIMESTAMP '2024-01-01 00:00:00' + i * INTERVAL '1 microsecond' AS timestamp_value,
                CASE WHEN i % 7 = 0 THEN NULL ELSE '你好-' || i::VARCHAR END AS text_value,
                CASE WHEN i % 5 = 0 THEN NULL ELSE from_hex('00FF80') END AS binary_value,
                CASE WHEN i % 11 = 0 THEN NULL ELSE [i::INTEGER, NULL, (i+1)::INTEGER] END AS list_value,
                CASE WHEN i % 13 = 0 THEN NULL ELSE {{'a': i, 'b': CASE WHEN i % 3 = 0 THEN NULL ELSE 'x' END}} END AS struct_value
            FROM range(10001) t(i)
        ) TO {} (FORMAT PARQUET);
        COPY (SELECT * FROM read_parquet({})) TO {} (FORMAT LANCE);
    "#,
        literal(&input),
        literal(&input),
        literal(&output)
    )).await;
    let expected_input = input.clone();
    let expected = spawn_blocking(move || {
        let reader = ParquetFileSource::new(&expected_input).open().unwrap();
        let schema = reader.schema();
        let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
        concat_batches(&schema, &batches).unwrap()
    })
    .await
    .unwrap();
    let actual = common::read_lance(&output).await;
    assert_eq!(actual.num_rows(), 10001);
    assert_eq!(actual.num_columns(), expected.num_columns());
    for col in 0..expected.num_columns() {
        assert_eq!(
            actual.schema().field(col).name(),
            expected.schema().field(col).name()
        );
        assert_array_values(actual.column(col).as_ref(), expected.column(col).as_ref());
    }
    let existing = sql(&format!(
        "COPY (SELECT 1 AS id) TO {} (FORMAT LANCE);",
        literal(&output)
    ))
    .await;
    assert!(!existing.status.success());
    assert_eq!(common::read_lance(&output).await.num_rows(), 10001);

    let empty = temp.path().join("empty.lance");
    ok(&format!(
        "COPY (SELECT * FROM read_parquet({}) WHERE false) TO {} (FORMAT LANCE);",
        literal(&input),
        literal(&empty)
    ))
    .await;
    let empty_batch = common::read_lance(&empty).await;
    assert_eq!(empty_batch.num_rows(), 0);
    assert_eq!(empty_batch.num_columns(), expected.num_columns());

    let failed = temp.path().join("failed.lance");
    let result = sql(&format!("COPY (SELECT CASE WHEN i > 5000 THEN error('upstream failed') ELSE i END AS id FROM range(10001) t(i)) TO {} (FORMAT LANCE);", literal(&failed))).await;
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("upstream failed"));
    assert!(!tokio::fs::try_exists(&failed).await.unwrap());

    for option in [
        "APPEND",
        "PARTITION_BY (id)",
        "PER_THREAD_OUTPUT",
        "USE_TMP_FILE",
        "COMPRESSION 'zstd'",
    ] {
        let unsupported = temp.path().join("unsupported.lance");
        let result = sql(&format!(
            "COPY (SELECT 1 AS id) TO {} (FORMAT LANCE, {option});",
            literal(&unsupported)
        ))
        .await;
        assert!(
            !result.status.success(),
            "accepted unsupported option {option}"
        );
        assert!(!tokio::fs::try_exists(&unsupported).await.unwrap());
    }
    ok(&format!(
        "COPY (SELECT * FROM read_parquet({}) WHERE id < 4) TO {} (FORMAT LANCE, OVERWRITE);",
        literal(&input),
        literal(&output)
    ))
    .await;
    let replacement = common::read_lance(&output).await;
    assert_eq!(replacement.num_rows(), 4);
    for col in 0..expected.num_columns() {
        assert_array_values(
            replacement.column(col).as_ref(),
            expected.column(col).slice(0, 4).as_ref(),
        );
    }
    let rejected = sql(&format!(
        "COPY (SELECT 1 AS id) TO {} (FORMAT LANCE, OVERWRITE false);",
        literal(&output)
    ))
    .await;
    assert!(!rejected.status.success());
    let aborted = sql(&format!("COPY (SELECT CASE WHEN i > 5000 THEN error('upstream failed') ELSE i END AS id FROM range(10001) t(i)) TO {} (FORMAT LANCE, OVERWRITE);", literal(&output))).await;
    assert!(!aborted.status.success());
    common::assert_values(&common::read_lance(&output).await, &replacement);

    let unsupported = temp.path().join("bad-type.lance");
    let result = sql(&format!("COPY (SELECT 170141183460469231731687303715884105727::HUGEINT AS id) TO {} (FORMAT LANCE);", literal(&unsupported))).await;
    assert!(!result.status.success());
    assert!(!tokio::fs::try_exists(&unsupported).await.unwrap());
}
