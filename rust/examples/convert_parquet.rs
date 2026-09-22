use anyhow::{ensure, Result};
use lance_conversion::{convert, ParquetFileSource, WriteOptions};

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    ensure!(
        args.len() == 2,
        "usage: convert_parquet INPUT.parquet OUTPUT.lance"
    );
    let result = convert(
        ParquetFileSource::new(&args[0]),
        &args[1],
        WriteOptions::default(),
    )?;
    println!("{} rows written", result.rows_written);
    Ok(())
}
