mod lance;
pub use lance::{LanceDatasetWriter, LanceFragmentWriter, LanceWriter, WriteMode, WriteOptions};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteSummary {
    pub rows_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}
