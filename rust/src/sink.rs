mod lance;
pub use lance::{
    ColumnEncoding, IndexKind, LanceDatasetWriter, LanceFragmentWriter, LanceWriter,
    StructuralEncoding, WriteMode, WriteOptions,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteSummary {
    pub rows_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}
