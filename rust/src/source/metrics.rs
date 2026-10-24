use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, PoisonError,
};

use futures::io::AsyncBufRead;
use futures::TryStreamExt;
use opendal::Operator;

use crate::Result;

#[derive(Debug, Default)]
pub(crate) struct ReadMetrics {
    bytes_read: AtomicU64,
    total_bytes: AtomicU64,
    /// Count and total size of the objects opened so far by `add_object`.
    opened: Mutex<(u64, u64)>,
}

impl ReadMetrics {
    /// Records an opened object of `size` bytes out of `objects`, estimating
    /// the total as their mean size so far times `objects`. The estimate is
    /// exact once every object is opened.
    pub(crate) fn add_object(&self, size: u64, objects: u64) {
        let mut opened = self.opened.lock().unwrap_or_else(PoisonError::into_inner);
        opened.0 += 1;
        opened.1 = opened.1.saturating_add(size);
        let total = u128::from(opened.1) * u128::from(objects) / u128::from(opened.0);
        self.set_total_bytes(u64::try_from(total).unwrap_or(u64::MAX));
    }

    pub(crate) fn add_bytes_read(&self, bytes: usize) {
        self.bytes_read
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
    }

    pub(crate) fn add_total_bytes(&self, bytes: u64) {
        let _ = self
            .total_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |total| {
                Some(total.saturating_add(bytes))
            });
    }

    pub(crate) fn set_total_bytes(&self, bytes: u64) {
        self.total_bytes.store(bytes, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> ReadMetricsSnapshot {
        ReadMetricsSnapshot {
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReadMetricsSnapshot {
    pub bytes_read: u64,
    pub total_bytes: u64,
}

/// Streams a whole object of known `size` sequentially, recording the bytes
/// received so far. Callers set the expected total.
pub(crate) async fn read_object(
    operator: &Operator,
    path: &str,
    size: u64,
    metrics: Arc<ReadMetrics>,
) -> Result<impl AsyncBufRead + Unpin + Send + 'static> {
    // Without the hint, an open-ended range stats the object again.
    Ok(operator
        .reader_with(path)
        .content_length_hint(size)
        .await?
        .into_bytes_stream(..)
        .await?
        .inspect_ok(move |bytes| metrics.add_bytes_read(bytes.len()))
        .into_async_read())
}
