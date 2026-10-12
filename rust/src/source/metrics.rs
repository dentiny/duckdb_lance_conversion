use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use futures::io::AsyncBufRead;
use futures::TryStreamExt;
use opendal::Operator;

use crate::Result;

#[derive(Debug, Default)]
pub(crate) struct ReadMetrics {
    bytes_read: AtomicU64,
    total_bytes: AtomicU64,
}

impl ReadMetrics {
    pub(crate) fn add_bytes_read(&self, bytes: usize) {
        self.bytes_read
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
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

/// Streams a whole object sequentially, recording its size and the bytes
/// received so far.
pub(crate) async fn read_object(
    operator: &Operator,
    path: &str,
    metrics: Arc<ReadMetrics>,
) -> Result<impl AsyncBufRead + Unpin + Send + 'static> {
    metrics.set_total_bytes(operator.stat(path).await?.content_length());
    Ok(operator
        .reader(path)
        .await?
        .into_bytes_stream(..)
        .await?
        .inspect_ok(move |bytes| metrics.add_bytes_read(bytes.len()))
        .into_async_read())
}
