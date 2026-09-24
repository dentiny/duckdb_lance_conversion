use std::pin::Pin;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

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

pub(crate) struct MetricsReader<R> {
    inner: R,
    metrics: Arc<ReadMetrics>,
}

impl<R> MetricsReader<R> {
    pub(crate) fn new(inner: R, metrics: Arc<ReadMetrics>) -> Self {
        Self { inner, metrics }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for MetricsReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let filled = buffer.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(context, buffer);
        if let Poll::Ready(Ok(())) = &result {
            self.metrics
                .add_bytes_read(buffer.filled().len().saturating_sub(filled));
        }
        result
    }
}
