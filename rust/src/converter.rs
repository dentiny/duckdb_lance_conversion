use anyhow::Result;

use crate::{BatchSink, BatchSource, WriteSummary};

pub async fn convert(source: impl BatchSource, sink: impl BatchSink) -> Result<WriteSummary> {
    let (schema, batches) = source.open().await?;
    sink.write(schema, batches).await
}
