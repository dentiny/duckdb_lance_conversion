//! Materialize media at its native ATIF field position before writing Lance.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    builder::LargeBinaryBuilder, make_array, Array, ArrayRef, RecordBatch, StringArray,
};
use arrow_schema::{DataType, FieldRef, Schema, SchemaRef};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures::{future::BoxFuture, TryStreamExt};
use url::Url;

use crate::error::ResultExt;
use crate::source::ReadMetrics;
use crate::storage::InputFile;
use crate::{Error, OpendalConfig, Result, S3StorageConfig};

#[derive(Clone, Copy, Debug)]
pub struct AtifMediaOptions {
    /// Resolve media by default. False keeps the native string reference schema.
    pub resolve_media: bool,
    pub max_media_bytes: u64,
    pub max_trajectory_media_bytes: u64,
}

impl Default for AtifMediaOptions {
    fn default() -> Self {
        Self {
            resolve_media: true,
            max_media_bytes: 256 * 1024 * 1024,
            max_trajectory_media_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

impl AtifMediaOptions {
    pub(super) fn validate(self) -> Result<Self> {
        if self.max_media_bytes == 0 || self.max_trajectory_media_bytes == 0 {
            return Err(Error::message("ATIF media byte limits must be positive"));
        }
        Ok(self)
    }
}

/// The extension materializes MediaSource.path in-place; ContentPart.text and
/// every other native ATIF field keep their types and positions.
pub(super) fn embedded_schema() -> SchemaRef {
    fn embedded_field(field: &FieldRef) -> FieldRef {
        let data_type = match field.data_type() {
            DataType::Struct(fields)
                if fields.len() == 3
                    && fields[0].name() == "media_type"
                    && fields[1].name() == "path"
                    && fields[2].name() == "duration_sec" =>
            {
                let mut fields = fields.to_vec();
                fields[1] = Arc::new(
                    fields[1]
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::LargeBinary),
                );
                DataType::Struct(fields.into())
            }
            DataType::Struct(fields) => {
                DataType::Struct(fields.iter().map(embedded_field).collect())
            }
            DataType::List(item) => DataType::List(embedded_field(item)),
            _ => return field.clone(),
        };
        Arc::new(field.as_ref().clone().with_data_type(data_type))
    }
    let native = atif_arrow::trajectory_schema();
    Arc::new(Schema::new_with_metadata(
        native
            .fields()
            .iter()
            .map(embedded_field)
            .collect::<Vec<_>>(),
        native.metadata().clone(),
    ))
}

/// Remote documents cannot cause local-file reads or forward S3 credentials to
/// another bucket. Relative remote paths use URL resolution, not host paths.
fn resolve_uri(document: &str, reference: &str) -> Result<String> {
    if reference.starts_with("data:") {
        return Ok(reference.to_owned());
    }
    if let Ok(base) = Url::parse(document) {
        if matches!(base.scheme(), "s3" | "http" | "https") {
            let target = base.join(reference)?;
            if target.scheme() != base.scheme()
                || target.host_str() != base.host_str()
                || target.port_or_known_default() != base.port_or_known_default()
                || !target.username().is_empty()
                || target.password().is_some()
            {
                return Err(Error::message(
                    "ATIF remote media must use the trajectory's storage origin",
                ));
            }
            return Ok(target.to_string());
        }
    }
    if Path::new(reference).is_absolute() {
        return Ok(reference.to_owned());
    }
    if let Ok(url) = Url::parse(reference) {
        if url.scheme() == "file" {
            return url
                .to_file_path()
                .map(|path| path.to_string_lossy().into_owned())
                .map_err(|_| Error::message("invalid ATIF media file URI"));
        }
        if matches!(url.scheme(), "s3" | "http" | "https") {
            return Ok(url.to_string());
        }
        return Err(Error::message(format!(
            "unsupported ATIF media scheme: {}",
            url.scheme()
        )));
    }
    Ok(Path::new(document)
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(reference)
        .to_string_lossy()
        .into_owned())
}

fn limit_error() -> Error {
    Error::message("ATIF media exceeds max_media_bytes or max_trajectory_media_bytes")
}

fn decode_data_uri(uri: &str, limit: u64) -> Result<Vec<u8>> {
    let (header, encoded) = uri
        .split_once(',')
        .ok_or_else(|| Error::message("invalid ATIF media data URI"))?;
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    let base64 = header.ends_with(";base64");
    let encoded_limit = if base64 {
        limit.div_ceil(3).saturating_mul(4)
    } else {
        limit
    };
    let encoded = percent_encoding::percent_decode_str(encoded)
        .take(encoded_limit.saturating_add(1))
        .collect::<Vec<_>>();
    if encoded.len() > encoded_limit {
        return Err(limit_error());
    }
    let bytes = if base64 {
        STANDARD
            .decode(encoded)
            .map_err(|error| Error::message(format!("invalid ATIF media base64: {error}")))?
    } else {
        encoded
    };
    if bytes.len() > limit {
        return Err(limit_error());
    }
    Ok(bytes)
}

#[derive(Clone)]
pub(super) struct MediaResolver {
    pub document: String,
    pub options: AtifMediaOptions,
    pub s3_config: Option<S3StorageConfig>,
    pub opendal_config: OpendalConfig,
    pub metrics: Arc<ReadMetrics>,
}

impl MediaResolver {
    async fn read(&self, uri: &str, remaining: u64) -> Result<Vec<u8>> {
        let limit = self.options.max_media_bytes.min(remaining);
        if uri.starts_with("data:") {
            return decode_data_uri(uri, limit);
        }
        let input = InputFile::open(
            uri,
            self.s3_config.as_ref().filter(|_| uri.starts_with("s3://")),
            &self.opendal_config,
        )?;
        let size = input
            .operator
            .stat(&input.opendal_path)
            .await?
            .content_length();
        if size > limit || usize::try_from(size).is_err() {
            return Err(limit_error());
        }
        self.metrics.add_total_bytes(size);
        let mut stream = input
            .operator
            .reader_with(&input.opendal_path)
            .content_length_hint(size)
            .await?
            .into_bytes_stream(..)
            .await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            self.metrics.add_bytes_read(chunk.len());
            if (bytes.len() as u64).saturating_add(chunk.len() as u64) > limit {
                return Err(limit_error());
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() as u64 != size {
            return Err(Error::message("ATIF media size changed during read"));
        }
        Ok(bytes)
    }

    /// Media batches contain one trajectory, so one budget covers all its messages
    /// and observations. Projection that excludes steps never calls this resolver.
    pub async fn resolve(&self, batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
        let mut remaining = self.options.max_trajectory_media_bytes;
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (array, field) in batch.columns().iter().zip(schema.fields()) {
            columns.push(self.resolve_array(array, field, &mut remaining).await?);
        }
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn resolve_array<'a>(
        &'a self,
        array: &'a ArrayRef,
        target: &'a FieldRef,
        remaining: &'a mut u64,
    ) -> BoxFuture<'a, Result<ArrayRef>> {
        Box::pin(async move {
            if array.data_type() == target.data_type() {
                return Ok(array.clone());
            }
            if array.data_type() == &DataType::Utf8 && target.data_type() == &DataType::LargeBinary
            {
                let paths = array.as_any().downcast_ref::<StringArray>().unwrap();
                let mut builder = LargeBinaryBuilder::new();
                for path in paths.iter() {
                    if let Some(path) = path {
                        let uri = resolve_uri(&self.document, path).context("ATIF media URI")?;
                        let bytes = self
                            .read(&uri, *remaining)
                            .await
                            .context(format!("ATIF media {path}"))?;
                        *remaining -= bytes.len() as u64;
                        builder.append_value(bytes);
                    } else {
                        builder.append_null();
                    }
                }
                return Ok(Arc::new(builder.finish()) as ArrayRef);
            }
            let children: Vec<FieldRef> = match target.data_type() {
                DataType::Struct(fields) => fields.to_vec(),
                DataType::List(item) => vec![item.clone()],
                _ => return Err(Error::message("unexpected ATIF media field type")),
            };
            let data = array.to_data();
            let mut transformed = Vec::with_capacity(children.len());
            for (child, field) in data.child_data().iter().zip(&children) {
                let child = make_array(child.clone());
                transformed.push(
                    self.resolve_array(&child, field, remaining)
                        .await?
                        .to_data(),
                );
            }
            Ok(make_array(
                data.into_builder()
                    .data_type(target.data_type().clone())
                    .child_data(transformed)
                    .build()?,
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_media_relative_to_the_document() {
        assert_eq!(
            resolve_uri("/runs/a/trajectory.json", "images/a.png").unwrap(),
            Path::new("/runs/a").join("images/a.png").to_string_lossy()
        );
        assert_eq!(
            resolve_uri("https://example.com/run/t.json", "../image.png").unwrap(),
            "https://example.com/image.png"
        );
        assert_eq!(
            resolve_uri("s3://bucket/run/t.json.gz", "a.png").unwrap(),
            "s3://bucket/run/a.png"
        );
        for path in [
            "file:///etc/passwd",
            "s3://other/image.png",
            "https://other/image.png",
        ] {
            assert!(resolve_uri("s3://bucket/run/t.json", path).is_err());
        }
    }

    #[test]
    fn s3_relative_media_keeps_the_decoded_object_key() {
        let uri = resolve_uri("s3://bucket/run/t.json", "image 1.png").unwrap();
        let input = InputFile::open(
            &uri,
            Some(&S3StorageConfig::default()),
            &OpendalConfig::default(),
        )
        .unwrap();
        assert_eq!(input.opendal_path, "run/image 1.png");
    }

    #[test]
    fn decodes_data_uris_with_byte_limits() {
        assert_eq!(
            decode_data_uri("data:image/png;base64,AP8=", 2).unwrap(),
            [0, 255]
        );
        assert_eq!(
            decode_data_uri("data:image/png,%00%ff", 2).unwrap(),
            [0, 255]
        );
        assert_eq!(
            decode_data_uri("data:image/png;base64,AP8%3D", 2).unwrap(),
            [0, 255]
        );
        assert!(decode_data_uri("data:image/png;base64,AP8=", 1).is_err());
        assert!(decode_data_uri("data:image/png,%00%ff", 1).is_err());
        assert!(decode_data_uri("data:image/png;base64,????", 3).is_err());
    }
}
