//! Materialize media at its native ATIF field position before writing Lance.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{
    builder::LargeBinaryBuilder, make_array, Array, ArrayRef, RecordBatch, StringArray,
};
use arrow_schema::{DataType, FieldRef, Schema, SchemaRef};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures::{future::BoxFuture, AsyncReadExt};
use url::Url;

use crate::error::ResultExt;
use crate::source::{read_object, ReadMetrics};
use crate::storage::InputFile;
use crate::{Error, OpendalConfig, Result, S3StorageConfig};

/// The extension materializes MediaSource.path in-place; ContentPart.text and
/// every other native ATIF field keep their types and positions.
pub(super) fn embedded_schema() -> SchemaRef {
    fn embedded_field(field: &FieldRef) -> FieldRef {
        let data_type = match field.data_type() {
            // In atif-arrow's fixed schema, MediaSource has these three fields in
            // this order. Match the full shape to replace only its path reference
            // with payload bytes, preserving the MIME type and duration fields.
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

fn decode_data_uri(uri: &str) -> Result<Vec<u8>> {
    let (header, encoded) = uri
        .split_once(',')
        .ok_or_else(|| Error::message("invalid ATIF media data URI"))?;
    let encoded = percent_encoding::percent_decode_str(encoded).collect::<Vec<_>>();
    if header.ends_with(";base64") {
        STANDARD
            .decode(encoded)
            .map_err(|error| Error::message(format!("invalid ATIF media base64: {error}")))
    } else {
        Ok(encoded)
    }
}

#[derive(Clone)]
pub(super) struct MediaResolver {
    pub document: String,
    pub s3_config: Option<S3StorageConfig>,
    pub opendal_config: OpendalConfig,
    pub metrics: Arc<ReadMetrics>,
}

impl MediaResolver {
    async fn read(&self, uri: &str) -> Result<Vec<u8>> {
        if uri.starts_with("data:") {
            return decode_data_uri(uri);
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
        self.metrics.add_total_bytes(size);
        let mut reader = read_object(
            &input.operator,
            &input.opendal_path,
            size,
            self.metrics.clone(),
        )
        .await?;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await?;
        if bytes.len() as u64 != size {
            return Err(Error::message("ATIF media size changed during read"));
        }
        Ok(bytes)
    }

    /// Materialize message/observation media. Projections excluding steps skip this resolver.
    pub async fn resolve(&self, batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (array, field) in batch.columns().iter().zip(schema.fields()) {
            columns.push(self.resolve_array(array, field).await?);
        }
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn resolve_array<'a>(
        &'a self,
        array: &'a ArrayRef,
        target: &'a FieldRef,
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
                            .read(&uri)
                            .await
                            .context(format!("ATIF media {path}"))?;
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
                transformed.push(self.resolve_array(&child, field).await?.to_data());
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
    fn decodes_data_uris() {
        assert_eq!(
            decode_data_uri("data:image/png;base64,AP8=").unwrap(),
            [0, 255]
        );
        assert_eq!(decode_data_uri("data:image/png,%00%ff").unwrap(), [0, 255]);
        assert_eq!(
            decode_data_uri("data:image/png;base64,AP8%3D").unwrap(),
            [0, 255]
        );
        assert!(decode_data_uri("data:image/png").is_err());
        assert!(decode_data_uri("data:image/png;base64,????").is_err());
    }
}
