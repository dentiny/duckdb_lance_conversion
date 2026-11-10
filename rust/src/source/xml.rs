use std::sync::Arc;

use arrow_schema::{Fields, SchemaRef};
use futures::TryStreamExt;
use xml_arrow::{InferenceOptions, NamespaceBinding, ReaderOptions, XmlReader};

use super::{decompressed_input, is_gzipped, read_object, BatchSource, BatchStream, ReadMetrics};
use crate::storage::InputFile;
use crate::{Error, OpendalConfig, Result, S3StorageConfig};

/// Reads one row per element at an absolute record path such as
/// `/catalog/book`, with a schema inferred from the leading records.
pub struct XmlSource {
    path: String,
    record_path: String,
    namespaces: Vec<(String, String)>,
    inference: InferenceOptions,
    expected_schema: Option<SchemaRef>,
    s3_config: Option<S3StorageConfig>,
    opendal_config: OpendalConfig,
    projection: Option<Vec<usize>>,
    metrics: Arc<ReadMetrics>,
}

impl XmlSource {
    pub fn new(path: impl Into<String>, record_path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            record_path: record_path.into(),
            namespaces: Vec::new(),
            inference: InferenceOptions::default(),
            expected_schema: None,
            s3_config: None,
            opendal_config: OpendalConfig::default(),
            projection: None,
            metrics: Arc::new(ReadMetrics::default()),
        }
    }

    /// `(prefix, uri)` pairs for prefixes in the record path. Inferred
    /// columns use these prefixes, such as `atom:title`; other namespaces
    /// get generated prefixes such as `ns0`.
    pub fn with_namespaces(mut self, namespaces: Vec<(String, String)>) -> Self {
        self.namespaces = namespaces;
        self
    }

    /// Records sampled for schema inference. Fields that first appear after
    /// the sample are not read.
    pub fn with_sample_records(mut self, records: usize) -> Self {
        self.inference.max_records = records;
        self
    }

    /// Serialized XML bytes sampled for schema inference.
    pub fn with_sample_bytes(mut self, bytes: usize) -> Self {
        self.inference.max_bytes = bytes;
        self
    }

    /// The full schema a previous open inferred. Each open infers again, so
    /// a file changed since then fails instead of shifting projected columns.
    pub fn with_expected_schema(mut self, schema: SchemaRef) -> Self {
        self.expected_schema = Some(schema);
        self
    }

    pub fn with_s3_config(mut self, config: S3StorageConfig) -> Self {
        self.s3_config = Some(config);
        self
    }

    pub fn with_opendal_config(mut self, config: OpendalConfig) -> Self {
        self.opendal_config = config;
        self
    }

    /// Top-level column indices in output order; empty reads every column.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<ReadMetrics>) -> Self {
        self.metrics = metrics;
        self
    }
}

impl BatchSource for XmlSource {
    async fn open(self) -> Result<(SchemaRef, BatchStream)> {
        let input = InputFile::open(&self.path, self.s3_config.as_ref(), &self.opendal_config)?;
        let size = input
            .operator
            .stat(&input.opendal_path)
            .await?
            .content_length();
        self.metrics.set_total_bytes(size);
        let reader = read_object(&input.operator, &input.opendal_path, size, self.metrics).await?;
        let reader = decompressed_input(reader, is_gzipped(&input.opendal_path));
        let projection = self.projection.filter(|projection| !projection.is_empty());
        let expected_fields = match (&self.expected_schema, &projection) {
            (Some(expected), Some(projection)) => Some(expected.project(projection)?.fields),
            (Some(expected), None) => Some(expected.fields().clone()),
            (None, _) => None,
        };
        let reads_every_column = projection.is_none();
        let options = ReaderOptions {
            projection,
            ..Default::default()
        };
        let bindings = self
            .namespaces
            .iter()
            .map(|(prefix, uri)| NamespaceBinding { prefix, uri })
            .collect::<Vec<_>>();
        let reader = XmlReader::infer(
            reader,
            &self.record_path,
            &bindings,
            options,
            self.inference,
        )
        .await
        .map_err(|error| Error::message(format!("XML reader for '{}': {error}", input.uri)))?;
        let schema = reader.schema();
        if reads_every_column && schema.fields().is_empty() {
            return Err(Error::message(format!(
                "XML record path '{}' matched no fields in '{}'",
                self.record_path, input.uri
            )));
        }
        if let Some(expected) = expected_fields {
            if !same_columns(&expected, schema.fields()) {
                return Err(Error::message(format!(
                    "XML schema of '{}' changed since the query was bound: expected {}, found {}",
                    input.uri,
                    column_names(&expected),
                    column_names(schema.fields())
                )));
            }
        }
        let batches = reader
            .into_stream()
            .map_err(|error| Error::message(format!("XML read error: {error}")));
        Ok((schema, Box::pin(batches)))
    }
}

fn same_columns(expected: &Fields, actual: &Fields) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual.iter())
            .all(|(expected, actual)| {
                expected.name() == actual.name() && expected.data_type() == actual.data_type()
            })
}

fn column_names(fields: &Fields) -> String {
    let names = fields
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    format!("[{}]", names.join(", "))
}

#[cfg(test)]
mod tests {
    use arrow_array::{cast::AsArray, types::Float64Type, types::Int64Type, RecordBatch};
    use arrow_schema::DataType;

    use super::*;

    const STATIONS: &str = "/wx_station_index/station";

    fn fixture(name: &str) -> String {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap();
        root.join("test/data")
            .join(name)
            .to_string_lossy()
            .into_owned()
    }

    async fn read_all(source: XmlSource) -> Result<(SchemaRef, RecordBatch)> {
        let (schema, stream) = source.open().await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok((
            schema.clone(),
            arrow_select::concat::concat_batches(&schema, &batches)?,
        ))
    }

    fn strings(batch: &RecordBatch, name: &str) -> Vec<String> {
        let column = batch.column_by_name(name).unwrap().as_string::<i32>();
        column
            .iter()
            .map(|value| value.unwrap().to_owned())
            .collect()
    }

    #[tokio::test]
    async fn reads_noaa_stations_plain_and_gzipped() {
        let metrics = Arc::new(ReadMetrics::default());
        let source =
            XmlSource::new(fixture("noaa_stations_al.xml"), STATIONS).with_metrics(metrics.clone());
        let (schema, batch) = read_all(source).await.unwrap();
        let fields = schema
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.data_type().clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            fields,
            [
                ("html_url", DataType::Utf8),
                ("latitude", DataType::Float64),
                ("longitude", DataType::Float64),
                ("rss_url", DataType::Utf8),
                ("state", DataType::Utf8),
                ("station_id", DataType::Utf8),
                ("station_name", DataType::Utf8),
                ("xml_url", DataType::Utf8),
            ]
        );
        assert_eq!(batch.num_rows(), 46);
        let ids = strings(&batch, "station_id");
        assert_eq!((ids[0].as_str(), ids[45].as_str()), ("KSXS", "K1A9"));
        let jasper = ids.iter().position(|id| id == "KJFX").unwrap();
        assert_eq!(
            strings(&batch, "station_name")[jasper],
            "Jasper Walker County Airport & Bevill Field"
        );
        let latitude = batch.column_by_name("latitude").unwrap();
        assert_eq!(latitude.as_primitive::<Float64Type>().value(0), 31.35);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.bytes_read, snapshot.total_bytes);

        let gzipped = XmlSource::new(fixture("noaa_stations_al.xml.gz"), STATIONS);
        assert_eq!(read_all(gzipped).await.unwrap().1, batch);
    }

    #[tokio::test]
    async fn reads_namespaced_usgs_atom_entries() {
        let source = XmlSource::new(
            fixture("usgs_significant_month.atom"),
            "/atom:feed/atom:entry",
        )
        .with_namespaces(vec![
            ("atom".into(), "http://www.w3.org/2005/Atom".into()),
            ("georss".into(), "http://www.georss.org/georss".into()),
        ]);
        let (schema, batch) = read_all(source).await.unwrap();
        assert_eq!(batch.num_rows(), 11);
        assert_eq!(
            strings(&batch, "atom:title")[10],
            "M 6.1 - 15 km W of Río Grande, Panama"
        );
        assert_eq!(
            schema.field_with_name("georss:elev").unwrap().data_type(),
            &DataType::Int64
        );
        let elev = batch.column_by_name("georss:elev").unwrap();
        assert_eq!(elev.as_primitive::<Int64Type>().value(2), -492376);

        let link = batch.column_by_name("atom:link").unwrap().as_struct();
        let href = link.column_by_name("@href").unwrap().as_string::<i32>();
        assert_eq!(
            href.value(0),
            "https://earthquake.usgs.gov/earthquakes/eventpage/us7000tx16"
        );
        let categories = batch
            .column_by_name("atom:category")
            .unwrap()
            .as_list::<i32>();
        assert_eq!(categories.value_length(0), 4);
        let first = categories.value(0);
        let terms = first.as_struct().column_by_name("@term").unwrap();
        assert_eq!(terms.as_string::<i32>().value(0), "Past Day");
    }

    #[tokio::test]
    async fn projects_by_bound_schema_and_detects_changes() {
        let path = fixture("noaa_stations_al.xml");
        let (bound, _) = read_all(XmlSource::new(&path, STATIONS)).await.unwrap();
        let projection = vec![
            bound.index_of("station_name").unwrap(),
            bound.index_of("station_id").unwrap(),
        ];
        let (schema, batch) = read_all(
            XmlSource::new(&path, STATIONS)
                .with_projection(projection.clone())
                .with_expected_schema(bound.clone()),
        )
        .await
        .unwrap();
        assert_eq!(schema.field(0).name(), "station_name");
        assert_eq!(batch.num_columns(), 2);

        // A schema bound from different contents must not be projected by index.
        let stale = Arc::new(bound.project(&[0, 1, 2, 3, 4, 6, 5, 7]).unwrap());
        let error = read_all(
            XmlSource::new(&path, STATIONS)
                .with_projection(projection)
                .with_expected_schema(stale),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("changed since the query was bound"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn reports_errors_and_honors_sample_size() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("rows.xml");
        std::fs::write(
            &path,
            "<rows><row><a>1</a></row><row><a>2</a><b>x</b></row></rows>",
        )
        .unwrap();
        let path = path.to_string_lossy();
        let (schema, _) =
            read_all(XmlSource::new(path.clone(), "/rows/row").with_sample_records(1))
                .await
                .unwrap();
        assert_eq!(schema.fields().len(), 1);
        let (_, batch) = read_all(XmlSource::new(path.clone(), "/rows/row"))
            .await
            .unwrap();
        assert_eq!(batch.column_by_name("b").unwrap().null_count(), 1);

        let error = read_all(XmlSource::new(
            fixture("noaa_stations_al.xml"),
            "/wx_station_index/x",
        ))
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("matched no fields"), "{error}");

        let broken = temp.path().join("broken.xml");
        std::fs::write(&broken, "<rows><row><a>1</a></row><row><a>2</row></rows>").unwrap();
        let error = read_all(XmlSource::new(broken.to_string_lossy(), "/rows/row"))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("XML input at byte"), "{error}");
    }
}
