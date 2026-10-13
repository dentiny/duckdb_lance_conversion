use std::io::Read;

use flate2::read::MultiGzDecoder;
use opendal::Operator;

use crate::error::ResultExt;
use crate::{Error, Result};

/// A Common Crawl segment, addressed by an object path ending in
/// `crawl-data/<crawl>/segments/<segment>` or its `warc` subdirectory.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct WarcSegment {
    /// Object path prefix before `crawl-data/`, empty or ending in `/`; for
    /// example `""` for `crawl-data/...` or `"mirror/cc/"` for
    /// `mirror/cc/crawl-data/...`.
    root: String,
    /// Crawl name, for example `CC-MAIN-2024-10`.
    crawl: String,
    /// Segment name within the crawl, for example `1707947473347.0`.
    segment: String,
}

impl WarcSegment {
    pub(super) fn parse(path: &str) -> Option<Self> {
        let parts = path
            .split('/')
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        let parts = parts.strip_suffix(&["warc"]).unwrap_or(&parts);
        match parts {
            [root @ .., "crawl-data", crawl, "segments", segment] => Some(Self {
                root: root.iter().map(|part| format!("{part}/")).collect(),
                crawl: (*crawl).to_owned(),
                segment: (*segment).to_owned(),
            }),
            _ => None,
        }
    }

    /// Lists the segment's WARC files from its crawl's `warc.paths.gz`.
    /// Manifest entries are relative to the `crawl-data/` parent.
    pub(super) async fn files(&self, operator: &Operator) -> Result<Vec<String>> {
        let manifest_path = format!("{}crawl-data/{}/warc.paths.gz", self.root, self.crawl);
        // Crawl manifests are a few MiB uncompressed, so they are read whole.
        let manifest = operator
            .read(&manifest_path)
            .await
            .context(format!("reading WARC manifest {manifest_path}"))?
            .to_bytes();
        self.files_in_manifest(&manifest)
    }

    fn files_in_manifest(&self, manifest: &[u8]) -> Result<Vec<String>> {
        let mut text = String::new();
        MultiGzDecoder::new(manifest)
            .read_to_string(&mut text)
            .map_err(|error| Error::message(format!("decoding WARC manifest: {error}")))?;
        let prefix = format!("crawl-data/{}/segments/{}/warc/", self.crawl, self.segment);
        let files = text
            .lines()
            .map(str::trim)
            .filter(|entry| entry.len() > prefix.len() && entry.starts_with(&prefix))
            .map(|entry| format!("{}{entry}", self.root))
            .collect::<Vec<_>>();
        if files.is_empty() {
            return Err(Error::message(format!(
                "WARC segment contains no files: {prefix}"
            )));
        }
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::gzip;
    use super::*;

    fn segment(root: &str, crawl: &str, segment: &str) -> Option<WarcSegment> {
        Some(WarcSegment {
            root: root.to_owned(),
            crawl: crawl.to_owned(),
            segment: segment.to_owned(),
        })
    }

    #[test]
    fn parses_segment_and_warc_directories() {
        for suffix in ["", "/", "/warc", "/warc/"] {
            assert_eq!(
                WarcSegment::parse(&format!("crawl-data/CC-MAIN-2026-30/segments/1.52{suffix}")),
                segment("", "CC-MAIN-2026-30", "1.52")
            );
        }
        assert_eq!(
            WarcSegment::parse("data/cc/crawl-data/c/segments/1/warc/"),
            segment("data/cc/", "c", "1")
        );
        for path in [
            "crawl-data/c/segments/1/wet/",
            "crawl-data/c/segments/1/warc/a.warc.gz",
            "crawl-data/c/segments/",
            "crawl-data/c/warc.paths.gz",
            "archive.warc.gz",
        ] {
            assert_eq!(WarcSegment::parse(path), None, "{path}");
        }
    }

    #[test]
    fn keeps_only_manifest_entries_in_the_segment() {
        let manifest = gzip(
            b"crawl-data/c/segments/1/warc/a.warc.gz\n\n\
              crawl-data/c/segments/10/warc/b.warc.gz\n\
              crawl-data/c/segments/1/wet/a.warc.wet.gz\n\
              crawl-data/c/segments/1/warc/c.warc.gz  \n",
        );

        let files = segment("root/", "c", "1")
            .unwrap()
            .files_in_manifest(&manifest)
            .unwrap();
        assert_eq!(
            files,
            [
                "root/crawl-data/c/segments/1/warc/a.warc.gz",
                "root/crawl-data/c/segments/1/warc/c.warc.gz",
            ]
        );
        let error = segment("", "c", "2")
            .unwrap()
            .files_in_manifest(&manifest)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("WARC segment contains no files: crawl-data/c/segments/2/warc/"));
    }
}
