//! ATIF content variants and field-level storage metadata are separate concepts.

pub(crate) const SOURCE_TYPE: &str = "lance_conversion:source_type";
pub(crate) const STORAGE_POLICY: &str = "lance_conversion:storage_policy";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ContentType {
    Text,
    Image,
    Audio,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StoragePolicy {
    Text,
    Blob,
}

impl StoragePolicy {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Blob => "blob",
        }
    }
}
