//! Metadata shared by the ATIF reader and its Lance storage policy.

use serde::{Deserialize, Serialize};

pub(crate) const SOURCE_TYPE: &str = "lance_conversion:source_type";
pub(crate) const CONTENT_TYPES: &str = "lance_conversion:content_types";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ContentType {
    Text,
    Image,
    Audio,
}
