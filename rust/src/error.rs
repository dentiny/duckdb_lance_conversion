use std::fmt::Display;

use thiserror::Error as ThisError;

use crate::error_struct::{ErrorStatus, ErrorStruct};

#[derive(Clone, Debug, ThisError)]
pub enum Error {
    #[error("{0}")]
    InvalidArgument(ErrorStruct),

    #[error("{0}")]
    Internal(ErrorStruct),

    #[error("{0}")]
    Arrow(ErrorStruct),

    #[error("{0}")]
    Io(ErrorStruct),

    #[error("{0}")]
    Lance(ErrorStruct),

    #[error("{0}")]
    OpenDal(ErrorStruct),

    #[error("{0}")]
    Parquet(ErrorStruct),

    #[error("{0}")]
    Http(ErrorStruct),

    #[error("{0}")]
    Task(ErrorStruct),

    #[error("{0}")]
    Url(ErrorStruct),

    #[error("{0}")]
    Utf8(ErrorStruct),
}

impl Error {
    #[track_caller]
    pub fn message(message: impl Into<String>) -> Self {
        Self::Internal(ErrorStruct::new(message, ErrorStatus::Permanent))
    }

    #[track_caller]
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(ErrorStruct::new(message, ErrorStatus::Permanent))
    }

    pub fn status(&self) -> ErrorStatus {
        self.inner().status
    }

    fn inner(&self) -> &ErrorStruct {
        match self {
            Self::InvalidArgument(inner)
            | Self::Internal(inner)
            | Self::Arrow(inner)
            | Self::Io(inner)
            | Self::Lance(inner)
            | Self::OpenDal(inner)
            | Self::Parquet(inner)
            | Self::Http(inner)
            | Self::Task(inner)
            | Self::Url(inner)
            | Self::Utf8(inner) => inner,
        }
    }

    fn inner_mut(&mut self) -> &mut ErrorStruct {
        match self {
            Self::InvalidArgument(inner)
            | Self::Internal(inner)
            | Self::Arrow(inner)
            | Self::Io(inner)
            | Self::Lance(inner)
            | Self::OpenDal(inner)
            | Self::Parquet(inner)
            | Self::Http(inner)
            | Self::Task(inner)
            | Self::Url(inner)
            | Self::Utf8(inner) => inner,
        }
    }

    fn with_context(mut self, context: impl Display) -> Self {
        let inner = self.inner_mut();
        inner.message = format!("{context}: {}", inner.message);
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) trait ResultExt<T> {
    fn context(self, context: impl Display) -> Result<T>;
}

impl<T, E> ResultExt<T> for std::result::Result<T, E>
where
    E: Into<Error>,
{
    fn context(self, context: impl Display) -> Result<T> {
        self.map_err(|source| source.into().with_context(context))
    }
}

macro_rules! permanent_error_from {
    ($source:ty, $variant:ident, $message:literal) => {
        impl From<$source> for Error {
            #[track_caller]
            fn from(source: $source) -> Self {
                Self::$variant(
                    ErrorStruct::new($message, ErrorStatus::Permanent).with_source(source),
                )
            }
        }
    };
}

permanent_error_from!(arrow_schema::ArrowError, Arrow, "Arrow error");
permanent_error_from!(lance::Error, Lance, "Lance error");
permanent_error_from!(parquet::errors::ParquetError, Parquet, "Parquet error");
permanent_error_from!(tokio::task::JoinError, Task, "async task error");
permanent_error_from!(url::ParseError, Url, "URL parse error");
permanent_error_from!(std::str::Utf8Error, Utf8, "UTF-8 decode error");

impl From<reqwest::Error> for Error {
    #[track_caller]
    fn from(source: reqwest::Error) -> Self {
        let temporary = source.is_connect()
            || source.is_timeout()
            || source.status().is_some_and(|status| {
                status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
            });
        let status = if temporary {
            ErrorStatus::Temporary
        } else {
            ErrorStatus::Permanent
        };
        Self::Http(ErrorStruct::new("HTTP error", status).with_source(source))
    }
}

impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        let status = match source.kind() {
            std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::TimedOut => ErrorStatus::Temporary,
            _ => ErrorStatus::Permanent,
        };
        Self::Io(ErrorStruct::new("I/O error", status).with_source(source))
    }
}

impl From<opendal::Error> for Error {
    #[track_caller]
    fn from(source: opendal::Error) -> Self {
        let status = if source.is_temporary() {
            ErrorStatus::Temporary
        } else {
            ErrorStatus::Permanent
        };
        Self::OpenDal(ErrorStruct::new("OpenDAL error", status).with_source(source))
    }
}
