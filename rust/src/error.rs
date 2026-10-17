use std::fmt::{self, Display};
use std::panic::Location;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorStatus {
    Temporary,
    Permanent,
}

impl Display for ErrorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Temporary => write!(f, "temporary"),
            Self::Permanent => write!(f, "permanent"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Error {
    message: String,
    status: ErrorStatus,
    source: Option<Arc<anyhow::Error>>,
    location: &'static Location<'static>,
}

impl Error {
    #[track_caller]
    fn new(message: impl Into<String>, status: ErrorStatus) -> Self {
        Self {
            message: message.into(),
            status,
            source: None,
            location: Location::caller(),
        }
    }

    #[track_caller]
    fn from_source(message: &str, temporary: bool, source: impl Into<anyhow::Error>) -> Self {
        let status = if temporary {
            ErrorStatus::Temporary
        } else {
            ErrorStatus::Permanent
        };
        Self {
            source: Some(Arc::new(source.into())),
            ..Self::new(message, status)
        }
    }

    #[track_caller]
    pub fn message(message: impl Into<String>) -> Self {
        Self::new(message, ErrorStatus::Permanent)
    }

    pub fn status(&self) -> ErrorStatus {
        self.status
    }

    fn with_context(mut self, context: impl Display) -> Self {
        self.message = format!("{context}: {}", self.message);
        self
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}) at {}", self.message, self.status, self.location)?;
        if let Some(source) = &self.source {
            write!(f, ", caused by: {source}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {}

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
    ($source:ty, $message:literal) => {
        impl From<$source> for Error {
            #[track_caller]
            fn from(source: $source) -> Self {
                Self::from_source($message, false, source)
            }
        }
    };
}

permanent_error_from!(arrow_schema::ArrowError, "Arrow error");
permanent_error_from!(lance::Error, "Lance error");
permanent_error_from!(parquet::errors::ParquetError, "Parquet error");
permanent_error_from!(tokio::task::JoinError, "async task error");
permanent_error_from!(url::ParseError, "URL parse error");
permanent_error_from!(std::str::Utf8Error, "UTF-8 decode error");

impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(source: std::io::Error) -> Self {
        let temporary = matches!(
            source.kind(),
            std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::TimedOut
        );
        Self::from_source("I/O error", temporary, source)
    }
}

impl From<opendal::Error> for Error {
    #[track_caller]
    fn from(source: opendal::Error) -> Self {
        Self::from_source("OpenDAL error", source.is_temporary(), source)
    }
}
