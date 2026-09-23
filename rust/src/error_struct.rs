use std::fmt;
use std::panic::Location;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorStatus {
    Temporary,
    Permanent,
}

impl fmt::Display for ErrorStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Temporary => write!(f, "temporary"),
            Self::Permanent => write!(f, "permanent"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ErrorStruct {
    pub message: String,
    pub status: ErrorStatus,
    pub source: Option<Arc<anyhow::Error>>,
    pub location: Option<String>,
}

impl ErrorStruct {
    #[track_caller]
    pub fn new(message: impl Into<String>, status: ErrorStatus) -> Self {
        let location = Location::caller();
        Self {
            message: message.into(),
            status,
            source: None,
            location: Some(format!(
                "{}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            )),
        }
    }

    pub fn with_source(mut self, source: impl Into<anyhow::Error>) -> Self {
        assert!(
            self.source.is_none(),
            "the source error has already been set"
        );
        self.source = Some(Arc::new(source.into()));
        self
    }

    pub fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|source| source.as_ref().as_ref())
    }
}

impl fmt::Display for ErrorStruct {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.status)?;
        if let Some(location) = &self.location {
            write!(f, " at {location}")?;
        }
        if let Some(source) = &self.source {
            write!(f, ", caused by: {source}")?;
        }
        Ok(())
    }
}
