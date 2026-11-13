use std::path::PathBuf;

use thiserror::Error;
use webhook_async_compat::CompatError;
use webhook_async_core::CoreError;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration file '{path}': {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse TOML configuration: {0}")]
    TomlParse(#[from] toml::de::Error),

    #[error("configuration validation error in '{field}': {message}")]
    Validation {
        field: &'static str,
        message: String,
    },

    #[error("asynchronous execution requires a configured [storage] backend")]
    MissingQueueBackend,

    #[error("duplicate hook id '{id}'")]
    DuplicateHookId { id: String },

    #[error(transparent)]
    Compat(#[from] CompatError),

    #[error(transparent)]
    Core(#[from] CoreError),
}

impl ConfigError {
    pub(crate) fn validation(field: &'static str, message: impl Into<String>) -> Self {
        Self::Validation {
            field,
            message: message.into(),
        }
    }
}
