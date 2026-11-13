use std::path::PathBuf;

use thiserror::Error;
use webhook_async_core::CoreError;

#[derive(Debug, Error)]
pub enum CompatError {
    #[error("failed to read hook configuration file '{path}': {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to parse JSON hook definition: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("failed to parse YAML hook definition: {0}")]
    YamlParse(#[from] yaml_serde::Error),

    #[error("duplicate hook id '{id}'")]
    DuplicateHookId { id: String },

    #[error("invalid hook '{id}': {message}")]
    InvalidHook { id: String, message: String },

    #[error("template processing error: {details}")]
    Template { details: String },

    #[error(transparent)]
    Core(#[from] CoreError),
}
