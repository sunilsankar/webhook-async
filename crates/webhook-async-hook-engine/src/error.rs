use thiserror::Error;
use webhook_async_core::ValueSource;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum HookEngineError {
    #[error("parameter '{name}' from source '{}' not found", value_source.as_str())]
    ParameterMissing {
        value_source: ValueSource,
        name: String,
    },

    #[error("signature validation failed: {0}")]
    SignatureMismatch(String),

    #[error("failed to parse request payload: {0}")]
    PayloadParse(String),

    #[error("parameter extraction error: {0}")]
    Extraction(String),

    #[error("trigger evaluation error: {0}")]
    Evaluation(String),
}

impl HookEngineError {
    #[must_use]
    pub const fn is_parameter_missing(&self) -> bool {
        matches!(self, Self::ParameterMissing { .. })
    }

    #[must_use]
    pub const fn is_signature_mismatch(&self) -> bool {
        matches!(self, Self::SignatureMismatch(_))
    }
}
