use thiserror::Error;

use crate::JobState;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CoreError {
    #[error("invalid job state transition from '{from}' to '{to}'")]
    InvalidStateTransition { from: JobState, to: JobState },

    #[error("--sync and --async cannot be used together")]
    ConflictingExecutionModeFlags,

    #[error("validation error: {0}")]
    Validation(String),
}
