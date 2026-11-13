use thiserror::Error;
use webhook_async_core::{CoreError, JobId, JobState, LeaseToken};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("job '{0}' not found")]
    NotFound(JobId),

    #[error("lease ownership mismatch for job '{job_id}' (token '{token}')")]
    OwnershipMismatch { job_id: JobId, token: LeaseToken },

    #[error("invalid job state transition for '{job_id}' from '{from}' to '{to}'")]
    InvalidTransition {
        job_id: JobId,
        from: JobState,
        to: JobState,
    },

    #[error("queue capacity exceeded ({pending}/{limit} pending jobs)")]
    CapacityExceeded { pending: u64, limit: u64 },

    #[error("invalid storage query input: {0}")]
    InvalidInput(String),

    #[error("storage backend unavailable: {0}")]
    BackendUnavailable(String),

    #[error("internal storage error: {0}")]
    Internal(String),

    #[error(transparent)]
    Core(#[from] CoreError),
}
