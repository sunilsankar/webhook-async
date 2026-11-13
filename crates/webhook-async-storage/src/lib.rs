pub mod conformance;
pub mod error;

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use webhook_async_core::{ErrorClass, JobAttempt, JobId, JobRecord, JobState, LeaseToken};

pub use error::StorageError;

pub const MAX_PAGE_LIMIT: u32 = 500;

/// Explicit capability descriptor allowing callers to inspect backend characteristics
/// without exposing backend-specific types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendCapabilities {
    pub backend: &'static str,
    pub supports_priority: bool,
    pub supports_multi_host: bool,
    pub supports_idempotency: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Created(Box<JobRecord>),
    Deduplicated(Box<JobRecord>),
}

impl EnqueueOutcome {
    #[must_use]
    pub fn job(&self) -> &JobRecord {
        match self {
            Self::Created(job) | Self::Deduplicated(job) => job,
        }
    }

    #[must_use]
    pub fn into_job(self) -> JobRecord {
        match self {
            Self::Created(job) | Self::Deduplicated(job) => *job,
        }
    }

    #[must_use]
    pub fn is_deduplicated(&self) -> bool {
        matches!(self, Self::Deduplicated(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionSuccess {
    pub exit_code: i32,
    pub stdout_preview: Option<String>,
    pub stderr_preview: Option<String>,
    pub finished_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureDisposition {
    RetryWait { next_retry_at: DateTime<Utc> },
    Failed,
    DeadLetter,
}

impl FailureDisposition {
    #[must_use]
    pub const fn target_state(self) -> JobState {
        match self {
            Self::RetryWait { .. } => JobState::RetryWait,
            Self::Failed => JobState::Failed,
            Self::DeadLetter => JobState::DeadLetter,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionFailure {
    pub error_class: ErrorClass,
    pub error_summary: String,
    pub exit_code: Option<i32>,
    pub stdout_preview: Option<String>,
    pub stderr_preview: Option<String>,
    pub disposition: FailureDisposition,
    pub finished_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFilter {
    pub state: Option<JobState>,
    pub hook_id: Option<String>,
    pub limit: u32,
    pub offset: u64,
}

impl Default for JobFilter {
    fn default() -> Self {
        Self {
            state: None,
            hook_id: None,
            limit: 50,
            offset: 0,
        }
    }
}

impl JobFilter {
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.limit == 0 || self.limit > MAX_PAGE_LIMIT {
            return Err(StorageError::InvalidInput(format!(
                "pagination limit must be between 1 and {MAX_PAGE_LIMIT}, got {}",
                self.limit
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPage {
    pub items: Vec<JobRecord>,
    pub total: u64,
    pub limit: u32,
    pub offset: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueStats {
    pub counts_by_state: BTreeMap<String, u64>,
    pub pending_count: u64,
}

/// Backend-neutral persistence contract implemented by SQLite and shared Valkey/Redis adapters.
///
/// Trait exists as the architectural seam isolating domain, queue, worker, and API crates from
/// database drivers and wire protocols (`AGENTS.md` §5.2–5.3).
#[async_trait]
pub trait QueueStorage: Send + Sync {
    fn capabilities(&self) -> BackendCapabilities;

    async fn ping(&self) -> Result<(), StorageError>;

    async fn count_pending(&self) -> Result<u64, StorageError>;

    async fn stats(&self) -> Result<QueueStats, StorageError>;

    async fn enqueue(
        &self,
        job: JobRecord,
        idempotency_retention_seconds: u64,
    ) -> Result<EnqueueOutcome, StorageError>;

    async fn claim_due(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<JobRecord>, StorageError>;

    async fn heartbeat(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    ) -> Result<JobRecord, StorageError>;

    async fn ack_success(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        success: ExecutionSuccess,
    ) -> Result<JobRecord, StorageError>;

    async fn ack_failure(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        failure: ExecutionFailure,
    ) -> Result<JobRecord, StorageError>;

    async fn ack_cancelled(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        finished_at: DateTime<Utc>,
        summary: Option<String>,
        stdout_preview: Option<String>,
        stderr_preview: Option<String>,
    ) -> Result<JobRecord, StorageError>;

    async fn promote_due_retries(&self, now: DateTime<Utc>) -> Result<u64, StorageError>;

    async fn recover_expired_leases(&self, now: DateTime<Utc>) -> Result<u64, StorageError>;

    async fn request_cancel(
        &self,
        job_id: JobId,
        now: DateTime<Utc>,
    ) -> Result<JobRecord, StorageError>;

    async fn retry_job(&self, job_id: JobId, now: DateTime<Utc>)
    -> Result<JobRecord, StorageError>;

    async fn get_job(&self, job_id: JobId) -> Result<Option<JobRecord>, StorageError>;

    async fn list_attempts(&self, job_id: JobId) -> Result<Vec<JobAttempt>, StorageError>;

    async fn list_jobs(&self, filter: JobFilter) -> Result<JobPage, StorageError>;
}
