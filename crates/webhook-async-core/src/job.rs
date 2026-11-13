use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::JobState;

/// Stable unique identifier for a durable asynchronous job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobId(pub Uuid);

impl JobId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for JobId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(s).map(Self)
    }
}

/// Ownership token assigned atomically when a worker claims a job lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseToken(pub Uuid);

impl LeaseToken {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for LeaseToken {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for LeaseToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Request-derived temporary file material persisted inside an immutable execution snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TempFileSpec {
    pub env_name: String,
    pub content: Vec<u8>,
}

/// Immutable execution snapshot constructed before HTTP 202 acceptance so later
/// configuration reloads cannot alter in-flight or queued jobs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionSpec {
    pub hook_id: String,
    pub executable: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub working_dir: Option<String>,
    pub temp_files: Vec<TempFileSpec>,
    pub timeout_seconds: u64,
    pub capture_output: bool,
    pub max_output_bytes: usize,
    pub config_version: String,
}

/// Failure classification guiding retry versus dead-letter decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    Transient,
    Permanent,
    Configuration,
    Timeout,
    Cancelled,
}

impl ErrorClass {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Permanent => "permanent",
            Self::Configuration => "configuration",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Backend-neutral representation of a durable queued/executed webhook job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: JobId,
    pub hook_id: String,
    pub state: JobState,
    pub priority: i32,
    pub created_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
    pub attempt: u32,
    pub max_attempts: u32,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub idempotency_key: Option<String>,
    pub worker_id: Option<String>,
    pub lease_token: Option<LeaseToken>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub execution_spec: ExecutionSpec,
    pub request_id: String,
    pub request_method: String,
    pub remote_addr: Option<String>,
    pub exit_code: Option<i32>,
    pub stdout_preview: Option<String>,
    pub stderr_preview: Option<String>,
    pub error_class: Option<ErrorClass>,
    pub error_summary: Option<String>,
}

/// Per-attempt execution audit record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobAttempt {
    pub job_id: JobId,
    pub attempt: u32,
    pub worker_id: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub exit_code: Option<i32>,
    pub error_class: Option<ErrorClass>,
    pub error_summary: Option<String>,
    pub stdout_preview: Option<String>,
    pub stderr_preview: Option<String>,
}

/// Backend-neutral worker registration & heartbeat view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub id: String,
    pub hostname: String,
    pub concurrency: u32,
    pub active_jobs: u32,
    pub completed_count: u64,
    pub failed_count: u64,
    pub last_heartbeat_at: DateTime<Utc>,
    pub draining: bool,
}
