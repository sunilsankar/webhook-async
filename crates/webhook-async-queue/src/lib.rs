use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use webhook_async_config::QueueConfig;
use webhook_async_core::{
    CoreError, ErrorClass, ExecutionSpec, JobId, JobRecord, JobState, LeaseToken,
};
use webhook_async_storage::{
    EnqueueOutcome, ExecutionFailure, FailureDisposition, QueueStorage, StorageError,
};

#[derive(Debug, Error)]
pub enum QueueError {
    #[error("queue capacity exceeded ({pending}/{limit} pending jobs)")]
    CapacityFull { pending: u64, limit: u64 },

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Core(#[from] CoreError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueueMetadata {
    pub request_id: String,
    pub request_method: String,
    pub remote_addr: Option<String>,
    pub idempotency_key: Option<String>,
    pub priority: i32,
    pub max_attempts: Option<u32>,
}

/// Backend-neutral coordinator handling admission control, failure classification,
/// retry backoff with jitter, and state-safe cancellation/retry operations.
#[derive(Clone)]
pub struct QueueCoordinator {
    storage: Arc<dyn QueueStorage>,
    config: QueueConfig,
}

impl QueueCoordinator {
    #[must_use]
    pub fn new(storage: Arc<dyn QueueStorage>, config: QueueConfig) -> Self {
        Self { storage, config }
    }

    #[must_use]
    pub fn storage(&self) -> &Arc<dyn QueueStorage> {
        &self.storage
    }

    #[must_use]
    pub const fn config(&self) -> &QueueConfig {
        &self.config
    }

    pub async fn enqueue(
        &self,
        spec: ExecutionSpec,
        meta: EnqueueMetadata,
        now: DateTime<Utc>,
    ) -> Result<EnqueueOutcome, QueueError> {
        let max_attempts = meta
            .max_attempts
            .unwrap_or(self.config.default_max_attempts);
        if max_attempts == 0 {
            return Err(CoreError::Validation("max_attempts must be > 0".into()).into());
        }

        // shortcut: non-transactional count_pending check before enqueue can briefly overshoot
        // max_pending_jobs under concurrent bursts; enforce inside a single storage check if a
        // strict hard cap is required.
        let pending = self.storage.count_pending().await?;
        if pending >= self.config.max_pending_jobs {
            return Err(QueueError::CapacityFull {
                pending,
                limit: self.config.max_pending_jobs,
            });
        }

        let job = JobRecord {
            id: JobId::new(),
            hook_id: spec.hook_id.clone(),
            state: JobState::Queued,
            priority: meta.priority,
            created_at: now,
            available_at: now,
            started_at: None,
            completed_at: None,
            updated_at: now,
            attempt: 0,
            max_attempts,
            next_retry_at: None,
            idempotency_key: meta.idempotency_key,
            worker_id: None,
            lease_token: None,
            lease_expires_at: None,
            heartbeat_at: None,
            execution_spec: spec,
            request_id: meta.request_id,
            request_method: meta.request_method,
            remote_addr: meta.remote_addr,
            exit_code: None,
            stdout_preview: None,
            stderr_preview: None,
            error_class: None,
            error_summary: None,
        };

        Ok(self
            .storage
            .enqueue(job, self.config.idempotency_retention_seconds)
            .await?)
    }

    /// Computes capped exponential backoff with deterministic ±25% downward jitter (`[75%, 100%]`)
    /// so retries spread cleanly without exceeding `retry_max_delay_seconds`.
    #[must_use]
    pub fn compute_retry_delay(&self, job_id: JobId, attempt: u32) -> Duration {
        let base = self.config.retry_base_delay_seconds.max(1);
        let max_delay = self.config.retry_max_delay_seconds.max(base);
        let shift = attempt.saturating_sub(1).min(30);
        let exp = base.saturating_mul(1u64 << shift);
        let capped_ms = exp.min(max_delay).saturating_mul(1000);

        // Mix job_id + attempt via FNV-1a to derive jitter in [750, 1000] permille (no rand crate needed).
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in job_id.0.as_bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        for b in attempt.to_le_bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        let factor_permille = 750 + (h % 251);
        let jittered_ms = ((u128::from(capped_ms) * u128::from(factor_permille)) / 1000) as u64;
        let clamped_ms = jittered_ms.max(1000);
        Duration::milliseconds(i64::try_from(clamped_ms).unwrap_or(i64::MAX))
    }

    #[must_use]
    pub fn classify_failure(
        &self,
        error_class: ErrorClass,
        next_attempt_count: u32,
        max_attempts: u32,
        job_state: JobState,
        job_id: JobId,
        now: DateTime<Utc>,
    ) -> FailureDisposition {
        if job_state == JobState::CancelRequested || error_class == ErrorClass::Cancelled {
            return FailureDisposition::Failed;
        }
        match error_class {
            ErrorClass::Permanent | ErrorClass::Configuration => FailureDisposition::DeadLetter,
            ErrorClass::Transient | ErrorClass::Timeout => {
                if next_attempt_count >= max_attempts {
                    FailureDisposition::DeadLetter
                } else {
                    FailureDisposition::RetryWait {
                        next_retry_at: now + self.compute_retry_delay(job_id, next_attempt_count),
                    }
                }
            }
            ErrorClass::Cancelled => FailureDisposition::Failed,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn complete_failure(
        &self,
        job: &JobRecord,
        lease_token: LeaseToken,
        error_class: ErrorClass,
        error_summary: String,
        exit_code: Option<i32>,
        stdout_preview: Option<String>,
        stderr_preview: Option<String>,
        now: DateTime<Utc>,
    ) -> Result<JobRecord, QueueError> {
        if error_class == ErrorClass::Cancelled {
            return Ok(self
                .storage
                .ack_cancelled(
                    job.id,
                    lease_token,
                    now,
                    Some(error_summary),
                    stdout_preview,
                    stderr_preview,
                )
                .await?);
        }

        let next_attempt = job.attempt.saturating_add(1);
        let disposition = self.classify_failure(
            error_class,
            next_attempt,
            job.max_attempts,
            job.state,
            job.id,
            now,
        );
        Ok(self
            .storage
            .ack_failure(
                job.id,
                lease_token,
                ExecutionFailure {
                    error_class,
                    error_summary,
                    exit_code,
                    stdout_preview,
                    stderr_preview,
                    disposition,
                    finished_at: now,
                },
            )
            .await?)
    }

    pub async fn cancel_job(
        &self,
        job_id: JobId,
        now: DateTime<Utc>,
    ) -> Result<JobRecord, QueueError> {
        Ok(self.storage.request_cancel(job_id, now).await?)
    }

    pub async fn retry_job(
        &self,
        job_id: JobId,
        now: DateTime<Utc>,
    ) -> Result<JobRecord, QueueError> {
        Ok(self.storage.retry_job(job_id, now).await?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use chrono::{Duration, Utc};
    use webhook_async_config::{QueueConfig, SqliteStorageConfig};
    use webhook_async_core::{ErrorClass, ExecutionSpec, JobId, JobState};
    use webhook_async_storage::FailureDisposition;
    use webhook_async_storage_sqlite::SqliteStorage;

    use crate::{EnqueueMetadata, QueueCoordinator, QueueError};

    fn test_spec() -> ExecutionSpec {
        ExecutionSpec {
            hook_id: "deploy".into(),
            executable: "/bin/echo".into(),
            args: vec!["ok".into()],
            env: BTreeMap::new(),
            working_dir: None,
            temp_files: Vec::new(),
            timeout_seconds: 30,
            capture_output: true,
            max_output_bytes: 1024,
            config_version: "sha256:test".into(),
        }
    }

    async fn sqlite_coordinator(queue_cfg: QueueConfig) -> (tempfile::TempDir, QueueCoordinator) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("queue.db").to_string_lossy().into_owned();
        let storage = SqliteStorage::connect(&SqliteStorageConfig {
            database_url: db_path,
            wal_mode: true,
            busy_timeout_ms: 5000,
        })
        .await
        .expect("connect");
        (dir, QueueCoordinator::new(Arc::new(storage), queue_cfg))
    }

    #[tokio::test]
    async fn enforces_pending_capacity_limit() {
        let cfg = QueueConfig {
            max_pending_jobs: 1,
            ..Default::default()
        };
        let (_dir, coord) = sqlite_coordinator(cfg).await;
        let now = Utc::now();

        coord
            .enqueue(
                test_spec(),
                EnqueueMetadata {
                    request_id: "r1".into(),
                    request_method: "POST".into(),
                    remote_addr: None,
                    idempotency_key: None,
                    priority: 0,
                    max_attempts: None,
                },
                now,
            )
            .await
            .expect("first enqueue fits");

        let err = coord
            .enqueue(
                test_spec(),
                EnqueueMetadata {
                    request_id: "r2".into(),
                    request_method: "POST".into(),
                    remote_addr: None,
                    idempotency_key: None,
                    priority: 0,
                    max_attempts: None,
                },
                now,
            )
            .await
            .expect_err("second enqueue exceeds max_pending_jobs");
        assert!(matches!(
            err,
            QueueError::CapacityFull {
                pending: 1,
                limit: 1
            }
        ));
    }

    #[tokio::test]
    async fn retry_backoff_is_bounded_and_classifies_permanent_vs_transient() {
        let cfg = QueueConfig {
            retry_base_delay_seconds: 5,
            retry_max_delay_seconds: 30,
            ..Default::default()
        };
        let (_dir, coord) = sqlite_coordinator(cfg).await;
        let job_id = JobId::new();
        let now = Utc::now();

        let d1 = coord.compute_retry_delay(job_id, 1);
        let d2 = coord.compute_retry_delay(job_id, 2);
        let d10 = coord.compute_retry_delay(job_id, 10);
        assert!(d1 >= Duration::seconds(3) && d1 <= Duration::seconds(5));
        assert!(d2 >= Duration::seconds(7) && d2 <= Duration::seconds(10));
        assert!(d10 >= Duration::seconds(22) && d10 <= Duration::seconds(30));

        assert!(matches!(
            coord.classify_failure(ErrorClass::Permanent, 1, 5, JobState::Running, job_id, now),
            FailureDisposition::DeadLetter
        ));
        assert!(matches!(
            coord.classify_failure(
                ErrorClass::Configuration,
                1,
                5,
                JobState::Running,
                job_id,
                now
            ),
            FailureDisposition::DeadLetter
        ));
        assert!(matches!(
            coord.classify_failure(ErrorClass::Transient, 1, 2, JobState::Running, job_id, now),
            FailureDisposition::RetryWait { .. }
        ));
        assert!(matches!(
            coord.classify_failure(ErrorClass::Timeout, 2, 2, JobState::Running, job_id, now),
            FailureDisposition::DeadLetter
        ));
    }

    #[tokio::test]
    async fn end_to_end_transient_retry_exhaustion_and_manual_retry() {
        let cfg = QueueConfig {
            default_max_attempts: 2,
            retry_base_delay_seconds: 2,
            ..Default::default()
        };
        let (_dir, coord) = sqlite_coordinator(cfg).await;
        let now = Utc::now();

        let job = coord
            .enqueue(
                test_spec(),
                EnqueueMetadata {
                    request_id: "r1".into(),
                    request_method: "POST".into(),
                    remote_addr: Some("127.0.0.1:9999".into()),
                    idempotency_key: Some("k1".into()),
                    priority: 0,
                    max_attempts: None,
                },
                now,
            )
            .await
            .expect("enqueue")
            .into_job();

        // Attempt 1 fails transiently -> RetryWait
        let claimed1 = coord
            .storage()
            .claim_due("w1", now, now + Duration::seconds(60), 1)
            .await
            .expect("claim1")
            .pop()
            .expect("job");
        let after_f1 = coord
            .complete_failure(
                &claimed1,
                claimed1.lease_token.expect("token"),
                ErrorClass::Transient,
                "exit 1".into(),
                Some(1),
                None,
                None,
                now + Duration::seconds(1),
            )
            .await
            .expect("fail 1");
        assert_eq!(after_f1.state, JobState::RetryWait);
        let due = after_f1.next_retry_at.expect("next_retry_at");

        // Promote -> Attempt 2 fails transiently -> DeadLetter
        coord
            .storage()
            .promote_due_retries(due)
            .await
            .expect("promote");
        let claimed2 = coord
            .storage()
            .claim_due("w1", due, due + Duration::seconds(60), 1)
            .await
            .expect("claim2")
            .pop()
            .expect("job");
        let after_f2 = coord
            .complete_failure(
                &claimed2,
                claimed2.lease_token.expect("token"),
                ErrorClass::Transient,
                "exit 2".into(),
                Some(2),
                None,
                None,
                due + Duration::seconds(1),
            )
            .await
            .expect("fail 2");
        assert_eq!(after_f2.state, JobState::DeadLetter);
        assert_eq!(after_f2.attempt, 2);

        // Operator retry -> Queued with bumped max_attempts and preserved attempts history
        let retried = coord
            .retry_job(job.id, due + Duration::seconds(5))
            .await
            .expect("manual retry");
        assert_eq!(retried.state, JobState::Queued);
        assert!(retried.max_attempts >= 3);
        assert_eq!(
            coord
                .storage()
                .list_attempts(job.id)
                .await
                .expect("attempts")
                .len(),
            2
        );
    }
}
