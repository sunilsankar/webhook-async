mod mapping;

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use webhook_async_config::SqliteStorageConfig;
use webhook_async_core::{JobAttempt, JobId, JobRecord, JobState, LeaseToken};
use webhook_async_storage::{
    BackendCapabilities, EnqueueOutcome, ExecutionFailure, ExecutionSuccess, FailureDisposition,
    JobFilter, JobPage, QueueStats, QueueStorage, StorageError,
};

use mapping::{error_class_str, fmt_ts, map_sqlx_err, parse_state, row_to_attempt, row_to_job};

#[derive(Debug, Clone)]
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    pub async fn connect(cfg: &SqliteStorageConfig) -> Result<Self, StorageError> {
        let is_memory = cfg.database_url == ":memory:" || cfg.database_url.contains("mode=memory");
        let mut opts = if cfg.database_url.starts_with("sqlite:") || is_memory {
            SqliteConnectOptions::from_str(&cfg.database_url).map_err(|err| {
                StorageError::BackendUnavailable(format!("invalid SQLite URL: {err}"))
            })?
        } else {
            SqliteConnectOptions::new().filename(&cfg.database_url)
        };

        opts = opts
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_millis(cfg.busy_timeout_ms))
            .journal_mode(if cfg.wal_mode && !is_memory {
                SqliteJournalMode::Wal
            } else {
                SqliteJournalMode::Delete
            })
            .synchronous(if cfg.wal_mode && !is_memory {
                SqliteSynchronous::Normal
            } else {
                SqliteSynchronous::Full
            });

        let max_conns = if is_memory { 1 } else { 4 };
        let pool = SqlitePoolOptions::new()
            .max_connections(max_conns)
            .connect_with(opts)
            .await
            .map_err(map_sqlx_err)?;

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|err| StorageError::Internal(format!("SQLite migration failed: {err}")))?;

        Ok(Self { pool })
    }

    /// Closes the underlying connection pool, rejecting future queries.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    async fn classify_lease_update_miss(
        &self,
        job_id: JobId,
        token: LeaseToken,
        target_state: JobState,
    ) -> StorageError {
        let row = match sqlx::query("SELECT state, lease_token FROM jobs WHERE id = ?1")
            .bind(job_id.to_string())
            .fetch_optional(&self.pool)
            .await
        {
            Ok(Some(row)) => row,
            Ok(None) => return StorageError::NotFound(job_id),
            Err(err) => return map_sqlx_err(err),
        };

        let state_raw: String = match row.try_get("state") {
            Ok(s) => s,
            Err(err) => return map_sqlx_err(err),
        };
        let state = match parse_state(&state_raw) {
            Ok(s) => s,
            Err(err) => return err,
        };
        let current_token: Option<String> = match row.try_get("lease_token") {
            Ok(t) => t,
            Err(err) => return map_sqlx_err(err),
        };

        if current_token.as_deref() != Some(&token.to_string()) {
            return StorageError::OwnershipMismatch { job_id, token };
        }
        StorageError::InvalidTransition {
            job_id,
            from: state,
            to: target_state,
        }
    }

    async fn classify_state_miss(&self, job_id: JobId, target_state: JobState) -> StorageError {
        match sqlx::query("SELECT state FROM jobs WHERE id = ?1")
            .bind(job_id.to_string())
            .fetch_optional(&self.pool)
            .await
        {
            Ok(Some(row)) => match row.try_get::<String, _>("state") {
                Ok(raw) => match parse_state(&raw) {
                    Ok(from) => StorageError::InvalidTransition {
                        job_id,
                        from,
                        to: target_state,
                    },
                    Err(err) => err,
                },
                Err(err) => map_sqlx_err(err),
            },
            Ok(None) => StorageError::NotFound(job_id),
            Err(err) => map_sqlx_err(err),
        }
    }
}

#[async_trait]
impl QueueStorage for SqliteStorage {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            backend: "sqlite",
            supports_priority: true,
            supports_multi_host: false,
            supports_idempotency: true,
        }
    }

    async fn ping(&self) -> Result<(), StorageError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(map_sqlx_err)
    }

    async fn count_pending(&self) -> Result<u64, StorageError> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM jobs WHERE state IN ('queued', 'running', 'retry_wait', 'cancel_requested')",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    async fn stats(&self) -> Result<QueueStats, StorageError> {
        let rows = sqlx::query("SELECT state, COUNT(*) AS cnt FROM jobs GROUP BY state")
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_err)?;

        let mut counts_by_state = BTreeMap::new();
        let mut pending_count = 0u64;
        for row in rows {
            let state_raw: String = row.try_get("state").map_err(map_sqlx_err)?;
            let count =
                u64::try_from(row.try_get::<i64, _>("cnt").map_err(map_sqlx_err)?).unwrap_or(0);
            if parse_state(&state_raw)?.is_pending_or_active() {
                pending_count = pending_count.saturating_add(count);
            }
            counts_by_state.insert(state_raw, count);
        }
        Ok(QueueStats {
            counts_by_state,
            pending_count,
        })
    }

    async fn enqueue(
        &self,
        job: JobRecord,
        idempotency_retention_seconds: u64,
    ) -> Result<EnqueueOutcome, StorageError> {
        if job.state != JobState::Queued {
            return Err(StorageError::InvalidInput(format!(
                "new job must start in 'queued' state, got '{}'",
                job.state
            )));
        }

        let spec_json = serde_json::to_string(&job.execution_spec).map_err(|err| {
            StorageError::Internal(format!("failed to serialize execution_spec: {err}"))
        })?;
        let created_ts = fmt_ts(job.created_at);
        let available_ts = fmt_ts(job.available_at);
        let updated_ts = fmt_ts(job.updated_at);
        let idem_expires_ts = job.idempotency_key.as_ref().map(|_| {
            let retention = i64::try_from(idempotency_retention_seconds).unwrap_or(i64::MAX / 2);
            fmt_ts(job.created_at + chrono::Duration::seconds(retention))
        });

        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;

        // Write-first in the tx acquires SQLite RESERVED lock immediately (avoiding read-to-write deadlocks).
        if let Some(key) = &job.idempotency_key {
            sqlx::query(
                "UPDATE jobs SET idempotency_key = NULL, idempotency_expires_at = NULL \
                 WHERE idempotency_key = ?1 AND idempotency_expires_at IS NOT NULL AND idempotency_expires_at <= ?2",
            )
            .bind(key)
            .bind(&created_ts)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
        }

        let res = sqlx::query(
            "INSERT INTO jobs (
                id, hook_id, state, priority, created_at, available_at, started_at, completed_at,
                updated_at, attempt, max_attempts, next_retry_at, idempotency_key, idempotency_expires_at,
                worker_id, lease_token, lease_expires_at, heartbeat_at, execution_spec_json,
                request_id, request_method, remote_addr, exit_code, stdout_preview, stderr_preview,
                error_class, error_summary
            ) VALUES (
                ?1, ?2, 'queued', ?3, ?4, ?5, NULL, NULL,
                ?6, ?7, ?8, NULL, ?9, ?10,
                NULL, NULL, NULL, NULL, ?11,
                ?12, ?13, ?14, NULL, NULL, NULL,
                NULL, NULL
            ) ON CONFLICT(idempotency_key) DO NOTHING",
        )
        .bind(job.id.to_string())
        .bind(&job.hook_id)
        .bind(i64::from(job.priority))
        .bind(&created_ts)
        .bind(&available_ts)
        .bind(&updated_ts)
        .bind(i64::from(job.attempt))
        .bind(i64::from(job.max_attempts))
        .bind(&job.idempotency_key)
        .bind(&idem_expires_ts)
        .bind(&spec_json)
        .bind(&job.request_id)
        .bind(&job.request_method)
        .bind(&job.remote_addr)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        if res.rows_affected() == 1 {
            tx.commit().await.map_err(map_sqlx_err)?;
            return Ok(EnqueueOutcome::Created(Box::new(job)));
        }

        let key = job.idempotency_key.as_deref().ok_or_else(|| {
            StorageError::Internal("INSERT conflict without idempotency_key".into())
        })?;
        let existing_row = sqlx::query("SELECT * FROM jobs WHERE idempotency_key = ?1")
            .bind(key)
            .fetch_one(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;
        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(EnqueueOutcome::Deduplicated(Box::new(row_to_job(
            &existing_row,
        )?)))
    }

    async fn claim_due(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<JobRecord>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let now_ts = fmt_ts(now);
        let lease_ts = fmt_ts(lease_until);
        let mut claimed = Vec::with_capacity(limit.min(32) as usize);

        for _ in 0..limit {
            let token = LeaseToken::new();
            let row = sqlx::query(
                "UPDATE jobs
                 SET state = 'running',
                     worker_id = ?1,
                     lease_token = ?2,
                     lease_expires_at = ?3,
                     heartbeat_at = ?4,
                     started_at = COALESCE(started_at, ?4),
                     updated_at = ?4
                 WHERE id = (
                     SELECT id FROM jobs
                     WHERE state = 'queued' AND available_at <= ?4
                     ORDER BY priority DESC, available_at ASC, created_at ASC
                     LIMIT 1
                 ) AND state = 'queued'
                 RETURNING *",
            )
            .bind(worker_id)
            .bind(token.to_string())
            .bind(&lease_ts)
            .bind(&now_ts)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?;

            match row {
                Some(r) => claimed.push(row_to_job(&r)?),
                None => break,
            }
        }
        Ok(claimed)
    }

    async fn heartbeat(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    ) -> Result<JobRecord, StorageError> {
        let row = sqlx::query(
            "UPDATE jobs
             SET heartbeat_at = ?3,
                 lease_expires_at = ?4,
                 updated_at = ?3
             WHERE id = ?1 AND lease_token = ?2 AND state IN ('running', 'cancel_requested')
             RETURNING *",
        )
        .bind(job_id.to_string())
        .bind(lease_token.to_string())
        .bind(fmt_ts(now))
        .bind(fmt_ts(lease_until))
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        match row {
            Some(r) => row_to_job(&r),
            None => Err(self
                .classify_lease_update_miss(job_id, lease_token, JobState::Running)
                .await),
        }
    }

    async fn ack_success(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        success: ExecutionSuccess,
    ) -> Result<JobRecord, StorageError> {
        let finished_ts = fmt_ts(success.finished_at);
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;

        let row = sqlx::query(
            "UPDATE jobs
             SET state = 'succeeded',
                 attempt = attempt + 1,
                 completed_at = ?3,
                 updated_at = ?3,
                 next_retry_at = NULL,
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 exit_code = ?4,
                 stdout_preview = ?5,
                 stderr_preview = ?6,
                 error_class = NULL,
                 error_summary = NULL
             WHERE id = ?1 AND lease_token = ?2 AND state IN ('running', 'cancel_requested')
             RETURNING *",
        )
        .bind(job_id.to_string())
        .bind(lease_token.to_string())
        .bind(&finished_ts)
        .bind(i64::from(success.exit_code))
        .bind(&success.stdout_preview)
        .bind(&success.stderr_preview)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        let Some(row) = row else {
            tx.rollback().await.map_err(map_sqlx_err)?;
            return Err(self
                .classify_lease_update_miss(job_id, lease_token, JobState::Succeeded)
                .await);
        };

        let job = row_to_job(&row)?;
        let started_ts = fmt_ts(
            job.heartbeat_at
                .or(job.started_at)
                .unwrap_or(success.finished_at),
        );
        sqlx::query(
            "INSERT INTO job_attempts (
                job_id, attempt, worker_id, started_at, finished_at,
                exit_code, error_class, error_summary, stdout_preview, stderr_preview
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, ?7, ?8)",
        )
        .bind(job.id.to_string())
        .bind(i64::from(job.attempt))
        .bind(job.worker_id.as_deref().unwrap_or("unknown"))
        .bind(&started_ts)
        .bind(&finished_ts)
        .bind(i64::from(success.exit_code))
        .bind(&success.stdout_preview)
        .bind(&success.stderr_preview)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(job)
    }

    async fn ack_failure(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        failure: ExecutionFailure,
    ) -> Result<JobRecord, StorageError> {
        let target_state = failure.disposition.target_state();
        let finished_ts = fmt_ts(failure.finished_at);
        let next_retry_ts = match failure.disposition {
            FailureDisposition::RetryWait { next_retry_at } => Some(fmt_ts(next_retry_at)),
            FailureDisposition::Failed | FailureDisposition::DeadLetter => None,
        };
        let completed_ts = match failure.disposition {
            FailureDisposition::RetryWait { .. } => None,
            FailureDisposition::Failed | FailureDisposition::DeadLetter => {
                Some(finished_ts.clone())
            }
        };

        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;
        let row = sqlx::query(
            "UPDATE jobs
             SET state = ?3,
                 attempt = attempt + 1,
                 completed_at = COALESCE(?4, completed_at),
                 updated_at = ?5,
                 next_retry_at = ?6,
                 available_at = COALESCE(?6, available_at),
                 worker_id = CASE WHEN ?3 = 'retry_wait' THEN NULL ELSE worker_id END,
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 exit_code = ?7,
                 stdout_preview = ?8,
                 stderr_preview = ?9,
                 error_class = ?10,
                 error_summary = ?11
             WHERE id = ?1
               AND lease_token = ?2
               AND (state = 'running' OR (state = 'cancel_requested' AND ?3 != 'retry_wait'))
             RETURNING *",
        )
        .bind(job_id.to_string())
        .bind(lease_token.to_string())
        .bind(target_state.as_str())
        .bind(&completed_ts)
        .bind(&finished_ts)
        .bind(&next_retry_ts)
        .bind(failure.exit_code.map(i64::from))
        .bind(&failure.stdout_preview)
        .bind(&failure.stderr_preview)
        .bind(error_class_str(failure.error_class))
        .bind(&failure.error_summary)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        let Some(row) = row else {
            tx.rollback().await.map_err(map_sqlx_err)?;
            return Err(self
                .classify_lease_update_miss(job_id, lease_token, target_state)
                .await);
        };

        let job = row_to_job(&row)?;
        let started_ts = fmt_ts(
            job.heartbeat_at
                .or(job.started_at)
                .unwrap_or(failure.finished_at),
        );
        sqlx::query(
            "INSERT INTO job_attempts (
                job_id, attempt, worker_id, started_at, finished_at,
                exit_code, error_class, error_summary, stdout_preview, stderr_preview
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        )
        .bind(job.id.to_string())
        .bind(i64::from(job.attempt))
        .bind(job.worker_id.as_deref().unwrap_or("unknown"))
        .bind(&started_ts)
        .bind(&finished_ts)
        .bind(failure.exit_code.map(i64::from))
        .bind(error_class_str(failure.error_class))
        .bind(&failure.error_summary)
        .bind(&failure.stdout_preview)
        .bind(&failure.stderr_preview)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(job)
    }

    async fn ack_cancelled(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        finished_at: DateTime<Utc>,
        summary: Option<String>,
        stdout_preview: Option<String>,
        stderr_preview: Option<String>,
    ) -> Result<JobRecord, StorageError> {
        let finished_ts = fmt_ts(finished_at);
        let summary_text = summary.unwrap_or_else(|| "job cancelled".to_string());
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;

        let row = sqlx::query(
            "UPDATE jobs
             SET state = 'cancelled',
                 attempt = attempt + 1,
                 completed_at = ?3,
                 updated_at = ?3,
                 next_retry_at = NULL,
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 stdout_preview = ?4,
                 stderr_preview = ?5,
                 error_class = 'cancelled',
                 error_summary = ?6
             WHERE id = ?1 AND lease_token = ?2 AND state IN ('running', 'cancel_requested')
             RETURNING *",
        )
        .bind(job_id.to_string())
        .bind(lease_token.to_string())
        .bind(&finished_ts)
        .bind(&stdout_preview)
        .bind(&stderr_preview)
        .bind(&summary_text)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        let Some(row) = row else {
            tx.rollback().await.map_err(map_sqlx_err)?;
            return Err(self
                .classify_lease_update_miss(job_id, lease_token, JobState::Cancelled)
                .await);
        };

        let job = row_to_job(&row)?;
        let started_ts = fmt_ts(job.heartbeat_at.or(job.started_at).unwrap_or(finished_at));
        sqlx::query(
            "INSERT INTO job_attempts (
                job_id, attempt, worker_id, started_at, finished_at,
                exit_code, error_class, error_summary, stdout_preview, stderr_preview
             ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, 'cancelled', ?6, ?7, ?8)",
        )
        .bind(job.id.to_string())
        .bind(i64::from(job.attempt))
        .bind(job.worker_id.as_deref().unwrap_or("unknown"))
        .bind(&started_ts)
        .bind(&finished_ts)
        .bind(&summary_text)
        .bind(&stdout_preview)
        .bind(&stderr_preview)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        tx.commit().await.map_err(map_sqlx_err)?;
        Ok(job)
    }

    async fn promote_due_retries(&self, now: DateTime<Utc>) -> Result<u64, StorageError> {
        let now_ts = fmt_ts(now);
        let res = sqlx::query(
            "UPDATE jobs
             SET state = 'queued',
                 available_at = ?1,
                 next_retry_at = NULL,
                 updated_at = ?1
             WHERE state = 'retry_wait' AND next_retry_at IS NOT NULL AND next_retry_at <= ?1",
        )
        .bind(&now_ts)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(res.rows_affected())
    }

    async fn recover_expired_leases(&self, now: DateTime<Utc>) -> Result<u64, StorageError> {
        // shortcut: lease-expiry recovery returns `running` jobs to `queued` without incrementing
        // `attempt` (worker crash != execution failure); add crash-loop poison detection in Phase 5
        // if jobs repeatedly crash worker processes before acking.
        let now_ts = fmt_ts(now);
        let res = sqlx::query(
            "UPDATE jobs
             SET state = CASE WHEN state = 'cancel_requested' THEN 'cancelled' ELSE 'queued' END,
                 completed_at = CASE WHEN state = 'cancel_requested' THEN ?1 ELSE completed_at END,
                 available_at = CASE WHEN state = 'running' THEN ?1 ELSE available_at END,
                 worker_id = NULL,
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 heartbeat_at = NULL,
                 updated_at = ?1
             WHERE state IN ('running', 'cancel_requested')
               AND lease_expires_at IS NOT NULL
               AND lease_expires_at <= ?1",
        )
        .bind(&now_ts)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;
        Ok(res.rows_affected())
    }

    async fn request_cancel(
        &self,
        job_id: JobId,
        now: DateTime<Utc>,
    ) -> Result<JobRecord, StorageError> {
        let now_ts = fmt_ts(now);
        let row = sqlx::query(
            "UPDATE jobs
             SET state = CASE
                     WHEN state IN ('queued', 'retry_wait') THEN 'cancelled'
                     ELSE 'cancel_requested'
                 END,
                 completed_at = CASE
                     WHEN state IN ('queued', 'retry_wait') THEN ?2
                     ELSE completed_at
                 END,
                 updated_at = ?2
             WHERE id = ?1 AND state IN ('queued', 'retry_wait', 'running', 'cancel_requested')
             RETURNING *",
        )
        .bind(job_id.to_string())
        .bind(&now_ts)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        match row {
            Some(r) => row_to_job(&r),
            None => Err(self.classify_state_miss(job_id, JobState::Cancelled).await),
        }
    }

    async fn retry_job(
        &self,
        job_id: JobId,
        now: DateTime<Utc>,
    ) -> Result<JobRecord, StorageError> {
        let now_ts = fmt_ts(now);
        let row = sqlx::query(
            "UPDATE jobs
             SET state = 'queued',
                 available_at = ?2,
                 next_retry_at = NULL,
                 completed_at = NULL,
                 worker_id = NULL,
                 lease_token = NULL,
                 lease_expires_at = NULL,
                 heartbeat_at = NULL,
                 error_class = NULL,
                 error_summary = NULL,
                 max_attempts = MAX(max_attempts, attempt + 1),
                 updated_at = ?2
             WHERE id = ?1 AND state IN ('failed', 'dead_letter')
             RETURNING *",
        )
        .bind(job_id.to_string())
        .bind(&now_ts)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        match row {
            Some(r) => row_to_job(&r),
            None => Err(self.classify_state_miss(job_id, JobState::Queued).await),
        }
    }

    async fn get_job(&self, job_id: JobId) -> Result<Option<JobRecord>, StorageError> {
        let row = sqlx::query("SELECT * FROM jobs WHERE id = ?1")
            .bind(job_id.to_string())
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        row.as_ref().map(row_to_job).transpose()
    }

    async fn list_attempts(&self, job_id: JobId) -> Result<Vec<JobAttempt>, StorageError> {
        let rows = sqlx::query("SELECT * FROM job_attempts WHERE job_id = ?1 ORDER BY attempt ASC")
            .bind(job_id.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        rows.iter().map(row_to_attempt).collect()
    }

    async fn list_jobs(&self, filter: JobFilter) -> Result<JobPage, StorageError> {
        filter.validate()?;

        let mut count_qb: QueryBuilder<'_, Sqlite> =
            QueryBuilder::new("SELECT COUNT(*) FROM jobs WHERE 1=1");
        if let Some(state) = filter.state {
            count_qb.push(" AND state = ").push_bind(state.as_str());
        }
        if let Some(hook_id) = &filter.hook_id {
            count_qb.push(" AND hook_id = ").push_bind(hook_id);
        }
        let total_i64: i64 = count_qb
            .build_query_scalar()
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        let total = u64::try_from(total_i64).unwrap_or(0);

        let mut list_qb: QueryBuilder<'_, Sqlite> =
            QueryBuilder::new("SELECT * FROM jobs WHERE 1=1");
        if let Some(state) = filter.state {
            list_qb.push(" AND state = ").push_bind(state.as_str());
        }
        if let Some(hook_id) = &filter.hook_id {
            list_qb.push(" AND hook_id = ").push_bind(hook_id);
        }
        let offset_i64 = i64::try_from(filter.offset)
            .map_err(|_| StorageError::InvalidInput("pagination offset overflow".into()))?;
        list_qb
            .push(" ORDER BY created_at DESC, id DESC LIMIT ")
            .push_bind(i64::from(filter.limit))
            .push(" OFFSET ")
            .push_bind(offset_i64);

        let rows = list_qb
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_err)?;
        let items = rows.iter().map(row_to_job).collect::<Result<Vec<_>, _>>()?;

        Ok(JobPage {
            items,
            total,
            limit: filter.limit,
            offset: filter.offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use webhook_async_config::SqliteStorageConfig;
    use webhook_async_core::JobState;
    use webhook_async_storage::{QueueStorage, StorageError};

    use crate::SqliteStorage;

    #[tokio::test]
    async fn applies_wal_foreign_keys_and_enforces_cancel_retry_guard() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("test.db");
        let storage = SqliteStorage::connect(&SqliteStorageConfig {
            database_url: db_path.to_string_lossy().into_owned(),
            wal_mode: true,
            busy_timeout_ms: 5000,
        })
        .await
        .expect("connect");

        let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(&storage.pool)
            .await
            .expect("journal_mode");
        assert_eq!(mode.to_ascii_lowercase(), "wal");

        let fk: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
            .fetch_one(&storage.pool)
            .await
            .expect("foreign_keys");
        assert_eq!(fk, 1);

        let now = chrono::Utc::now();
        let job = webhook_async_storage::conformance::sample_job("guard", now, None);
        let id = job.id;
        storage.enqueue(job, 60).await.expect("enqueue");
        let claimed = storage
            .claim_due("w1", now, now + chrono::Duration::seconds(30), 1)
            .await
            .expect("claim")
            .pop()
            .expect("job");
        let token = claimed.lease_token.expect("token");
        storage.request_cancel(id, now).await.expect("cancel req");

        let invalid = storage
            .ack_failure(
                id,
                token,
                webhook_async_storage::ExecutionFailure {
                    error_class: webhook_async_core::ErrorClass::Transient,
                    error_summary: "err".into(),
                    exit_code: Some(1),
                    stdout_preview: None,
                    stderr_preview: None,
                    disposition: webhook_async_storage::FailureDisposition::RetryWait {
                        next_retry_at: now + chrono::Duration::seconds(5),
                    },
                    finished_at: now,
                },
            )
            .await;
        assert!(matches!(
            invalid,
            Err(StorageError::InvalidTransition {
                from: JobState::CancelRequested,
                to: JobState::RetryWait,
                ..
            })
        ));
    }
}
