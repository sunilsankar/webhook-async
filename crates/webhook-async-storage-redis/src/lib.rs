//! Shared Valkey and Redis RESP storage adapter using ZSET indexes and atomic Lua transitions.

mod keys;
mod mapping;
mod scripts;

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use uuid::Uuid;
use webhook_async_config::{RedisProtocolStorageConfig, StorageBackendKind};
use webhook_async_core::{JobAttempt, JobId, JobRecord, JobState, LeaseToken};
use webhook_async_storage::conformance::StorageRestartHandle;
use webhook_async_storage::{
    BackendCapabilities, EnqueueOutcome, ExecutionFailure, ExecutionSuccess, FailureDisposition,
    JobFilter, JobPage, QueueStats, QueueStorage, StorageError,
};

use keys::{KeyLayout, normalize_resp_url};
use mapping::{
    error_class_str, fmt_ts, hash_map_to_job, hash_pairs_to_job, job_to_hash_fields, parse_state,
    redis_err_to_storage, ts_millis,
};
use scripts::{
    ACK_CANCELLED_SCRIPT, ACK_FAILURE_SCRIPT, ACK_SUCCESS_SCRIPT, CLAIM_DUE_SCRIPT, ENQUEUE_SCRIPT,
    HEARTBEAT_SCRIPT, PROMOTE_RETRIES_SCRIPT, RECOVER_EXPIRED_LEASES_SCRIPT, REQUEST_CANCEL_SCRIPT,
    RETRY_JOB_SCRIPT,
};

#[derive(Clone)]
pub struct RedisProtocolStorage {
    backend: StorageBackendKind,
    config: RedisProtocolStorageConfig,
    layout: KeyLayout,
    conn: ConnectionManager,
}

impl std::fmt::Debug for RedisProtocolStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisProtocolStorage")
            .field("backend", &self.backend)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl RedisProtocolStorage {
    pub async fn connect(
        backend: StorageBackendKind,
        config: &RedisProtocolStorageConfig,
    ) -> Result<Self, StorageError> {
        if backend == StorageBackendKind::Sqlite {
            return Err(StorageError::InvalidInput(
                "RedisProtocolStorage only accepts 'valkey' or 'redis' backend kinds".into(),
            ));
        }
        let resp_url = normalize_resp_url(&config.url);
        let client = redis::Client::open(resp_url.as_str())
            .map_err(|err| redis_err_to_storage(err, &config.url))?;
        let mgr_cfg = redis::aio::ConnectionManagerConfig::new()
            .set_factor(2)
            .set_max_delay(500)
            .set_number_of_retries(2)
            .set_connection_timeout(std::time::Duration::from_secs(5))
            .set_response_timeout(std::time::Duration::from_secs(10));
        let conn = ConnectionManager::new_with_config(client, mgr_cfg)
            .await
            .map_err(|err| redis_err_to_storage(err, &config.url))?;
        Ok(Self {
            backend,
            config: config.clone(),
            layout: KeyLayout::new(&config.key_prefix),
            conn,
        })
    }

    #[must_use]
    pub const fn backend_kind(&self) -> StorageBackendKind {
        self.backend
    }

    /// Removes every key created under `config.key_prefix` (useful in isolated tests).
    pub async fn purge_prefixed_keys(&self) -> Result<(), StorageError> {
        let mut conn = self.conn.clone();
        let pattern = format!("{{{}}}:*", self.config.key_prefix.trim());
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .map_err(|err| redis_err_to_storage(err, &self.config.url))?;
        if !keys.is_empty() {
            let _: () = conn
                .del(keys)
                .await
                .map_err(|err| redis_err_to_storage(err, &self.config.url))?;
        }
        Ok(())
    }

    fn err(&self, err: redis::RedisError) -> StorageError {
        redis_err_to_storage(err, &self.config.url)
    }

    async fn fetch_jobs_by_ids(&self, ids: &[String]) -> Result<Vec<JobRecord>, StorageError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for id in ids {
            if let Ok(parsed_id) = Uuid::parse_str(id) {
                pipe.hgetall(self.layout.job(JobId(parsed_id)));
            }
        }
        let mut conn = self.conn.clone();
        let maps: Vec<HashMap<String, String>> =
            pipe.query_async(&mut conn).await.map_err(|e| self.err(e))?;
        let mut jobs = Vec::with_capacity(maps.len());
        for map in maps {
            if !map.is_empty() {
                jobs.push(hash_map_to_job(&map)?);
            }
        }
        Ok(jobs)
    }
}

pub struct RedisRestartHandle {
    backend: StorageBackendKind,
    config: RedisProtocolStorageConfig,
}

impl RedisRestartHandle {
    #[must_use]
    pub fn new(backend: StorageBackendKind, config: RedisProtocolStorageConfig) -> Self {
        Self { backend, config }
    }
}

#[async_trait]
impl StorageRestartHandle for RedisRestartHandle {
    async fn open(&self) -> Arc<dyn QueueStorage> {
        Arc::new(
            RedisProtocolStorage::connect(self.backend, &self.config)
                .await
                .expect("reopen RESP storage"),
        )
    }
}

#[async_trait]
impl QueueStorage for RedisProtocolStorage {
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            backend: match self.backend {
                StorageBackendKind::Valkey => "valkey",
                StorageBackendKind::Redis => "redis",
                StorageBackendKind::Sqlite => "sqlite",
            },
            supports_priority: true,
            supports_multi_host: true,
            supports_idempotency: true,
        }
    }

    async fn ping(&self) -> Result<(), StorageError> {
        let mut conn = self.conn.clone();
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(e))?;
        if pong == "PONG" {
            Ok(())
        } else {
            Err(StorageError::BackendUnavailable(format!(
                "unexpected PING response '{pong}'"
            )))
        }
    }

    async fn count_pending(&self) -> Result<u64, StorageError> {
        let mut conn = self.conn.clone();
        let (queued, retry_wait): (u64, u64) = redis::pipe()
            .zcard(self.layout.state(JobState::Queued))
            .zcard(self.layout.state(JobState::RetryWait))
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(e))?;
        Ok(queued + retry_wait)
    }

    async fn stats(&self) -> Result<QueueStats, StorageError> {
        let mut conn = self.conn.clone();
        let counts: Vec<u64> = redis::pipe()
            .zcard(self.layout.state(JobState::Queued))
            .zcard(self.layout.state(JobState::Running))
            .zcard(self.layout.state(JobState::RetryWait))
            .zcard(self.layout.state(JobState::Succeeded))
            .zcard(self.layout.state(JobState::Failed))
            .zcard(self.layout.state(JobState::CancelRequested))
            .zcard(self.layout.state(JobState::Cancelled))
            .zcard(self.layout.state(JobState::DeadLetter))
            .query_async(&mut conn)
            .await
            .map_err(|e| self.err(e))?;

        let mut counts_by_state = BTreeMap::new();
        for (state, count) in JobState::ALL.iter().zip(counts.iter()) {
            counts_by_state.insert(state.as_str().to_string(), *count);
        }
        let pending_count = counts_by_state.get("queued").copied().unwrap_or(0)
            + counts_by_state.get("retry_wait").copied().unwrap_or(0);
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
                "enqueue requires Queued state, got {:?}",
                job.state
            )));
        }

        let retention = i64::try_from(idempotency_retention_seconds).unwrap_or(i64::MAX / 2);
        let idemp_expires_at = job.created_at + Duration::seconds(retention);
        let has_idemp = if job.idempotency_key.is_some() {
            "1"
        } else {
            "0"
        };
        let idemp_key = match &job.idempotency_key {
            Some(key) => self.layout.idempotency(&job.hook_id, key),
            None => self.layout.idempotency_unused(),
        };

        let fields = job_to_hash_fields(&job)?;
        let mut inv = ENQUEUE_SCRIPT.prepare_invoke();
        inv.key(self.layout.job(job.id))
            .key(self.layout.all_jobs())
            .key(self.layout.state(JobState::Queued))
            .key(self.layout.hook(&job.hook_id))
            .key(self.layout.priorities())
            .key(self.layout.priority_queue(job.priority))
            .key(idemp_key)
            .arg(job.id.to_string())
            .arg(has_idemp)
            .arg(fmt_ts(job.created_at))
            .arg(fmt_ts(idemp_expires_at))
            .arg(self.layout.job_prefix())
            .arg(ts_millis(job.created_at))
            .arg(ts_millis(job.available_at))
            .arg(job.priority.to_string());
        for (k, v) in &fields {
            inv.arg(k).arg(v);
        }

        let mut conn = self.conn.clone();
        let reply: redis::Value = inv.invoke_async(&mut conn).await.map_err(|e| self.err(e))?;
        parse_enqueue_reply(reply, job)
    }

    async fn claim_due(
        &self,
        worker_id: &str,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<JobRecord>, StorageError> {
        if limit == 0 || worker_id.trim().is_empty() {
            return Err(StorageError::InvalidInput(
                "claim_due requires non-empty worker_id and limit >= 1".into(),
            ));
        }

        let mut inv = CLAIM_DUE_SCRIPT.prepare_invoke();
        inv.key(self.layout.priorities())
            .key(self.layout.state(JobState::Queued))
            .key(self.layout.state(JobState::Running))
            .key(self.layout.leases())
            .arg(ts_millis(now))
            .arg(fmt_ts(now))
            .arg(ts_millis(lease_expires_at))
            .arg(fmt_ts(lease_expires_at))
            .arg(worker_id)
            .arg(self.layout.job_prefix())
            .arg(self.layout.priority_queue_prefix())
            .arg(limit);
        for _ in 0..limit {
            inv.arg(LeaseToken::new().0.to_string());
        }

        let mut conn = self.conn.clone();
        let rows: Vec<Vec<String>> = inv.invoke_async(&mut conn).await.map_err(|e| self.err(e))?;
        rows.into_iter().map(hash_pairs_to_job).collect()
    }

    async fn heartbeat(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<JobRecord, StorageError> {
        let mut conn = self.conn.clone();
        let reply: redis::Value = HEARTBEAT_SCRIPT
            .key(self.layout.job(job_id))
            .key(self.layout.leases())
            .arg(job_id.to_string())
            .arg(lease_token.0.to_string())
            .arg(fmt_ts(now))
            .arg(ts_millis(lease_expires_at))
            .arg(fmt_ts(lease_expires_at))
            .invoke_async(&mut conn)
            .await
            .map_err(|e| self.err(e))?;
        parse_job_script_reply(reply, job_id, Some(lease_token), JobState::Running)
    }

    async fn ack_success(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        success: ExecutionSuccess,
    ) -> Result<JobRecord, StorageError> {
        let (has_out, out_str) = opt_str_flag(success.stdout_preview);
        let (has_err, err_str) = opt_str_flag(success.stderr_preview);
        let mut conn = self.conn.clone();
        let reply: redis::Value = ACK_SUCCESS_SCRIPT
            .key(self.layout.job(job_id))
            .key(self.layout.attempts(job_id))
            .key(self.layout.leases())
            .key(self.layout.state(JobState::Running))
            .key(self.layout.state(JobState::CancelRequested))
            .key(self.layout.state(JobState::Succeeded))
            .arg(job_id.to_string())
            .arg(lease_token.0.to_string())
            .arg(Uuid::new_v4().to_string())
            .arg(fmt_ts(success.finished_at))
            .arg(success.exit_code.to_string())
            .arg(has_out)
            .arg(out_str)
            .arg(has_err)
            .arg(err_str)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| self.err(e))?;
        parse_job_script_reply(reply, job_id, Some(lease_token), JobState::Succeeded)
    }

    async fn ack_failure(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        failure: ExecutionFailure,
    ) -> Result<JobRecord, StorageError> {
        let (target_state, next_retry_rfc, next_retry_ms) = match failure.disposition {
            FailureDisposition::RetryWait { next_retry_at } => (
                JobState::RetryWait,
                fmt_ts(next_retry_at),
                ts_millis(next_retry_at),
            ),
            FailureDisposition::Failed => (JobState::Failed, String::new(), 0),
            FailureDisposition::DeadLetter => (JobState::DeadLetter, String::new(), 0),
        };
        let (has_exit, exit_str) = match failure.exit_code {
            Some(c) => ("1", c.to_string()),
            None => ("0", String::new()),
        };
        let (has_out, out_str) = opt_str_flag(failure.stdout_preview);
        let (has_err, err_str) = opt_str_flag(failure.stderr_preview);

        let mut conn = self.conn.clone();
        let reply: redis::Value = ACK_FAILURE_SCRIPT
            .key(self.layout.job(job_id))
            .key(self.layout.attempts(job_id))
            .key(self.layout.leases())
            .key(self.layout.state(JobState::Running))
            .key(self.layout.state(JobState::CancelRequested))
            .key(self.layout.state(target_state))
            .key(self.layout.retry_wait())
            .arg(job_id.to_string())
            .arg(lease_token.0.to_string())
            .arg(Uuid::new_v4().to_string())
            .arg(target_state.as_str())
            .arg(fmt_ts(failure.finished_at))
            .arg(next_retry_rfc)
            .arg(next_retry_ms)
            .arg(error_class_str(failure.error_class))
            .arg(failure.error_summary)
            .arg(has_exit)
            .arg(exit_str)
            .arg(has_out)
            .arg(out_str)
            .arg(has_err)
            .arg(err_str)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| self.err(e))?;
        parse_job_script_reply(reply, job_id, Some(lease_token), target_state)
    }

    async fn ack_cancelled(
        &self,
        job_id: JobId,
        lease_token: LeaseToken,
        cancelled_at: DateTime<Utc>,
        reason: Option<String>,
        stdout_preview: Option<String>,
        stderr_preview: Option<String>,
    ) -> Result<JobRecord, StorageError> {
        let summary = reason.unwrap_or_else(|| "cancelled".into());
        let (has_out, out_str) = opt_str_flag(stdout_preview);
        let (has_err, err_str) = opt_str_flag(stderr_preview);

        let mut conn = self.conn.clone();
        let reply: redis::Value = ACK_CANCELLED_SCRIPT
            .key(self.layout.job(job_id))
            .key(self.layout.attempts(job_id))
            .key(self.layout.leases())
            .key(self.layout.state(JobState::Running))
            .key(self.layout.state(JobState::CancelRequested))
            .key(self.layout.state(JobState::Cancelled))
            .arg(job_id.to_string())
            .arg(lease_token.0.to_string())
            .arg(Uuid::new_v4().to_string())
            .arg(fmt_ts(cancelled_at))
            .arg(summary)
            .arg(has_out)
            .arg(out_str)
            .arg(has_err)
            .arg(err_str)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| self.err(e))?;
        parse_job_script_reply(reply, job_id, Some(lease_token), JobState::Cancelled)
    }

    async fn promote_due_retries(&self, now: DateTime<Utc>) -> Result<u64, StorageError> {
        let mut conn = self.conn.clone();
        let promoted: u64 = PROMOTE_RETRIES_SCRIPT
            .key(self.layout.retry_wait())
            .key(self.layout.state(JobState::RetryWait))
            .key(self.layout.state(JobState::Queued))
            .key(self.layout.priorities())
            .arg(ts_millis(now))
            .arg(fmt_ts(now))
            .arg(self.layout.job_prefix())
            .arg(self.layout.priority_queue_prefix())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| self.err(e))?;
        Ok(promoted)
    }

    async fn recover_expired_leases(&self, now: DateTime<Utc>) -> Result<u64, StorageError> {
        // shortcut: mirrors SQLite recover_expired_leases by returning expired running jobs to
        // Queued without incrementing `attempt`; synthesize a timeout attempt on recovery if
        // repeated worker crashes should also consume `max_attempts`.
        let mut conn = self.conn.clone();
        let ids: Vec<String> = RECOVER_EXPIRED_LEASES_SCRIPT
            .key(self.layout.leases())
            .key(self.layout.state(JobState::Running))
            .key(self.layout.state(JobState::CancelRequested))
            .key(self.layout.state(JobState::Queued))
            .key(self.layout.state(JobState::Cancelled))
            .key(self.layout.priorities())
            .arg(ts_millis(now))
            .arg(fmt_ts(now))
            .arg(self.layout.job_prefix())
            .arg(self.layout.priority_queue_prefix())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| self.err(e))?;

        Ok(u64::try_from(ids.len()).unwrap_or(u64::MAX))
    }

    async fn request_cancel(
        &self,
        job_id: JobId,
        now: DateTime<Utc>,
    ) -> Result<JobRecord, StorageError> {
        let mut conn = self.conn.clone();
        let reply: redis::Value = REQUEST_CANCEL_SCRIPT
            .key(self.layout.job(job_id))
            .key(self.layout.priorities())
            .key(self.layout.retry_wait())
            .key(self.layout.state(JobState::Queued))
            .key(self.layout.state(JobState::RetryWait))
            .key(self.layout.state(JobState::Running))
            .key(self.layout.state(JobState::CancelRequested))
            .key(self.layout.state(JobState::Cancelled))
            .arg(job_id.to_string())
            .arg(fmt_ts(now))
            .arg(self.layout.priority_queue_prefix())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| self.err(e))?;
        parse_job_script_reply(reply, job_id, None, JobState::Cancelled)
    }

    async fn retry_job(
        &self,
        job_id: JobId,
        now: DateTime<Utc>,
    ) -> Result<JobRecord, StorageError> {
        let mut conn = self.conn.clone();
        let reply: redis::Value = RETRY_JOB_SCRIPT
            .key(self.layout.job(job_id))
            .key(self.layout.priorities())
            .key(self.layout.state(JobState::Failed))
            .key(self.layout.state(JobState::DeadLetter))
            .key(self.layout.state(JobState::Queued))
            .arg(job_id.to_string())
            .arg(ts_millis(now))
            .arg(fmt_ts(now))
            .arg(self.layout.priority_queue_prefix())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| self.err(e))?;
        parse_job_script_reply(reply, job_id, None, JobState::Queued)
    }

    async fn get_job(&self, job_id: JobId) -> Result<Option<JobRecord>, StorageError> {
        let mut conn = self.conn.clone();
        let map: HashMap<String, String> = conn
            .hgetall(self.layout.job(job_id))
            .await
            .map_err(|e| self.err(e))?;
        if map.is_empty() {
            Ok(None)
        } else {
            Ok(Some(hash_map_to_job(&map)?))
        }
    }

    async fn list_attempts(&self, job_id: JobId) -> Result<Vec<JobAttempt>, StorageError> {
        let mut conn = self.conn.clone();
        let exists: bool = conn
            .exists(self.layout.job(job_id))
            .await
            .map_err(|e| self.err(e))?;
        if !exists {
            return Err(StorageError::NotFound(job_id));
        }
        let raw_items: Vec<String> = conn
            .lrange(self.layout.attempts(job_id), 0, -1)
            .await
            .map_err(|e| self.err(e))?;
        raw_items
            .into_iter()
            .map(|raw| {
                serde_json::from_str(&raw).map_err(|err| {
                    StorageError::Internal(format!("corrupted stored attempt: {err}"))
                })
            })
            .collect()
    }

    async fn list_jobs(&self, filter: JobFilter) -> Result<JobPage, StorageError> {
        filter.validate()?;

        // shortcut: scans the narrowest ZSET (state, hook, or all_jobs) into memory for exact
        // (created_at DESC, id DESC) ordering and dual state+hook intersection; replace with
        // composite ZSETs or chunked window scans if job history exceeds tens of thousands.
        let mut conn = self.conn.clone();
        let candidate_ids: Vec<String> = match (filter.state, filter.hook_id.as_deref()) {
            (Some(st), Some(hook)) => {
                let state_ids: Vec<String> = conn
                    .zrevrange(self.layout.state(st), 0, -1)
                    .await
                    .map_err(|e| self.err(e))?;
                let hook_ids: HashSet<String> = conn
                    .zrevrange::<_, Vec<String>>(self.layout.hook(hook), 0, -1)
                    .await
                    .map_err(|e| self.err(e))?
                    .into_iter()
                    .collect();
                state_ids
                    .into_iter()
                    .filter(|id| hook_ids.contains(id))
                    .collect()
            }
            (Some(st), None) => conn
                .zrevrange(self.layout.state(st), 0, -1)
                .await
                .map_err(|e| self.err(e))?,
            (None, Some(hook)) => conn
                .zrevrange(self.layout.hook(hook), 0, -1)
                .await
                .map_err(|e| self.err(e))?,
            (None, None) => conn
                .zrevrange(self.layout.all_jobs(), 0, -1)
                .await
                .map_err(|e| self.err(e))?,
        };

        let mut jobs = self.fetch_jobs_by_ids(&candidate_ids).await?;
        jobs.sort_by_key(|j| Reverse((j.created_at, j.id.0)));

        let total = u64::try_from(jobs.len()).unwrap_or(u64::MAX);
        let offset = usize::try_from(filter.offset).unwrap_or(usize::MAX);
        let limit = usize::try_from(filter.limit).unwrap_or(usize::MAX);
        let items = jobs.into_iter().skip(offset).take(limit).collect();

        Ok(JobPage {
            items,
            total,
            limit: filter.limit,
            offset: filter.offset,
        })
    }
}

fn opt_str_flag(val: Option<String>) -> (&'static str, String) {
    match val {
        Some(s) => ("1", s),
        None => ("0", String::new()),
    }
}

fn parse_enqueue_reply(
    reply: redis::Value,
    created_job: JobRecord,
) -> Result<EnqueueOutcome, StorageError> {
    let items = expect_array(reply)?;
    let tag = expect_string(items.first())?;
    match tag.as_str() {
        "CREATED" => Ok(EnqueueOutcome::Created(Box::new(created_job))),
        "DEDUP" => {
            let pairs: Vec<String> = redis::from_redis_value(
                items
                    .get(1)
                    .ok_or_else(|| StorageError::Internal("missing DEDUP payload".into()))?,
            )
            .map_err(|err| StorageError::Internal(format!("invalid DEDUP payload: {err}")))?;
            Ok(EnqueueOutcome::Deduplicated(Box::new(hash_pairs_to_job(
                pairs,
            )?)))
        }
        "ERR" => Err(StorageError::Internal(format!(
            "enqueue error: {}",
            expect_string(items.get(1))?
        ))),
        other => Err(StorageError::Internal(format!(
            "unexpected enqueue script reply '{other}'"
        ))),
    }
}

fn parse_job_script_reply(
    reply: redis::Value,
    job_id: JobId,
    token: Option<LeaseToken>,
    attempted_to: JobState,
) -> Result<JobRecord, StorageError> {
    let items = expect_array(reply)?;
    let tag = expect_string(items.first())?;
    match tag.as_str() {
        "OK" => {
            let pairs: Vec<String> = redis::from_redis_value(
                items
                    .get(1)
                    .ok_or_else(|| StorageError::Internal("missing OK job payload".into()))?,
            )
            .map_err(|err| StorageError::Internal(format!("invalid OK job payload: {err}")))?;
            hash_pairs_to_job(pairs)
        }
        "ERR" => match expect_string(items.get(1))?.as_str() {
            "NOT_FOUND" => Err(StorageError::NotFound(job_id)),
            "OWNERSHIP_MISMATCH" => Err(StorageError::OwnershipMismatch {
                job_id,
                token: token.unwrap_or_else(|| LeaseToken(Uuid::nil())),
            }),
            "INVALID_TRANSITION" => {
                let raw_state = expect_string(items.get(2))?;
                let from = parse_state(&raw_state)?;
                Err(StorageError::InvalidTransition {
                    job_id,
                    from,
                    to: attempted_to,
                })
            }
            other => Err(StorageError::Internal(format!("script error: {other}"))),
        },
        other => Err(StorageError::Internal(format!(
            "unexpected script reply '{other}'"
        ))),
    }
}

fn expect_array(val: redis::Value) -> Result<Vec<redis::Value>, StorageError> {
    match val {
        redis::Value::Array(items) => Ok(items),
        other => Err(StorageError::Internal(format!(
            "expected array script reply, got {other:?}"
        ))),
    }
}

fn expect_string(val: Option<&redis::Value>) -> Result<String, StorageError> {
    let v = val.ok_or_else(|| StorageError::Internal("missing script status tag".into()))?;
    redis::from_redis_value(v)
        .map_err(|err| StorageError::Internal(format!("invalid script status tag: {err}")))
}
