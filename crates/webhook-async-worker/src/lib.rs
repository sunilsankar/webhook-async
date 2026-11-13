//! Bounded-concurrency asynchronous job worker pool with lease heartbeats,
//! cooperative cancellation, expired lease recovery, and graceful shutdown.

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use chrono::Utc;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore, watch};
use tokio::task::JoinSet;
use uuid::Uuid;
use webhook_async_config::WorkerConfig;
use webhook_async_core::{ErrorClass, JobRecord, JobState, WorkerInfo};
use webhook_async_executor::{ExecutorError, execute_cancellable};
use webhook_async_queue::{QueueCoordinator, QueueError};
use webhook_async_storage::{ExecutionSuccess, StorageError};

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("worker configuration invalid: {0}")]
    InvalidConfig(String),
}

/// In-memory worker registry tracking active worker nodes.
#[derive(Clone, Default)]
pub struct WorkerRegistry {
    workers: Arc<RwLock<BTreeMap<String, WorkerInfo>>>,
}

impl WorkerRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn update(&self, info: WorkerInfo) {
        let mut map = self.workers.write().await;
        map.insert(info.id.clone(), info);
    }

    pub async fn remove(&self, id: &str) {
        let mut map = self.workers.write().await;
        map.remove(id);
    }

    pub async fn list(&self) -> Vec<WorkerInfo> {
        let map = self.workers.read().await;
        map.values().cloned().collect()
    }
}

/// Bounded worker pool that claims due jobs from `QueueCoordinator`, maintains lease heartbeats,
/// supports cooperative cancellation, recovers expired leases, and drains gracefully on shutdown.
#[derive(Clone)]
pub struct WorkerPool {
    worker_id: String,
    coordinator: QueueCoordinator,
    config: WorkerConfig,
    registry: Option<WorkerRegistry>,
}

impl WorkerPool {
    #[must_use]
    pub fn new(coordinator: QueueCoordinator, config: WorkerConfig) -> Self {
        Self {
            worker_id: format!("worker-{}", Uuid::new_v4().simple()),
            coordinator,
            config,
            registry: None,
        }
    }

    #[must_use]
    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = worker_id.into();
        self
    }

    #[must_use]
    pub fn with_registry(mut self, registry: WorkerRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), WorkerError> {
        if self.config.concurrency == 0 {
            return Err(WorkerError::InvalidConfig(
                "worker.concurrency must be >= 1".to_string(),
            ));
        }

        let concurrency = self.config.concurrency as usize;
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut join_set: JoinSet<()> = JoinSet::new();
        let poll_interval = Duration::from_millis(self.config.poll_interval_ms.max(10));
        let sweep_interval = Duration::from_secs((self.config.lease_duration_seconds / 2).max(1));
        let mut last_sweep = tokio::time::Instant::now() - sweep_interval;

        let active_jobs = Arc::new(AtomicU32::new(0));
        let completed_count = Arc::new(AtomicU64::new(0));
        let failed_count = Arc::new(AtomicU64::new(0));
        let hostname = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("HOST"))
            .unwrap_or_else(|_| "localhost".to_string());

        self.sync_registry(
            &hostname,
            &active_jobs,
            &completed_count,
            &failed_count,
            false,
        )
        .await;

        while !*shutdown.borrow() {
            while let Some(res) = join_set.try_join_next() {
                if let Err(err) = res {
                    tracing::error!(error = %err, "worker job task panicked");
                }
            }

            let available = semaphore.available_permits().min(concurrency);
            if available == 0 {
                tokio::select! {
                    _ = wait_for_shutdown(&mut shutdown) => break,
                    Some(res) = join_set.join_next() => {
                        if let Err(err) = res {
                            tracing::error!(error = %err, "worker job task panicked");
                        }
                    }
                }
                continue;
            }

            let now = Utc::now();
            if last_sweep.elapsed() >= sweep_interval {
                self.run_maintenance(now).await;
                self.sync_registry(
                    &hostname,
                    &active_jobs,
                    &completed_count,
                    &failed_count,
                    false,
                )
                .await;
                last_sweep = tokio::time::Instant::now();
            } else if let Err(err) = self.coordinator.storage().promote_due_retries(now).await {
                tracing::warn!(error = %err, "failed to promote due retries");
            }

            let lease_until =
                now + chrono::Duration::seconds(self.config.lease_duration_seconds as i64);
            let claimed = match self
                .coordinator
                .storage()
                .claim_due(&self.worker_id, now, lease_until, available as u32)
                .await
            {
                Ok(jobs) => jobs,
                Err(err) => {
                    tracing::warn!(error = %err, worker_id = %self.worker_id, "claim_due failed");
                    Vec::new()
                }
            };

            let claimed_count = claimed.len();
            for job in claimed {
                let Ok(permit) = Arc::clone(&semaphore).try_acquire_owned() else {
                    tracing::warn!(job_id = %job.id, "semaphore unexpectedly exhausted after claim");
                    break;
                };
                let coordinator = self.coordinator.clone();
                let config = self.config.clone();
                let active = Arc::clone(&active_jobs);
                let completed = Arc::clone(&completed_count);
                let failed = Arc::clone(&failed_count);
                join_set.spawn(async move {
                    run_claimed_job(coordinator, config, job, permit, active, completed, failed)
                        .await;
                });
            }

            if claimed_count > 0 {
                self.sync_registry(
                    &hostname,
                    &active_jobs,
                    &completed_count,
                    &failed_count,
                    false,
                )
                .await;
            }

            if claimed_count == 0 {
                tokio::select! {
                    _ = wait_for_shutdown(&mut shutdown) => break,
                    _ = tokio::time::sleep(poll_interval) => {}
                    Some(res) = join_set.join_next(), if !join_set.is_empty() => {
                        if let Err(err) = res {
                            tracing::error!(error = %err, "worker job task panicked");
                        }
                    }
                }
            }
        }

        // Graceful drain: wait up to shutdown_grace_seconds for in-flight tasks without cancelling
        // valid work; if grace expires, abort remaining tasks so children die via kill_on_drop and
        // un-acked leases can be recovered by another worker.
        self.sync_registry(
            &hostname,
            &active_jobs,
            &completed_count,
            &failed_count,
            true,
        )
        .await;
        let grace = Duration::from_secs(self.config.shutdown_grace_seconds.max(1));
        let drain = async {
            while let Some(res) = join_set.join_next().await {
                if let Err(err) = res {
                    tracing::error!(error = %err, "worker job task panicked during drain");
                }
            }
        };
        if tokio::time::timeout(grace, drain).await.is_err() {
            tracing::warn!(
                worker_id = %self.worker_id,
                remaining = join_set.len(),
                "worker shutdown grace period expired; aborting in-flight tasks"
            );
            join_set.abort_all();
            while join_set.join_next().await.is_some() {}
        }

        if let Some(registry) = &self.registry {
            registry.remove(&self.worker_id).await;
        }

        Ok(())
    }

    async fn sync_registry(
        &self,
        hostname: &str,
        active_jobs: &AtomicU32,
        completed_count: &AtomicU64,
        failed_count: &AtomicU64,
        draining: bool,
    ) {
        if let Some(registry) = &self.registry {
            registry
                .update(WorkerInfo {
                    id: self.worker_id.clone(),
                    hostname: hostname.to_string(),
                    concurrency: self.config.concurrency,
                    active_jobs: active_jobs.load(Ordering::Relaxed),
                    completed_count: completed_count.load(Ordering::Relaxed),
                    failed_count: failed_count.load(Ordering::Relaxed),
                    last_heartbeat_at: Utc::now(),
                    draining,
                })
                .await;
        }
    }

    async fn run_maintenance(&self, now: chrono::DateTime<Utc>) {
        if let Err(err) = self.coordinator.storage().promote_due_retries(now).await {
            tracing::warn!(error = %err, "promote_due_retries failed");
        }
        if let Err(err) = self.coordinator.storage().recover_expired_leases(now).await {
            tracing::warn!(error = %err, "recover_expired_leases failed");
        }
    }
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    let _ = shutdown.wait_for(|&flag| flag).await;
}

struct ActiveJobGuard(Arc<AtomicU32>);
impl Drop for ActiveJobGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn run_claimed_job(
    coordinator: QueueCoordinator,
    config: WorkerConfig,
    job: JobRecord,
    _permit: OwnedSemaphorePermit,
    active_jobs: Arc<AtomicU32>,
    completed_count: Arc<AtomicU64>,
    failed_count: Arc<AtomicU64>,
) {
    active_jobs.fetch_add(1, Ordering::Relaxed);
    let _guard = ActiveJobGuard(Arc::clone(&active_jobs));

    let Some(lease_token) = job.lease_token else {
        tracing::error!(job_id = %job.id, "claimed job missing lease_token");
        failed_count.fetch_add(1, Ordering::Relaxed);
        return;
    };

    if job.state == JobState::CancelRequested {
        failed_count.fetch_add(1, Ordering::Relaxed);
        if let Err(err) = coordinator
            .storage()
            .ack_cancelled(
                job.id,
                lease_token,
                Utc::now(),
                Some("cancelled before execution".to_string()),
                None,
                None,
            )
            .await
        {
            tracing::warn!(job_id = %job.id, error = %err, "ack_cancelled before start failed");
        }
        return;
    }

    let (cancel_tx, cancel_rx) = watch::channel(false);
    let (lost_lease_tx, lost_lease_rx) = watch::channel(false);

    let hb_storage = Arc::clone(coordinator.storage());
    let hb_interval = Duration::from_secs(config.heartbeat_interval_seconds.max(1));
    let lease_secs = config.lease_duration_seconds as i64;
    let job_id = job.id;

    let hb_handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(hb_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // skip immediate tick right after claim
        loop {
            ticker.tick().await;
            let now = Utc::now();
            let lease_until = now + chrono::Duration::seconds(lease_secs);
            match hb_storage
                .heartbeat(job_id, lease_token, lease_until, now)
                .await
            {
                Ok(updated) => {
                    if updated.state == JobState::CancelRequested {
                        let _ = cancel_tx.send(true);
                        break;
                    }
                }
                Err(
                    StorageError::OwnershipMismatch { .. }
                    | StorageError::InvalidTransition { .. }
                    | StorageError::NotFound(_),
                ) => {
                    tracing::warn!(job_id = %job_id, "lost lease ownership during heartbeat");
                    let _ = lost_lease_tx.send(true);
                    let _ = cancel_tx.send(true);
                    break;
                }
                Err(err) => {
                    // Per AGENTS.md §9.4, tolerate transient backend hiccups without killing work.
                    tracing::warn!(job_id = %job_id, error = %err, "transient heartbeat error");
                }
            }
        }
    });

    let exec_res = execute_cancellable(&job.execution_spec, Some(cancel_rx)).await;
    hb_handle.abort();

    if *lost_lease_rx.borrow() {
        tracing::warn!(job_id = %job.id, "skipping ack after lease ownership was lost");
        return;
    }

    let finished_at = Utc::now();
    match exec_res {
        Ok(outcome) => {
            let stdout_preview = outcome.stdout_preview();
            let stderr_preview = outcome.stderr_preview();
            if outcome.cancelled {
                failed_count.fetch_add(1, Ordering::Relaxed);
                ack_cancelled_safe(
                    &coordinator,
                    &job,
                    lease_token,
                    finished_at,
                    Some("cancelled by request".to_string()),
                    stdout_preview,
                    stderr_preview,
                )
                .await;
            } else if outcome.is_success() {
                completed_count.fetch_add(1, Ordering::Relaxed);
                let exit_code = outcome.exit_code.unwrap_or(0);
                if let Err(err) = coordinator
                    .storage()
                    .ack_success(
                        job.id,
                        lease_token,
                        ExecutionSuccess {
                            finished_at,
                            exit_code,
                            stdout_preview,
                            stderr_preview,
                        },
                    )
                    .await
                {
                    tracing::warn!(job_id = %job.id, error = %err, "ack_success failed");
                }
            } else {
                failed_count.fetch_add(1, Ordering::Relaxed);
                let (error_class, summary) = if outcome.timed_out {
                    (
                        ErrorClass::Timeout,
                        format!(
                            "execution timed out after {}s",
                            job.execution_spec.timeout_seconds
                        ),
                    )
                } else {
                    (
                        ErrorClass::Transient,
                        format!("command exited with code {:?}", outcome.exit_code),
                    )
                };
                complete_failure_safe(
                    &coordinator,
                    &job,
                    lease_token,
                    error_class,
                    summary,
                    outcome.exit_code,
                    stdout_preview,
                    stderr_preview,
                    finished_at,
                )
                .await;
            }
        }
        Err(err) => {
            failed_count.fetch_add(1, Ordering::Relaxed);
            let error_class = classify_executor_error(&err);
            complete_failure_safe(
                &coordinator,
                &job,
                lease_token,
                error_class,
                err.to_string(),
                None,
                None,
                None,
                finished_at,
            )
            .await;
        }
    }
}

fn classify_executor_error(err: &ExecutorError) -> ErrorClass {
    match err {
        ExecutorError::InvalidSpec(_) | ExecutorError::InvalidWorkingDir { .. } => {
            ErrorClass::Configuration
        }
        ExecutorError::Spawn { source, .. }
            if matches!(
                source.kind(),
                ErrorKind::NotFound | ErrorKind::PermissionDenied
            ) =>
        {
            ErrorClass::Configuration
        }
        ExecutorError::Spawn { .. } | ExecutorError::TempFile { .. } | ExecutorError::Io(_) => {
            ErrorClass::Transient
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn complete_failure_safe(
    coordinator: &QueueCoordinator,
    job: &JobRecord,
    lease_token: webhook_async_core::LeaseToken,
    error_class: ErrorClass,
    summary: String,
    exit_code: Option<i32>,
    stdout_preview: Option<String>,
    stderr_preview: Option<String>,
    finished_at: chrono::DateTime<Utc>,
) {
    match coordinator
        .complete_failure(
            job,
            lease_token,
            error_class,
            summary.clone(),
            exit_code,
            stdout_preview.clone(),
            stderr_preview.clone(),
            finished_at,
        )
        .await
    {
        Ok(_) => {}
        Err(QueueError::Storage(StorageError::InvalidTransition { .. })) => {
            // A concurrent request_cancel may have moved the DB row to CancelRequested after our
            // last heartbeat, which blocks Transition -> RetryWait. Acknowledge cancellation.
            if let Ok(Some(latest)) = coordinator.storage().get_job(job.id).await
                && latest.state == JobState::CancelRequested
            {
                ack_cancelled_safe(
                    coordinator,
                    job,
                    lease_token,
                    finished_at,
                    Some(format!("cancelled while failing: {summary}")),
                    stdout_preview,
                    stderr_preview,
                )
                .await;
            }
        }
        Err(err) => {
            tracing::warn!(job_id = %job.id, error = %err, "complete_failure failed");
        }
    }
}

async fn ack_cancelled_safe(
    coordinator: &QueueCoordinator,
    job: &JobRecord,
    lease_token: webhook_async_core::LeaseToken,
    now: chrono::DateTime<Utc>,
    summary: Option<String>,
    stdout_preview: Option<String>,
    stderr_preview: Option<String>,
) {
    if let Err(err) = coordinator
        .storage()
        .ack_cancelled(
            job.id,
            lease_token,
            now,
            summary,
            stdout_preview,
            stderr_preview,
        )
        .await
    {
        tracing::warn!(job_id = %job.id, error = %err, "ack_cancelled failed");
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::Utc;
    use tokio::sync::watch;
    use webhook_async_config::{QueueConfig, SqliteStorageConfig, WorkerConfig};
    use webhook_async_core::{ExecutionSpec, JobState};
    use webhook_async_queue::{EnqueueMetadata, QueueCoordinator};
    use webhook_async_storage::QueueStorage;
    use webhook_async_storage_sqlite::SqliteStorage;

    use crate::{WorkerPool, WorkerRegistry};

    async fn setup_pool(
        queue_cfg: QueueConfig,
        worker_cfg: WorkerConfig,
    ) -> (tempfile::TempDir, QueueCoordinator, WorkerPool) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("worker.db").to_string_lossy().into_owned();
        let storage = SqliteStorage::connect(&SqliteStorageConfig {
            database_url: db_path,
            wal_mode: true,
            busy_timeout_ms: 5000,
        })
        .await
        .expect("sqlite");
        let coord = QueueCoordinator::new(Arc::new(storage), queue_cfg);
        let pool = WorkerPool::new(coord.clone(), worker_cfg).with_worker_id("test-worker");
        (dir, coord, pool)
    }

    fn spec(executable: &str, args: &[&str]) -> ExecutionSpec {
        ExecutionSpec {
            hook_id: "test-hook".into(),
            executable: executable.into(),
            args: args.iter().map(|s| (*s).to_string()).collect(),
            env: BTreeMap::new(),
            working_dir: None,
            temp_files: Vec::new(),
            timeout_seconds: 10,
            capture_output: true,
            max_output_bytes: 4096,
            config_version: "sha256:test".into(),
        }
    }

    fn meta(id: &str) -> EnqueueMetadata {
        EnqueueMetadata {
            request_id: id.into(),
            request_method: "POST".into(),
            remote_addr: None,
            idempotency_key: None,
            priority: 0,
            max_attempts: None,
        }
    }

    async fn wait_for_state(
        storage: &Arc<dyn QueueStorage>,
        job_id: webhook_async_core::JobId,
        expected: JobState,
    ) -> webhook_async_core::JobRecord {
        for _ in 0..100 {
            let job = storage
                .get_job(job_id)
                .await
                .expect("get_job")
                .expect("job exists");
            if job.state == expected {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let job = storage
            .get_job(job_id)
            .await
            .expect("get_job")
            .expect("job exists");
        panic!(
            "timed out waiting for state {expected:?}, got {:?}",
            job.state
        );
    }

    #[tokio::test]
    async fn processes_multiple_jobs_within_bounded_concurrency() {
        let worker_cfg = WorkerConfig {
            concurrency: 2,
            poll_interval_ms: 20,
            lease_duration_seconds: 10,
            heartbeat_interval_seconds: 1,
            shutdown_grace_seconds: 5,
        };
        let (_dir, coord, pool) = setup_pool(QueueConfig::default(), worker_cfg).await;
        let now = Utc::now();

        let mut ids = Vec::new();
        for i in 0..4 {
            let j = coord
                .enqueue(
                    spec("/bin/echo", &[&format!("msg-{i}")]),
                    meta(&format!("r-{i}")),
                    now,
                )
                .await
                .expect("enqueue")
                .into_job();
            ids.push(j.id);
        }

        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move { pool.run(rx).await });

        for id in ids {
            let done = wait_for_state(coord.storage(), id, JobState::Succeeded).await;
            assert_eq!(done.exit_code, Some(0));
            assert!(
                done.stdout_preview
                    .as_deref()
                    .unwrap_or("")
                    .contains("msg-")
            );
        }

        let _ = tx.send(true);
        handle.await.expect("join").expect("pool run");
    }

    #[tokio::test]
    async fn missing_executable_dead_letters_immediately_without_retries() {
        let worker_cfg = WorkerConfig {
            concurrency: 1,
            poll_interval_ms: 20,
            lease_duration_seconds: 10,
            heartbeat_interval_seconds: 1,
            shutdown_grace_seconds: 5,
        };
        let (_dir, coord, pool) = setup_pool(QueueConfig::default(), worker_cfg).await;
        let job = coord
            .enqueue(
                spec("/nonexistent/webhook-async-missing-bin", &[]),
                meta("r-missing"),
                Utc::now(),
            )
            .await
            .expect("enqueue")
            .into_job();

        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move { pool.run(rx).await });

        let dead = wait_for_state(coord.storage(), job.id, JobState::DeadLetter).await;
        assert_eq!(dead.attempt, 1);

        let _ = tx.send(true);
        handle.await.expect("join").expect("pool run");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn heartbeat_detects_cancel_requested_and_terminates_running_job() {
        let worker_cfg = WorkerConfig {
            concurrency: 1,
            poll_interval_ms: 20,
            lease_duration_seconds: 10,
            heartbeat_interval_seconds: 1,
            shutdown_grace_seconds: 5,
        };
        let (_dir, coord, pool) = setup_pool(QueueConfig::default(), worker_cfg).await;
        let job = coord
            .enqueue(
                spec("/bin/sh", &["-c", "sleep 30"]),
                meta("r-cancel"),
                Utc::now(),
            )
            .await
            .expect("enqueue")
            .into_job();

        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move { pool.run(rx).await });

        wait_for_state(coord.storage(), job.id, JobState::Running).await;
        let requested = coord
            .cancel_job(job.id, Utc::now())
            .await
            .expect("cancel_job");
        assert_eq!(requested.state, JobState::CancelRequested);

        let cancelled = wait_for_state(coord.storage(), job.id, JobState::Cancelled).await;
        assert!(cancelled.completed_at.is_some());

        let _ = tx.send(true);
        handle.await.expect("join").expect("pool run");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn retries_transient_failure_then_succeeds() {
        let queue_cfg = QueueConfig {
            retry_base_delay_seconds: 1,
            retry_max_delay_seconds: 1,
            default_max_attempts: 3,
            ..Default::default()
        };
        let worker_cfg = WorkerConfig {
            concurrency: 1,
            poll_interval_ms: 20,
            lease_duration_seconds: 10,
            heartbeat_interval_seconds: 1,
            shutdown_grace_seconds: 5,
        };
        let (dir, coord, pool) = setup_pool(queue_cfg, worker_cfg).await;
        let marker = dir.path().join("marker");
        let script = format!(
            "if [ -f '{m}' ]; then echo recovered; exit 0; else touch '{m}'; exit 1; fi",
            m = marker.display()
        );
        let job = coord
            .enqueue(
                spec("/bin/sh", &["-c", &script]),
                meta("r-retry"),
                Utc::now(),
            )
            .await
            .expect("enqueue")
            .into_job();

        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move { pool.run(rx).await });

        let done = wait_for_state(coord.storage(), job.id, JobState::Succeeded).await;
        assert_eq!(done.attempt, 2);
        assert!(
            done.stdout_preview
                .as_deref()
                .unwrap_or("")
                .contains("recovered")
        );

        let _ = tx.send(true);
        handle.await.expect("join").expect("pool run");
    }

    #[tokio::test]
    async fn tracks_workers_in_registry() {
        let registry = WorkerRegistry::new();
        assert!(registry.list().await.is_empty());

        let queue_cfg = QueueConfig::default();
        let worker_cfg = WorkerConfig {
            concurrency: 2,
            poll_interval_ms: 20,
            lease_duration_seconds: 10,
            heartbeat_interval_seconds: 1,
            shutdown_grace_seconds: 1,
        };
        let (_dir, _coord, pool) = setup_pool(queue_cfg, worker_cfg).await;
        let pool = pool.with_registry(registry.clone());

        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move { pool.run(rx).await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        let workers = registry.list().await;
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].concurrency, 2);
        assert!(!workers[0].draining);

        let _ = tx.send(true);
        handle.await.expect("join").expect("pool run");

        let workers_after = registry.list().await;
        assert!(workers_after.is_empty());
    }
}
