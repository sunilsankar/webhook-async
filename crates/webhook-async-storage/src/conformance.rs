use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use tokio::task::JoinSet;
use webhook_async_core::{ErrorClass, ExecutionSpec, JobId, JobRecord, JobState, TempFileSpec};

use crate::{
    EnqueueOutcome, ExecutionFailure, ExecutionSuccess, FailureDisposition, JobFilter,
    QueueStorage, StorageError,
};

/// Handle used by `test_restart_recovery` to drop and reopen the backing store on the same
/// durable path/instance.
#[async_trait]
pub trait StorageRestartHandle: Send + Sync {
    async fn open(&self) -> Arc<dyn QueueStorage>;
}

pub fn sample_job(hook_id: &str, now: DateTime<Utc>, idempotency_key: Option<&str>) -> JobRecord {
    JobRecord {
        id: JobId::new(),
        hook_id: hook_id.to_string(),
        state: JobState::Queued,
        priority: 0,
        created_at: now,
        available_at: now,
        started_at: None,
        completed_at: None,
        updated_at: now,
        attempt: 0,
        max_attempts: 3,
        next_retry_at: None,
        idempotency_key: idempotency_key.map(str::to_string),
        worker_id: None,
        lease_token: None,
        lease_expires_at: None,
        heartbeat_at: None,
        execution_spec: ExecutionSpec {
            hook_id: hook_id.to_string(),
            executable: "/bin/echo".to_string(),
            args: vec!["hello".to_string()],
            env: BTreeMap::from([("HOOK_ENV".to_string(), "1".to_string())]),
            working_dir: None,
            temp_files: vec![TempFileSpec {
                env_name: "HOOK_FILE".to_string(),
                content: b"payload".to_vec(),
            }],
            timeout_seconds: 30,
            capture_output: true,
            max_output_bytes: 16_384,
            config_version: "sha256:test".to_string(),
        },
        request_id: "req-123".to_string(),
        request_method: "POST".to_string(),
        remote_addr: Some("127.0.0.1:4321".to_string()),
        exit_code: None,
        stdout_preview: None,
        stderr_preview: None,
        error_class: None,
        error_summary: None,
    }
}

/// Runs the full `AGENTS.md` §10.3 storage conformance suite against isolated backend instances.
pub async fn run_all<F, Fut, H>(make_handle: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = H>,
    H: StorageRestartHandle + 'static,
{
    test_enqueue_and_get(make_handle().await.open().await.as_ref()).await;
    test_concurrent_claim(make_handle().await.open().await).await;
    test_lease_extension(make_handle().await.open().await.as_ref()).await;
    test_ownership_checked_ack(make_handle().await.open().await.as_ref()).await;
    test_retry_scheduling(make_handle().await.open().await.as_ref()).await;
    test_idempotency(make_handle().await.open().await.as_ref()).await;
    test_cancellation(make_handle().await.open().await.as_ref()).await;
    test_dead_letter_and_manual_retry(make_handle().await.open().await.as_ref()).await;
    test_pagination(make_handle().await.open().await.as_ref()).await;
    test_expired_lease_recovery(make_handle().await.open().await.as_ref()).await;
    test_restart_recovery(&make_handle().await).await;
}

pub async fn test_enqueue_and_get(storage: &dyn QueueStorage) {
    storage.ping().await.expect("ping succeeds");
    let now = Utc::now();
    let job = sample_job("deploy", now, None);
    let job_id = job.id;

    let outcome = storage
        .enqueue(job.clone(), 3600)
        .await
        .expect("enqueue succeeds");
    assert!(!outcome.is_deduplicated());
    assert_eq!(outcome.job().id, job_id);
    assert_eq!(outcome.job().state, JobState::Queued);

    let fetched = storage
        .get_job(job_id)
        .await
        .expect("get_job succeeds")
        .expect("job exists");
    assert_eq!(fetched.execution_spec, job.execution_spec);
    assert_eq!(storage.count_pending().await.expect("count_pending"), 1);

    let stats = storage.stats().await.expect("stats");
    assert_eq!(stats.pending_count, 1);
    assert_eq!(stats.counts_by_state.get("queued"), Some(&1));
}

pub async fn test_concurrent_claim(storage: Arc<dyn QueueStorage>) {
    let now = Utc::now();
    let total_jobs = 12usize;
    for i in 0..total_jobs {
        let mut job = sample_job(&format!("hook-{i}"), now, None);
        job.priority = (i % 3) as i32;
        storage.enqueue(job, 3600).await.expect("enqueue");
    }

    let lease_until = now + Duration::seconds(60);
    let mut workers = JoinSet::new();
    for w in 0..6usize {
        let st = Arc::clone(&storage);
        workers.spawn(async move {
            let mut claimed = Vec::new();
            loop {
                let batch = st
                    .claim_due(&format!("worker-{w}"), now, lease_until, 2)
                    .await
                    .expect("claim_due");
                if batch.is_empty() {
                    break;
                }
                claimed.extend(batch);
            }
            claimed
        });
    }

    let mut seen_ids = HashSet::new();
    let mut seen_tokens = HashSet::new();
    while let Some(res) = workers.join_next().await {
        for job in res.expect("worker task joined") {
            assert_eq!(job.state, JobState::Running);
            let token = job.lease_token.expect("claimed job has lease token");
            assert!(
                seen_ids.insert(job.id),
                "duplicate claim for job {}",
                job.id
            );
            assert!(seen_tokens.insert(token), "duplicate lease token {token}");
        }
    }
    assert_eq!(seen_ids.len(), total_jobs);
}

pub async fn test_lease_extension(storage: &dyn QueueStorage) {
    let now = Utc::now();
    let job = sample_job("build", now, None);
    let job_id = job.id;
    storage.enqueue(job, 3600).await.expect("enqueue");

    let lease1 = now + Duration::seconds(30);
    let claimed = storage
        .claim_due("worker-a", now, lease1, 1)
        .await
        .expect("claim")
        .pop()
        .expect("claimed job");
    let token = claimed.lease_token.expect("lease token");

    let lease2 = now + Duration::seconds(90);
    let renewed = storage
        .heartbeat(job_id, token, now + Duration::seconds(10), lease2)
        .await
        .expect("heartbeat extends lease");
    assert_eq!(renewed.lease_token, Some(token));
    assert_eq!(renewed.lease_expires_at, Some(lease2));
}

pub async fn test_ownership_checked_ack(storage: &dyn QueueStorage) {
    let now = Utc::now();
    let job = sample_job("ownership", now, None);
    let job_id = job.id;
    storage.enqueue(job, 3600).await.expect("enqueue");

    let first_expiry = now + Duration::seconds(20);
    let first_claim = storage
        .claim_due("worker-stale", now, first_expiry, 1)
        .await
        .expect("first claim")
        .pop()
        .expect("job");
    let stale_token = first_claim.lease_token.expect("stale token");

    let after_expiry = first_expiry + Duration::seconds(1);
    assert_eq!(
        storage
            .recover_expired_leases(after_expiry)
            .await
            .expect("recover"),
        1
    );

    let second_claim = storage
        .claim_due(
            "worker-fresh",
            after_expiry,
            after_expiry + Duration::seconds(60),
            1,
        )
        .await
        .expect("second claim")
        .pop()
        .expect("reclaimed job");
    let fresh_token = second_claim.lease_token.expect("fresh token");
    assert_ne!(stale_token, fresh_token);

    let stale_hb = storage
        .heartbeat(
            job_id,
            stale_token,
            after_expiry,
            after_expiry + Duration::seconds(30),
        )
        .await;
    assert!(matches!(
        stale_hb,
        Err(StorageError::OwnershipMismatch { .. })
    ));

    let stale_ack = storage
        .ack_success(
            job_id,
            stale_token,
            ExecutionSuccess {
                exit_code: 0,
                stdout_preview: Some("stale".into()),
                stderr_preview: None,
                finished_at: after_expiry,
            },
        )
        .await;
    assert!(matches!(
        stale_ack,
        Err(StorageError::OwnershipMismatch { .. })
    ));

    let stale_fail = storage
        .ack_failure(
            job_id,
            stale_token,
            ExecutionFailure {
                error_class: ErrorClass::Transient,
                error_summary: "stale".into(),
                exit_code: Some(1),
                stdout_preview: None,
                stderr_preview: None,
                disposition: FailureDisposition::Failed,
                finished_at: after_expiry,
            },
        )
        .await;
    assert!(matches!(
        stale_fail,
        Err(StorageError::OwnershipMismatch { .. })
    ));

    let done = storage
        .ack_success(
            job_id,
            fresh_token,
            ExecutionSuccess {
                exit_code: 0,
                stdout_preview: Some("fresh".into()),
                stderr_preview: None,
                finished_at: after_expiry + Duration::seconds(1),
            },
        )
        .await
        .expect("fresh worker ack succeeds");
    assert_eq!(done.state, JobState::Succeeded);
    assert_eq!(done.attempt, 1);
    assert_eq!(done.stdout_preview.as_deref(), Some("fresh"));
}

pub async fn test_retry_scheduling(storage: &dyn QueueStorage) {
    let now = Utc::now();
    let job = sample_job("retry-hook", now, None);
    let job_id = job.id;
    storage.enqueue(job, 3600).await.expect("enqueue");

    let claimed = storage
        .claim_due("worker-1", now, now + Duration::seconds(30), 1)
        .await
        .expect("claim")
        .pop()
        .expect("job");
    let token = claimed.lease_token.expect("token");

    let retry_at = now + Duration::seconds(45);
    let retried = storage
        .ack_failure(
            job_id,
            token,
            ExecutionFailure {
                error_class: ErrorClass::Transient,
                error_summary: "exit code 1".into(),
                exit_code: Some(1),
                stdout_preview: None,
                stderr_preview: Some("boom".into()),
                disposition: FailureDisposition::RetryWait {
                    next_retry_at: retry_at,
                },
                finished_at: now + Duration::seconds(2),
            },
        )
        .await
        .expect("ack_failure -> retry_wait");
    assert_eq!(retried.state, JobState::RetryWait);
    assert_eq!(retried.attempt, 1);
    assert_eq!(retried.next_retry_at, Some(retry_at));

    // Not due yet
    assert_eq!(
        storage
            .promote_due_retries(retry_at - Duration::seconds(1))
            .await
            .expect("promote before due"),
        0
    );
    assert!(
        storage
            .claim_due(
                "worker-1",
                retry_at - Duration::seconds(1),
                retry_at + Duration::seconds(30),
                1,
            )
            .await
            .expect("claim before due")
            .is_empty()
    );

    // Due now
    assert_eq!(
        storage
            .promote_due_retries(retry_at)
            .await
            .expect("promote when due"),
        1
    );
    let second = storage
        .claim_due("worker-2", retry_at, retry_at + Duration::seconds(30), 1)
        .await
        .expect("claim after promotion")
        .pop()
        .expect("requeued job");
    assert_eq!(second.id, job_id);
    assert_eq!(second.attempt, 1);

    let attempts = storage.list_attempts(job_id).await.expect("attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].attempt, 1);
    assert_eq!(attempts[0].error_class, Some(ErrorClass::Transient));
}

pub async fn test_idempotency(storage: &dyn QueueStorage) {
    let now = Utc::now();
    let j1 = sample_job("idem", now, Some("evt-abc"));
    let first = storage.enqueue(j1, 60).await.expect("first enqueue");
    assert!(!first.is_deduplicated());

    let mut j2 = sample_job("idem", now + Duration::seconds(10), Some("evt-abc"));
    j2.request_id = "req-dup".into();
    let second = storage.enqueue(j2, 60).await.expect("duplicate enqueue");
    assert!(matches!(second, EnqueueOutcome::Deduplicated(_)));
    assert_eq!(second.job().id, first.job().id);

    // After retention expires, the same key is accepted as a new job.
    let after_ttl = now + Duration::seconds(61);
    let j3 = sample_job("idem", after_ttl, Some("evt-abc"));
    let j3_id = j3.id;
    let third = storage
        .enqueue(j3, 60)
        .await
        .expect("enqueue after retention");
    assert!(!third.is_deduplicated());
    assert_eq!(third.job().id, j3_id);
}

pub async fn test_cancellation(storage: &dyn QueueStorage) {
    let now = Utc::now();

    // 1. Queued -> Cancelled immediately
    let q_job = sample_job("cancel-q", now, None);
    let q_id = q_job.id;
    storage.enqueue(q_job, 3600).await.expect("enqueue");
    let cancelled_q = storage
        .request_cancel(q_id, now + Duration::seconds(1))
        .await
        .expect("cancel queued");
    assert_eq!(cancelled_q.state, JobState::Cancelled);
    assert!(cancelled_q.completed_at.is_some());

    // Terminal state rejects subsequent cancel
    let err = storage
        .request_cancel(q_id, now + Duration::seconds(2))
        .await;
    assert!(matches!(err, Err(StorageError::InvalidTransition { .. })));

    // 2. Running -> CancelRequested -> heartbeat observes -> ack_cancelled -> Cancelled
    let r_job = sample_job("cancel-r", now, None);
    let r_id = r_job.id;
    storage.enqueue(r_job, 3600).await.expect("enqueue");
    let claimed = storage
        .claim_due("worker-1", now, now + Duration::seconds(30), 1)
        .await
        .expect("claim")
        .pop()
        .expect("job");
    let token = claimed.lease_token.expect("token");

    let requested = storage
        .request_cancel(r_id, now + Duration::seconds(2))
        .await
        .expect("request_cancel on running");
    assert_eq!(requested.state, JobState::CancelRequested);

    let hb = storage
        .heartbeat(
            r_id,
            token,
            now + Duration::seconds(3),
            now + Duration::seconds(33),
        )
        .await
        .expect("heartbeat surfaces cancel_requested");
    assert_eq!(hb.state, JobState::CancelRequested);

    let done = storage
        .ack_cancelled(
            r_id,
            token,
            now + Duration::seconds(4),
            Some("terminated on cancel".into()),
            None,
            None,
        )
        .await
        .expect("ack_cancelled");
    assert_eq!(done.state, JobState::Cancelled);
    assert_eq!(done.attempt, 1);
}

pub async fn test_dead_letter_and_manual_retry(storage: &dyn QueueStorage) {
    let now = Utc::now();
    let mut job = sample_job("dlq", now, None);
    job.max_attempts = 1;
    let job_id = job.id;
    storage.enqueue(job, 3600).await.expect("enqueue");

    let claimed = storage
        .claim_due("worker-1", now, now + Duration::seconds(30), 1)
        .await
        .expect("claim")
        .pop()
        .expect("job");
    let token = claimed.lease_token.expect("token");

    let dlq = storage
        .ack_failure(
            job_id,
            token,
            ExecutionFailure {
                error_class: ErrorClass::Permanent,
                error_summary: "missing binary".into(),
                exit_code: None,
                stdout_preview: None,
                stderr_preview: None,
                disposition: FailureDisposition::DeadLetter,
                finished_at: now + Duration::seconds(1),
            },
        )
        .await
        .expect("dead_letter");
    assert_eq!(dlq.state, JobState::DeadLetter);
    assert_eq!(dlq.attempt, 1);

    let retried = storage
        .retry_job(job_id, now + Duration::seconds(2))
        .await
        .expect("manual retry from dead_letter");
    assert_eq!(retried.state, JobState::Queued);
    assert!(retried.max_attempts >= 2);
    assert_eq!(retried.attempt, 1);
}

pub async fn test_pagination(storage: &dyn QueueStorage) {
    let base = Utc::now();
    for i in 0..5i64 {
        let hook = if i % 2 == 0 { "even" } else { "odd" };
        let j = sample_job(hook, base + Duration::seconds(i), None);
        storage.enqueue(j, 3600).await.expect("enqueue");
    }

    let bad = storage
        .list_jobs(JobFilter {
            limit: 0,
            ..Default::default()
        })
        .await;
    assert!(matches!(bad, Err(StorageError::InvalidInput(_))));

    let p1 = storage
        .list_jobs(JobFilter {
            state: Some(JobState::Queued),
            hook_id: Some("even".into()),
            limit: 2,
            offset: 0,
        })
        .await
        .expect("page 1");
    assert_eq!(p1.total, 3);
    assert_eq!(p1.items.len(), 2);

    let p2 = storage
        .list_jobs(JobFilter {
            state: Some(JobState::Queued),
            hook_id: Some("even".into()),
            limit: 2,
            offset: 2,
        })
        .await
        .expect("page 2");
    assert_eq!(p2.total, 3);
    assert_eq!(p2.items.len(), 1);
    assert_ne!(p1.items[0].id, p2.items[0].id);
}

pub async fn test_expired_lease_recovery(storage: &dyn QueueStorage) {
    let now = Utc::now();

    let j1 = sample_job("recover-run", now, None);
    let j1_id = j1.id;
    storage.enqueue(j1, 3600).await.expect("enqueue");
    storage
        .claim_due("dead-worker", now, now + Duration::seconds(10), 1)
        .await
        .expect("claim j1");

    let j2 = sample_job("recover-cancel", now, None);
    let j2_id = j2.id;
    storage.enqueue(j2, 3600).await.expect("enqueue");
    storage
        .claim_due("dead-worker", now, now + Duration::seconds(10), 1)
        .await
        .expect("claim j2");
    storage
        .request_cancel(j2_id, now + Duration::seconds(1))
        .await
        .expect("cancel_requested j2");

    assert_eq!(
        storage
            .recover_expired_leases(now + Duration::seconds(11))
            .await
            .expect("recover"),
        2
    );

    let r1 = storage.get_job(j1_id).await.expect("get j1").expect("j1");
    assert_eq!(r1.state, JobState::Queued);
    assert!(r1.lease_token.is_none());
    assert!(r1.worker_id.is_none());

    let r2 = storage.get_job(j2_id).await.expect("get j2").expect("j2");
    assert_eq!(r2.state, JobState::Cancelled);
    assert!(r2.lease_token.is_none());
}

pub async fn test_restart_recovery(handle: &dyn StorageRestartHandle) {
    let now = Utc::now();
    let job_id = {
        let storage = handle.open().await;
        let job = sample_job("restart", now, Some("restart-key"));
        let job_id = job.id;
        storage.enqueue(job, 3600).await.expect("enqueue");
        storage
            .claim_due("crashed-worker", now, now + Duration::seconds(5), 1)
            .await
            .expect("claim");
        job_id
    };

    let reopened = handle.open().await;
    assert_eq!(
        reopened
            .recover_expired_leases(now + Duration::seconds(6))
            .await
            .expect("recover after restart"),
        1
    );
    let recovered = reopened
        .claim_due(
            "new-worker",
            now + Duration::seconds(6),
            now + Duration::seconds(60),
            1,
        )
        .await
        .expect("reclaim after restart")
        .pop()
        .expect("reclaimed job");
    assert_eq!(recovered.id, job_id);
    assert_eq!(recovered.execution_spec.executable, "/bin/echo");
    assert_eq!(recovered.execution_spec.temp_files.len(), 1);
}
