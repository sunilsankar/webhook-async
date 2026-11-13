use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use tower::ServiceExt;
use webhook_async_api::{ApiState, build_router};
use webhook_async_compat::parse_hooks_json;
use webhook_async_config::{
    AppConfig, QueueConfig, SqliteStorageConfig, StorageBackendKind, StorageConfig, WorkerConfig,
};
use webhook_async_core::{GlobalExecutionMode, JobId, JobState};
use webhook_async_queue::QueueCoordinator;
use webhook_async_storage::QueueStorage;
use webhook_async_storage_sqlite::SqliteStorage;
use webhook_async_worker::WorkerPool;

async fn setup_pipeline(
    hooks_json: &str,
    queue_cfg: Option<QueueConfig>,
    worker_cfg: Option<WorkerConfig>,
) -> (
    tempfile::TempDir,
    Arc<SqliteStorage>,
    axum::Router,
    tokio::sync::watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir
        .path()
        .join("pipeline.db")
        .to_string_lossy()
        .into_owned();

    let storage = Arc::new(
        SqliteStorage::connect(&SqliteStorageConfig {
            database_url: db_path.clone(),
            wal_mode: true,
            busy_timeout_ms: 5000,
        })
        .await
        .expect("sqlite connect"),
    );

    let queue = queue_cfg.unwrap_or_default();
    let worker = worker_cfg.unwrap_or(WorkerConfig {
        concurrency: 2,
        poll_interval_ms: 50,
        lease_duration_seconds: 5,
        heartbeat_interval_seconds: 1,
        shutdown_grace_seconds: 2,
    });

    let app = AppConfig {
        storage: Some(StorageConfig {
            backend: StorageBackendKind::Sqlite,
            sqlite: Some(SqliteStorageConfig {
                database_url: format!("sqlite://{db_path}"),
                wal_mode: true,
                busy_timeout_ms: 5000,
            }),
            valkey: None,
            redis: None,
        }),
        queue: queue.clone(),
        worker: worker.clone(),
        ..Default::default()
    };

    let hooks = parse_hooks_json(hooks_json).unwrap();
    let runtime = app
        .build_runtime(hooks, GlobalExecutionMode::Unspecified)
        .unwrap();

    let coord = QueueCoordinator::new(storage.clone(), queue);
    let state = ApiState::with_queue(runtime, Some(coord.clone())).unwrap();
    let router = build_router(state);

    let worker_pool = WorkerPool::new(coord, worker);
    let (tx, rx) = tokio::sync::watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = worker_pool.run(rx).await;
    });

    (dir, storage, router, tx, worker_handle)
}

#[tokio::test]
async fn end_to_end_async_webhook_execution_succeeds() {
    let hooks_json = r#"[{
        "id": "e2e-echo",
        "execute-command": "/bin/echo",
        "async": true,
        "response-message": "queued-echo",
        "pass-arguments-to-command": [
            {"source": "payload", "name": "msg"}
        ]
    }]"#;

    let (_dir, storage, router, shutdown_tx, worker_handle) =
        setup_pipeline(hooks_json, None, None).await;

    let resp = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/hooks/e2e-echo")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"msg":"hello-async-world"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body_bytes = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .unwrap()
        .to_bytes();
    let parsed: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(parsed["status"], "queued");
    assert_eq!(parsed["message"], "queued-echo");
    let job_id: JobId = parsed["job_id"].as_str().unwrap().parse().unwrap();

    // Poll storage until worker claims, executes, and marks job succeeded
    let mut final_job = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Ok(Some(job)) = storage.get_job(job_id).await
            && job.state == JobState::Succeeded
        {
            final_job = Some(job);
            break;
        }
    }

    let job = final_job.expect("job must reach Succeeded state");
    assert_eq!(job.exit_code, Some(0));
    assert_eq!(job.stdout_preview.as_deref(), Some("hello-async-world\n"));

    let _ = shutdown_tx.send(true);
    let _ = worker_handle.await;
}

#[tokio::test]
async fn end_to_end_async_webhook_dead_letters_on_missing_executable() {
    let hooks_json = r#"[{
        "id": "e2e-missing",
        "execute-command": "/nonexistent/webhook_test_binary_missing",
        "async": true
    }]"#;

    let (_dir, storage, router, shutdown_tx, worker_handle) =
        setup_pipeline(hooks_json, None, None).await;

    let resp = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/hooks/e2e-missing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body_bytes = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .unwrap()
        .to_bytes();
    let parsed: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    let job_id: JobId = parsed["job_id"].as_str().unwrap().parse().unwrap();

    // Permanent errors (missing executable) transition immediately to DeadLetter without retries
    let mut final_job = None;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Ok(Some(job)) = storage.get_job(job_id).await
            && job.state == JobState::DeadLetter
        {
            final_job = Some(job);
            break;
        }
    }

    let job = final_job.expect("job must reach DeadLetter state immediately");
    assert_eq!(
        job.error_class,
        Some(webhook_async_core::ErrorClass::Configuration)
    );
    assert_eq!(job.attempt, 1);

    let _ = shutdown_tx.send(true);
    let _ = worker_handle.await;
}

#[cfg(unix)]
#[tokio::test]
async fn end_to_end_async_webhook_cancellation_terminates_child_process() {
    let hooks_json = r#"[{
        "id": "e2e-cancel",
        "execute-command": "/bin/sleep",
        "async": true,
        "pass-arguments-to-command": [
            {"source": "string", "name": "30"}
        ]
    }]"#;

    let worker_cfg = WorkerConfig {
        concurrency: 1,
        poll_interval_ms: 20,
        lease_duration_seconds: 5,
        heartbeat_interval_seconds: 1,
        shutdown_grace_seconds: 2,
    };

    let (_dir, storage, router, shutdown_tx, worker_handle) =
        setup_pipeline(hooks_json, None, Some(worker_cfg)).await;

    let resp = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/hooks/e2e-cancel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let body_bytes = http_body_util::BodyExt::collect(resp.into_body())
        .await
        .unwrap()
        .to_bytes();
    let parsed: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    let job_id: JobId = parsed["job_id"].as_str().unwrap().parse().unwrap();

    // Wait until job is Running
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(30)).await;
        if let Ok(Some(job)) = storage.get_job(job_id).await
            && job.state == JobState::Running
        {
            break;
        }
    }

    // Request cancellation while running
    let now = chrono::Utc::now();
    storage.request_cancel(job_id, now).await.unwrap();

    // Heartbeat loop detects CancelRequested, kills process group, and transitions to Cancelled
    let mut final_job = None;
    for _ in 0..60 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        if let Ok(Some(job)) = storage.get_job(job_id).await
            && job.state == JobState::Cancelled
        {
            final_job = Some(job);
            break;
        }
    }

    let job = final_job.expect("job must reach Cancelled state within heartbeat window");
    assert_eq!(
        job.error_class,
        Some(webhook_async_core::ErrorClass::Cancelled)
    );

    let _ = shutdown_tx.send(true);
    let _ = worker_handle.await;
}
