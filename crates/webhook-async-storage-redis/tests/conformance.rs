use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;
use webhook_async_config::{RedisProtocolStorageConfig, StorageBackendKind};
use webhook_async_storage::QueueStorage;
use webhook_async_storage::conformance::{StorageRestartHandle, run_all};
use webhook_async_storage_redis::RedisProtocolStorage;

struct RedisTestHandle {
    backend: StorageBackendKind,
    cfg: RedisProtocolStorageConfig,
}

impl RedisTestHandle {
    fn new(backend: StorageBackendKind, url: String) -> Self {
        let prefix = format!("wa_test_{}", Uuid::new_v4().simple());
        let mut cfg = RedisProtocolStorageConfig::from_url(url);
        cfg.key_prefix = prefix;
        Self { backend, cfg }
    }
}

#[async_trait]
impl StorageRestartHandle for RedisTestHandle {
    async fn open(&self) -> Arc<dyn QueueStorage> {
        let storage = RedisProtocolStorage::connect(self.backend, &self.cfg)
            .await
            .expect("connect RESP conformance storage");
        Arc::new(storage)
    }
}

async fn run_live_suite(backend: StorageBackendKind, env_var: &str) {
    let Ok(url) = std::env::var(env_var) else {
        eprintln!("skipping {env_var} live conformance: {env_var} is not set");
        return;
    };
    run_all(|| {
        let url = url.clone();
        async move { RedisTestHandle::new(backend, url) }
    })
    .await;
}

#[tokio::test]
async fn redis_passes_shared_storage_conformance_suite() {
    run_live_suite(StorageBackendKind::Redis, "REDIS_URL").await;
}

#[tokio::test]
async fn valkey_passes_shared_storage_conformance_suite() {
    run_live_suite(StorageBackendKind::Valkey, "VALKEY_URL").await;
}

#[tokio::test]
async fn rejects_sqlite_backend_kind_and_redacts_unavailable_url_credentials() {
    let cfg = RedisProtocolStorageConfig::from_url("valkey://alice:super-secret@127.0.0.1:1/0");

    let err = RedisProtocolStorage::connect(StorageBackendKind::Sqlite, &cfg)
        .await
        .expect_err("sqlite backend must fail");
    assert!(err.to_string().contains("only accepts 'valkey' or 'redis'"));

    let err = RedisProtocolStorage::connect(StorageBackendKind::Valkey, &cfg)
        .await
        .expect_err("closed port must fail");
    let msg = err.to_string();
    assert!(!msg.contains("super-secret"), "leaked secret in: {msg}");

    // Test discrete fields with authentication and closed port
    let discrete_cfg = RedisProtocolStorageConfig {
        url: None,
        host: Some("127.0.0.1".into()),
        port: Some(1),
        database: Some(webhook_async_config::RedisDatabase::Index(2)),
        username: Some("bob".into()),
        password: Some("p@ss:discrete".into()),
        tls: Some(false),
        key_prefix: "test".into(),
        stream_name: "jobs".into(),
        consumer_group: "workers".into(),
    };
    let err = RedisProtocolStorage::connect(StorageBackendKind::Redis, &discrete_cfg)
        .await
        .expect_err("closed port must fail");
    let msg = err.to_string();
    assert!(
        !msg.contains("p@ss:discrete"),
        "leaked discrete password in: {msg}"
    );
    assert!(
        !msg.contains("p%40ss%3Adiscrete"),
        "leaked encoded password in: {msg}"
    );
}
