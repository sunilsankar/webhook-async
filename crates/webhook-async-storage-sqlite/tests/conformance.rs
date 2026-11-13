use std::sync::Arc;

use async_trait::async_trait;
use tempfile::TempDir;
use webhook_async_config::SqliteStorageConfig;
use webhook_async_storage::QueueStorage;
use webhook_async_storage::conformance::{StorageRestartHandle, run_all};
use webhook_async_storage_sqlite::SqliteStorage;

struct SqliteTestHandle {
    _dir: TempDir,
    db_path: String,
}

impl SqliteTestHandle {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join("conformance.db")
            .to_string_lossy()
            .into_owned();
        Self { _dir: dir, db_path }
    }
}

#[async_trait]
impl StorageRestartHandle for SqliteTestHandle {
    async fn open(&self) -> Arc<dyn QueueStorage> {
        let storage = SqliteStorage::connect(&SqliteStorageConfig {
            database_url: self.db_path.clone(),
            wal_mode: true,
            busy_timeout_ms: 5000,
        })
        .await
        .expect("connect sqlite conformance db");
        Arc::new(storage)
    }
}

#[tokio::test]
async fn sqlite_passes_shared_storage_conformance_suite() {
    run_all(|| async { SqliteTestHandle::new() }).await;
}
