# Storage Backends (`docs/STORAGE.md`)

`webhook-async` decouples queue state from storage engines behind the backend-neutral `QueueStorage` trait (`crates/webhook-async-storage`). Synchronous-only deployments do not require any storage backend or database connectivity.

## 1. Backend Selection & Rules

Configure `[storage]` in TOML (`--config`) when running `server --async` or when any hook specifies `async: true`:

- `backend = "sqlite"` — default embedded single-node backend (`webhook-async-storage-sqlite`), active since Phase 3.
- `backend = "valkey"` — preferred distributed backend (`webhook-async-storage-redis`), active in Phase 4.
- `backend = "redis"` — Redis-compatible distributed backend sharing the `webhook-async-storage-redis` protocol adapter, active in Phase 4.

`webhook-async` never silently downgrades or switches backends if the selected backend is misconfigured or unreachable.

## 2. Shared Queue & State Semantics (`QueueStorage`)

All storage implementations must pass the shared 11-scenario conformance suite in `webhook_async_storage::conformance`:

1. **Enqueue & idempotency**: Persists `JobRecord` + immutable `ExecutionSpec` snapshot atomically; deduplicates non-expired `idempotency_key` entries by returning `EnqueueOutcome::Deduplicated`.
2. **Atomic claim**: Transitions `queued` jobs (`available_at <= now`) to `running`, increments `attempt`, and sets unique `worker_id` + `lease_token` + `lease_expires_at` inside a single atomic operation.
3. **Heartbeats & ownership**: Only the active `(worker_id, lease_token)` while in `running` or `cancel_requested` extends `lease_expires_at`. Stale workers receive `StorageError::LeaseLost`.
4. **Ownership-checked acknowledgements**:
   - `ack_success`: transitions `running`/`cancel_requested` -> `succeeded` and appends a `job_attempts` row.
   - `ack_failure`: transitions `running` -> `retry_wait` / `failed` / `dead_letter` (blocks `retry_wait` if an operator already moved the job to `cancel_requested`).
   - `ack_cancelled`: transitions `cancel_requested`/`running` -> `cancelled`.
5. **Retry promotion & expired lease recovery**:
   - `promote_due_retries`: moves `retry_wait` jobs with `next_retry_at <= now` back to `queued`.
   - `recover_expired_leases`: moves stale `running` jobs whose `lease_expires_at <= now` back to `queued` (clearing lease metadata so another worker can reclaim; does not increment `attempt` on crash recovery). `cancel_requested` jobs with expired leases transition to `cancelled`.
6. **Cancellation & manual retry**:
   - `request_cancel`: `queued`/`retry_wait` -> `cancelled` immediately; `running` -> `cancel_requested`.
   - `retry_job`: `failed`/`cancelled`/`dead_letter` -> `queued`, preserving monotonic `attempt` count and ensuring `max_attempts >= attempt + 1`.
7. **Pagination**: Cursor/offset listing (`JobFilter` with `limit` bounded `1..=500`) ordered newest-first (`created_at DESC, id DESC`).

## 3. SQLite Backend (`webhook-async-storage-sqlite`)

- **Connection pragmas**: Enables `foreign_keys = ON`, configures `busy_timeout(busy_timeout_ms)` (default `5000 ms`), and enables `journal_mode = WAL` with `synchronous = NORMAL` for file-backed databases when `wal_mode = true` (default). `:memory:` databases use a single connection (`max_connections = 1`); file-backed databases default to a pool of `4`.
- **Migrations**: Embedded SQLx migrations (`crates/webhook-async-storage-sqlite/migrations/0001_init.sql`) run automatically at `SqliteStorage::connect`.
- **Schema & indexes**: `jobs` and `job_attempts` (with `ON DELETE CASCADE`) plus partial indexes on `queued` (`priority DESC, available_at ASC`), `retry_wait` (`next_retry_at ASC`), `running`/`cancel_requested` lease expiry (`lease_expires_at ASC`), `hook_id`, and non-null `idempotency_key` (`(hook_id, idempotency_key)`).
- **Concurrency model**: Write transactions start immediately (`BEGIN IMMEDIATE` via write-first short SQLx transactions) and never span child-process execution. SQLite serves single-node deployments well; high-concurrency multi-writer distributed deployments should select Valkey or Redis (Phase 4).

### SQLite Backup, Restore, and Crash Recovery

- **Online backup**: Use SQLite's online backup API or `sqlite3 <db_path> "VACUUM INTO '/path/to/backup.db'"` so WAL frames are included consistently without stopping the server. Do not copy only the main `.db` file while omitting `-wal` and `-shm`.
- **Restore**: Stop `webhook-async` processes, replace the `.db` file (removing stale `-wal`/`-shm` files from the old database), and start `webhook-async`.
- **Abrupt termination**: On restart, SQLite replays committed WAL frames automatically; any in-flight `running` job whose lease expires is returned to `queued` by `recover_expired_leases` for at-least-once re-execution.

## 4. Valkey and Redis Backend (`webhook-async-storage-redis` — Phase 4)

- **Shared RESP adapter**: One implementation (`RedisProtocolStorage`) serves both `backend = "valkey"` (`valkey://` / `valkeys://`) and `backend = "redis"` (`redis://` / `rediss://`) over `redis::aio::ConnectionManager` with bounded connect/response timeouts (`5s` / `10s`) and explicit credential redaction in `Debug` and error messages.
- **ZSET + Lua data model** (chosen over Streams so per-job ownership checks, `priority DESC` scheduling, and per-job lease expiry recovery match `QueueStorage` identically across SQLite, Valkey, and Redis):
  - All keys share a hash-tag `{<key_prefix>}:...` so multi-key Lua scripts execute safely on both standalone instances and Redis/Valkey Cluster deployments.
  - Job state & immutable `ExecutionSpec` snapshot live in `{prefix}:job:<id>` (Hash) and `{prefix}:attempts:<id>` (List of JSON `JobAttempt` entries).
  - Priority queues `{prefix}:q:<priority>` (scored by `available_at_ms`) + `{prefix}:priorities` (scored by `-priority`), delayed retries `{prefix}:retry_wait` (scored by `next_retry_at_ms`), active leases `{prefix}:leases` (scored by `lease_expires_at_ms`), state sets `{prefix}:state:<state>`, hook sets `{prefix}:hook:<hex>`, and idempotency keys `{prefix}:idemp:<hex(hook)>:<hex(key)>` are maintained atomically via 10 Lua scripts (`EVALSHA`).
  - `list_jobs` / `stats` note: while queue mutations and claims are ZSET-indexed, dual `(state + hook_id)` filtering and exact `(created_at DESC, id DESC)` sorting currently materialize IDs from the selected ZSET in memory (`shortcut:` documented in `crates/webhook-async-storage-redis/src/lib.rs`).
- **Persistence & replication assumptions**:
  - Enable `appendonly yes` (`appendfsync everysec` or `always`) or equivalent durable snapshots on Valkey/Redis servers when durability across server restarts is required; in-memory-only server configurations lose queued state on server crash.
  - Deploy against a single primary writer (or managed failover sentinel/primary); asynchronous replication lag during primary failover can violate at-most-one-active-lease visibility until failover settles.
- **Verification**:
  - Unit tests (`cargo test -p webhook-async-storage-redis`) verify config selector validation and credential redaction.
  - Live 11-scenario `QueueStorage` conformance suites (`redis_conformance` and `valkey_conformance`) run automatically when `REDIS_URL` and/or `VALKEY_URL` environment variables are set.
