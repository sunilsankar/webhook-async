# Architecture (`webhook-async`)

## 1. Crate Dependency Graph

Dependencies strictly flow inward toward `webhook-async-core`. Domain types (`HookDefinition`, `JobState`, `ExecutionSpec`, `JobRecord`, `CoreError`) never depend on Axum, SQLx, Redis/Valkey clients, Askama, or CLI crates.

```
                         +--------------------+
                         | webhook-async-cli  |
                         +---------+----------+
                                   |
        +--------------------------+--------------------------+
        |                          |                          |
        v                          v                          v
+---------------+          +---------------+          +---------------+
| webhook-async |          | webhook-async |          | webhook-async |
|     -api      |          |     -web      |          |    -worker    |
+-------+-------+          +-------+-------+          +-------+-------+
        |                          |                          |
        |   +----------------------+---------------+          |
        |   |                                      |          |
        v   v                                      v          v
+---------------+                          +---------------+  +---------------+
| webhook-async |                          | webhook-async |  | webhook-async |
| -hook-engine  |                          |    -queue     |  |   -executor   |
+-------+-------+                          +-------+-------+  +-------+-------+
        |                                          |                  |
        |                                          v                  |
        |                                  +---------------+          |
        |                                  | webhook-async |          |
        |                                  |   -storage    |          |
        |                                  +---+-------+---+          |
        |                                      |       |              |
        |                    +-----------------+       +----------+   |
        |                    v                                    v   |
        |       +------------------------+         +------------------------+
        |       | webhook-async-storage  |         | webhook-async-storage  |
        |       |        -sqlite         |         |        -redis          |
        |       +------------+-----------+         +------------+-----------+
        |                    |                                  |
        |   +----------------+----------------------------------+
        |   |
        v   v
+---------------+       +---------------+       +---------------+
| webhook-async | <---- | webhook-async | <---- | webhook-async |
|    -config    |       |    -compat    |       |     -core     |
+---------------+       +---------------+       +---------------+
```

## 2. Crate Responsibilities

| Crate | Purpose |
|---|---|
| `webhook-async-core` | Backend-neutral domain types (`HookDefinition`, `TriggerRule`, `JobState` state machine, `ExecutionSpec`, `JobRecord`, `JobAttempt`, `WorkerInfo`, `GlobalExecutionMode` / `EffectiveExecutionMode`, `CoreError`). |
| `webhook-async-compat` | `adnanh/webhook` v2.8.3 JSON/YAML hook schema parsing (`#[serde(deny_unknown_fields)]`), argument/rule validation, upstream defaults, and Go-compatible config templating (`-template`: `getenv`, `cat`, `credential`, `js` filter via `minijinja`). |
| `webhook-async-config` | Application TOML configuration (`[server]`, `[storage]`, `[worker]`, `[queue]`, `[ui]`, `[auth]`), credential-redacted `Debug`, cross-field validation, and `RuntimeConfig` construction enforcing queue backend presence when any async mode is active. |
| `webhook-async-hook-engine` | Normalized `RequestContext` preserving untouched raw bytes, MIME canonical header lookup, dot-path value extraction with literal-key precedence, JSON/XML (`mxj`)/form/multipart parsing, `parse-parameters-as-json`, `TriggerRule` boolean/error evaluation (`value`, `regex`, `payload-hmac-*`, `ip-whitelist`, `scalr-signature`), and `ExecutionSpec` builder. |
| `webhook-async-storage` | Backend-neutral `QueueStorage` trait, transactional queue contracts, `JobFilter` pagination, and shared 11-scenario conformance suite (`conformance::run_all`). |
| `webhook-async-storage-sqlite` | SQLite WAL implementation via `sqlx` (embedded migrations, busy timeout, foreign keys, single-writer short transactions, partial scheduling indexes). |
| `webhook-async-storage-redis` | Shared Valkey/Redis protocol adapter using hash-tagged `{prefix}:*` ZSETs and atomic Lua scripts (`EVALSHA`) for priority claiming, delayed retries, lease expiry, and ownership-checked acks. |
| `webhook-async-queue` | `QueueCoordinator` for `max_pending_jobs` admission control, capped exponential backoff + deterministic jitter, failure classification, and cancel/retry delegation. |
| `webhook-async-executor` | Secure direct child-process execution (`tokio::process::Command`), bounded output capture, timeout/process-group cancellation (`SIGKILL` to `-pgid`), cooperative cancellation via `tokio::sync::watch`, and `pass-file-to-command` temp-file lifecycle. |
| `webhook-async-worker` | Bounded-concurrency worker pool (`tokio::sync::Semaphore`), lease heartbeats, expired lease recovery, and graceful shutdown. |
| `webhook-async-api` | Axum webhook receiver (`/{urlprefix}/{id}`), sync execution & bounded background dispatch (Phase 2), durable async enqueue before HTTP 202 (Phase 5), REST `/api/v1/*` admin endpoints, OpenAPI 3.1 schema, rate limiting, Bearer auth (Phase 6), and multi-transport serving (`serve_full` supporting plain TCP, TLS TCP, plain Unix domain sockets, and TLS Unix domain sockets via `tokio-rustls`). |
| `webhook-async-web` | Server-rendered Askama + HTMX + SSE management UI with session authentication, CSRF protection, and secret redaction (Phase 6). |
| `webhook-async-cli` | Binary entrypoint (`webhook-async`) wiring `server` / `run` (sync/async runtime, worker pool orchestration, Web UI, hot-reloading with atomic hook swaps, signals, pidfile, logfile, nopanic), standalone `worker`, `config validate`, `version`, `jobs list/get/retry/cancel`, and `storage status`. |

## 3. Execution Mode Invariants

1. Global execution mode is resolved from CLI flags (`--sync` vs `--async`, mutually exclusive before binding a listener; default `Unspecified` -> sync).
2. Per-hook mode resolution follows exact precedence: hook `async` boolean -> global CLI mode -> `Sync`.
3. Synchronous hooks never initialize or touch queue storage.
4. Asynchronous hooks persist an immutable `ExecutionSpec` snapshot (command, args, resolved env vars, temp-file payloads, working dir, timeout, max attempts, `config_version`) before returning `HTTP 202 Accepted`. Subsequent config reloads never mutate already accepted jobs.

## 4. Implementation Notes

- `webhook-async-config::compute_config_version` computes a deterministic `sha256:<hex>` digest over serialized configuration and resolved hooks.
- `webhook-async-core::JobState` enforces all legal transitions among the 8 canonical states (`queued`, `running`, `retry_wait`, `succeeded`, `failed`, `cancel_requested`, `cancelled`, `dead_letter`).
- `webhook-async-executor` places spawned children in their own Unix process group (`process_group(0)`) so timed-out trees are terminated cleanly (`SIGKILL` to `-pgid`) and `pass-file-to-command` temporary files are cleaned up on exit.
- `webhook-async-storage-sqlite` formats UTC timestamps with fixed-width RFC3339 nanoseconds (`SecondsFormat::Nanos`) so SQLite lexical timestamp comparisons remain exact across sub-second boundaries, and enforces single-writer discipline (`max_connections(1)` for `:memory:`, short write-first transactions) while never holding locks during child execution.
- `webhook-async-storage-redis` uses hash-tagged `{key_prefix}:*` keys with `redis::aio::ConnectionManager` (bounded 5s connect / 10s response timeouts) and atomic Lua scripts instead of Redis Streams so per-job ownership checks (`LeaseToken`), priority ordering, and `CancelRequested` guards enforce identical `QueueStorage` semantics across Valkey and Redis.
- `webhook-async-worker` runs a bounded worker pool owning a `tokio::task::JoinSet` with per-job heartbeat tasks that monitor `CancelRequested` and lost lease ownership, killing child process groups and transitioning jobs cleanly while draining gracefully up to `shutdown_grace_seconds`.
- `webhook-async-api` returns HTTP `202 Accepted` only after durable storage enqueue commits the immutable `ExecutionSpec` snapshot, mapping queue capacity saturation to `429 Too Many Requests` and storage unavailability to `503 Service Unavailable`.
- Multi-transport serving (`webhook-async-api::serve_full`) integrates Rustls with Axum via a zero-cost generic `TlsListener<L>` wrapping both `TcpListener` and `UnixListener`, supporting self-signed certificate generation on development without external tools.
- Operational reloading (`webhook-async-cli::spawn_reload_watcher`) dynamically reloads hook definitions on `SIGHUP`, `SIGUSR1`, or `-hotreload` polling via an atomic `Arc<RwLock<HashMap<String, ResolvedHook>>>` shared across the API receiver and Web UI, keeping current hooks active if new definitions fail to parse or validate while in-flight jobs retain their immutable `ExecutionSpec` snapshots.
