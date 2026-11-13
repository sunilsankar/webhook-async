# Implementation Task Plan (`TASKS.md`)

Pinned upstream compatibility baseline: `adnanh/webhook` `2.8.3` (`f78eeeca7d2d5f4af2e792d1df5c6e2b0c922dad`).

## Phase 0 — Repository, Compatibility Baseline, and Workspace Scaffold
- [x] Initialize Git repository and baseline `.gitignore` + `rust-toolchain.toml`
- [x] Audit `adnanh/webhook` v2.8.3 sources, docs, and test fixtures
- [x] Create `docs/COMPATIBILITY.md` inventory with pinned commit SHA
- [x] Create `ARCHITECTURE.md` and `TASKS.md`
- [x] Scaffold 13 workspace crates (`core`, `config`, `compat`, `hook-engine`, `api`, `queue`, `storage`, `storage-sqlite`, `storage-redis`, `worker`, `executor`, `web`, `cli`) with clean check/clippy/test build

## Phase 1 — Core Domain, Config, Compat Parsing, and Hook Engine Foundations
- [x] `webhook-async-core`: `JobState` (8 canonical states + validated transition matrix), `ExecutionSpec`, `JobRecord`, `JobAttempt`, `WorkerInfo`, `HookDefinition`, `TriggerRule`, `GlobalExecutionMode` / `EffectiveExecutionMode`
- [x] `webhook-async-compat`: strict JSON/YAML hook parsing (`#[serde(deny_unknown_fields)]`), argument & rule validation, duplicate-ID detection, `async` / `timeout-seconds` / `max-attempts` extensions
- [x] `webhook-async-config`: TOML `[server]`, `[storage]`, `[worker]`, `[queue]`, `[ui]`, `[auth]` parsing, credential-redacting `Debug`, cross-field validation, `RuntimeConfig` queue requirement enforcement
- [x] `webhook-async-hook-engine`: `RequestContext`, MIME canonical header lookup (preserving JSON subkey casing), literal-key-first dot-path extraction, JSON array `root` wrapping, `parse-parameters-as-json`, `TriggerRule` boolean eval with `Or` error-swallowing parity

## Phase 2 — Complete Upstream Matchers, Payload Parsers, and Sync HTTP Server
- [x] `webhook-async-config`: upgrade `compute_config_version` to SHA-256 (`sha2`) and add server `url_prefix`, `response_headers`, `use_x_request_id`, `x_request_id_limit`
- [x] `webhook-async-hook-engine`: XML (`mxj` `-attr`/`#text`), `application/x-www-form-urlencoded`, and `multipart/form-data` parsers; matchers (`value`, `regex`, `payload-hmac-sha1/256/512`, `ip-whitelist`, `scalr-signature` ±300s Date window); `prepare_execution_spec`
- [x] `webhook-async-executor`: direct allowlisted `tokio::process::Command` runner (argv, env, working dir, `pass-file-to-command` temp-file lifecycle, bounded stdout/stderr, process-group timeout kill)
- [x] `webhook-async-api` + `webhook-async-cli`: synchronous `server` (`--sync` / `--async` mutual exclusion / default sync), `--hooks`, `--urlprefix`, `--header`, `--x-request-id`, `--http-methods`, `--max-multipart-mem`, `config validate`, `version`, and upstream compatibility fixture test suite
- [x] Initial documentation: `docs/COMPATIBILITY.md`, `docs/API.md`, `docs/CONFIGURATION.md`

## Phase 3 — Storage Trait, Queue Coordinator, and SQLite Backend
- [x] `webhook-async-storage`: backend-neutral `QueueStorage` trait, pagination/filter types, idempotency types, and shared 11-scenario conformance test suite
- [x] `webhook-async-storage-sqlite`: embedded SQLx migration (`0001_init.sql`), WAL mode, busy timeout, foreign keys, atomic lease claims, heartbeats, ownership-checked acks, retries, cancellation, dead-letter, crash-recovery, and conformance tests
- [x] `webhook-async-queue`: `QueueCoordinator` admission control (`max_pending_jobs`), capped exponential backoff + jitter, transient vs permanent classification, and SQLite integration tests
- [x] `docs/STORAGE.md` created and `docs/CONFIGURATION.md` synchronized with actual config schema

## Phase 4 — Shared Valkey / Redis Protocol Backend
- [x] `webhook-async-storage-redis`: single shared RESP adapter (`valkey` & `redis` selectors), hash-tagged key layout (`{prefix}:*`), atomic ZSET + Lua scripts (`EVALSHA`) for enqueue/idempotency, priority claim, lease heartbeat/recovery, ownership-checked acks, retry ZSET promotion, and cancellation
- [x] Wire shared storage conformance suite in `webhook-async-storage-redis` (`REDIS_URL` / `VALKEY_URL`) and update `docs/STORAGE.md`, `docs/CONFIGURATION.md`, and `ARCHITECTURE.md`
- [x] Run live `webhook-async-storage-redis` conformance suite against running Valkey and Redis servers (`VALKEY_URL` / `REDIS_URL`; verified against `redis:7-alpine` and `valkey/valkey:8-alpine` with 100% pass across all 11 conformance scenarios)

## Phase 5 — Worker Pool, Lease Recovery, and Durable Async Receiver Path
- [x] `webhook-async-executor`: process-group termination, cooperative cancellation (`tokio::sync::watch`), output streaming bounds
- [x] `webhook-async-worker`: bounded worker concurrency (`Semaphore`), lease heartbeat task, stale-ownership handling, expired lease sweep, graceful shutdown
- [x] `webhook-async-api`: durable enqueue before HTTP 202 (`application/json`), queue full `429`/`503` rejection, storage failure `503` without 202, mixed sync/async routing tests
- [x] `webhook-async-cli`: standalone `worker` command, `server` / `run` async queue and worker pool orchestration with graceful shutdown

## Phase 6 — Management REST API and Askama + HTMX + SSE Web UI
- [x] `webhook-async-api`: `/api/v1/health`, `/api/v1/ready`, `/api/v1/stats`, `/api/v1/jobs`, `/api/v1/jobs/{id}`, `/api/v1/jobs/{id}/retry`, `/api/v1/jobs/{id}/cancel`, `/api/v1/hooks`, `/api/v1/workers`, `/api/v1/storage/status`, `/api/v1/openapi.json`, `/api/v1/events` (SSE), constant-time Bearer token auth, 60 req/min rate limits, hook secret redaction
- [x] `webhook-async-web`: Askama templates (`dashboard`, `jobs`, `job_detail`, `hooks`, `storage`, `login`), vendored HTMX, SSE live stream (`/events`), session auth, per-session CSRF protection, pagination, light/dark responsive CSS, UI/auth/CSRF/XSS tests
- [x] `webhook-async-cli`: CLI commands `jobs list`, `jobs get <job-id>`, `jobs retry <job-id>`, `jobs cancel <job-id>`, and `storage status` (`--json` and formatted ASCII table output); Web UI and WorkerRegistry orchestration in `server` / `run`
- [x] `docs/SECURITY.md` created (threat model, process group isolation, Bearer/session auth, CSRF, rate limiting, XSS escaping, secret redaction) and `docs/API.md` updated

## Phase 7 — Upstream Templates, Hot Reload, TLS, Signals, Unix Sockets, and Production Docs
- [x] `webhook-async-compat`: Go-style template preprocessing (`-template`: `getenv`, `cat`, `credential`, `js` filter)
- [x] Upstream operational flags: `-hotreload`, `SIGHUP`/`SIGUSR1` reload preserving in-flight job snapshots, `SIGTERM`/`SIGINT` cleanup, `-tls-*` / `-secure`, `-socket`, `-pidfile`, `-setuid`/`-setgid`, `-logfile`, `-nopanic`
- [x] Production operations documentation: `docs/DEPLOYMENT.md`, `docs/OPERATIONS.md`, `docs/COMPATIBILITY.md` inventory update
