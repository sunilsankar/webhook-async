# webhook-async — Product & Technical Specification

**Document:** `SPECIFICATION.md`  
**Project:** `webhook-async`  
**Status:** Baseline product specification  
**Implementation language:** Rust  
**Primary goal:** Feature-compatible webhook execution with durable asynchronous processing and an integrated lightweight management UI.

---

## 1. Product vision

`webhook-async` is a standalone, self-hostable webhook receiver and job execution engine written primarily in Rust.

It must independently implement the externally observable functionality of [`adnanh/webhook`](https://github.com/adnanh/webhook), while adding a durable asynchronous queue, worker management, job history, retries, operational APIs and a lightweight web dashboard.

It is not a wrapper around the upstream executable and must not require the upstream project at runtime.

The product has three equally important goals:

1. **Webhook compatibility:** Existing supported hook configurations and synchronous workflows should continue to work with minimal or no changes.
2. **Reliable asynchronous execution:** Webhooks can be accepted durably and executed later by bounded worker processes.
3. **Operational usability:** Administrators can inspect hooks, jobs, workers, failures and queue health through a simple integrated UI.

### 1.1 Product principles

- Rust-first implementation; avoid unnecessary services and runtimes.
- One executable for the API, optional worker process, and embedded UI.
- SQLite works out of the box without an external service.
- Valkey is the preferred self-hosted distributed queue backend.
- Redis remains a supported alternative.
- Storage backends are selectable and isolated behind a stable abstraction.
- Synchronous behavior remains available for legacy compatibility.
- Asynchronous behavior is explicit and durable.
- Security and operational clarity take priority over convenience shortcuts.
- No fake functionality, silently ignored configuration, or misleading compatibility claims.

### 1.2 Non-goals for the initial release

Unless explicitly added to a later approved scope, v1 does not aim to provide:

- A general-purpose visual workflow builder.
- A SaaS multi-tenant control plane.
- A plugin marketplace.
- Arbitrary user-authored code execution through the UI.
- Exactly-once execution guarantees.
- A replacement for a full observability platform.
- A mandatory Kubernetes, Docker, Node.js or external frontend deployment.
- Distributed transactions across external systems.

---

## 2. Reference projects and compatibility policy

### 2.1 Functional references

- Upstream webhook: https://github.com/adnanh/webhook
- Celery Flower: https://github.com/mher/flower
- Hook0: https://github.com/hook0/hook0
- Valkey: https://valkey.io/
- Redis: https://redis.io/

Use upstream projects as behavioral and UX references. Do not copy source code, branding, trademarks, logos or assets without reviewing their licenses and complying with all applicable terms.

### 2.2 Full webhook feature compatibility

The target is **full externally observable feature parity for the documented and tested behavior of the selected upstream webhook release**, subject to explicit platform limitations.

This means compatibility work must begin with a source and test audit. Do not assume a feature exists or infer its exact semantics from its name.

Before implementation, create `docs/COMPATIBILITY.md` containing:

- Exact upstream release or commit audited.
- Documentation pages and examples reviewed.
- CLI flags and defaults.
- Hook configuration fields and defaults.
- Request parsing behavior.
- Every payload and request-value source.
- Trigger-rule types, operators and composition semantics.
- Signature validation behavior.
- Response status, headers and body behavior.
- Command execution and temporary-file behavior.
- Template behavior and supported functions.
- TLS, listener, socket and signal behavior.
- Platform-specific capabilities.
- An upstream-to-webhook-async regression test mapping.

Every inventory item must be marked as one of:

- `NOT_STARTED`
- `IMPLEMENTED`
- `TESTED_COMPATIBLE`
- `PLATFORM_LIMITATION`
- `INTENTIONALLY_DIFFERENT`

A feature must not be marked `TESTED_COMPATIBLE` without an automated test or a documented manual verification procedure tied to a known upstream behavior.

### 2.3 Compatibility modes

The implementation must distinguish:

- **Legacy synchronous mode:** Preserve upstream-style behavior, response semantics and command execution timing.
- **Asynchronous mode:** Persist a job and return an acceptance response; command output and final result are retrieved later.
- **Mixed mode:** Different hooks may select different execution modes.

Existing hook definitions must not unexpectedly become asynchronous.

### 2.4 Compatibility versioning

Record the upstream compatibility baseline in the repository. When upstream behavior changes, update the inventory and regression fixtures deliberately. Do not silently redefine compatibility.

---

## 3. Functional scope

### 3.1 Hook configuration

The system must support the documented hook configuration format and behavior identified by the compatibility audit, including as applicable:

- JSON hook definitions.
- YAML hook definitions.
- Multiple hook files and directories where supported by the compatibility baseline.
- Hook identifiers and trigger names.
- Executable/command definitions.
- Working directory configuration.
- Allowed HTTP methods.
- Hook-specific response configuration.
- Trigger rules.
- Request and payload value extraction.
- Environment-variable construction.
- Argument construction.
- Temporary-file configuration.
- Template-based configuration.
- Global defaults and server-level configuration.

Configuration parsing must produce validated internal domain types. Keep upstream-compatible input formats separate from internal Rust data models.

Unknown fields must follow a documented compatibility policy. Do not silently discard fields that affect execution.

### 3.2 HTTP request handling

Support the behavior required by the compatibility audit, including:

- Configured routes and URL prefixes.
- HTTP method restrictions.
- Request headers.
- Query parameters.
- Request body and content type.
- JSON payload parsing.
- URL-encoded forms.
- Multipart forms and file uploads.
- Configured content-type overrides, if present in the upstream contract.
- Request identifiers.
- Body-size and multipart limits.
- Clear errors for malformed or unsupported input.

Request data must be parsed once into a normalized request context. Preserve original bytes where required for signature validation, raw-body access or compatibility.

### 3.3 Request-value sources and transformations

Inventory and implement every upstream-documented value source and transformation. The compatibility audit must explicitly cover, where present in the selected upstream release:

- Header values.
- URL/query values.
- Parsed payload fields and nested paths.
- Static string values.
- Raw body.
- Entire payload.
- JSON parsing of selected values.
- Form fields.
- Multipart fields and uploaded files.
- Base64 decoding.
- Temporary-file creation and argument references.
- Environment-variable values.
- Dynamic command arguments.

The exact syntax, missing-value behavior, type conversion and error behavior must be verified against upstream documentation and tests.

### 3.4 Trigger-rule engine

Implement every documented trigger-rule type, comparator, operator and nesting behavior in the compatibility baseline.

The audit must include:

- Equality and inequality.
- String and regular-expression matching.
- Numeric comparisons where supported.
- Presence/absence checks where supported.
- Header, query and payload value checks.
- IP/network matching where supported.
- HMAC/signature rules and supported algorithms.
- Nested `and` / `or` composition.
- Soft-failure behavior where documented.
- Rule mismatch status and response behavior.

Do not replace legacy signature semantics with a new scheme. Additional authentication methods may be added as independent extensions.

Rule evaluation must be deterministic, side-effect-free and complete before job acceptance.

### 3.5 Command execution

Support upstream-compatible command execution in synchronous mode, including:

- Configured executable.
- Explicit argument vector.
- Working directory.
- Environment variables.
- Request-derived temporary files.
- Exit status.
- Standard output and standard error.
- Configured response behavior.
- Timeout behavior where supported or added as an explicit extension.

Never build a shell command by interpolating untrusted request values. Use direct process execution with an explicit executable and argument vector. If shell execution is intentionally supported for compatibility, it must be an explicit administrator-configured executable such as `/bin/sh` with separately constructed arguments; never evaluate request content as shell syntax.

Output capture must be bounded. Process termination must handle child processes and process groups where the platform permits.

### 3.6 HTTP response behavior

Synchronous hooks must preserve configured response semantics, including applicable:

- Status codes.
- Response headers.
- Response body/message.
- Trigger mismatch responses.
- Error responses.
- Command output response modes.
- Content type.

Asynchronous hooks must return only after durable acceptance. Their response must contain a stable job identifier and a way to inspect the job.

Do not claim that a command succeeded when it has only been queued.

### 3.7 Server and runtime options

The compatibility audit must inventory and implement the applicable upstream command-line and server features, including:

- Listen address and port.
- URL prefix.
- Hook file or directory selection.
- Default HTTP methods.
- Verbose/debug logging.
- Log file behavior.
- PID file behavior.
- Version output.
- Request IDs.
- TLS certificate and key configuration.
- TLS protocol and cipher configuration where supported.
- Global response headers.
- Template configuration.
- Multipart memory thresholds.
- Hot reload.
- Signal-triggered reload.
- Graceful shutdown.
- TCP listeners.
- Unix domain sockets where supported.
- systemd socket activation where supported.
- Privilege dropping where supported.
- Platform-specific listener and process behavior.

Preserve documented defaults unless a security concern requires a change. Any changed default must be documented and configurable.

---

## 4. Asynchronous queue and execution engine


### 3.8 Execution mode and asynchronous opt-in

Asynchronous execution is **opt-in**. The default behavior must remain synchronous and must preserve the externally observable behavior of the pinned `adnanh/webhook` compatibility baseline.

#### Global CLI flags

The `server` command must support:

```bash
webhook-async server          # default: synchronous
webhook-async server --sync   # explicitly force synchronous mode
webhook-async server --async  # enable asynchronous mode globally
```

Rules:

- If neither flag is supplied, use synchronous mode.
- `--sync` and `--async` are mutually exclusive. Supplying both must fail before the listener starts, with a clear configuration error and non-zero exit status.
- The selected global mode is the fallback for hooks without an explicit per-hook override.
- Do not silently enable asynchronous mode because a queue backend happens to be configured.

#### Per-hook override and precedence

Support an optional `async` boolean extension in each hook definition:

```json
[
  {
    "id": "deploy-production",
    "execute-command": "/usr/local/bin/deploy.sh",
    "async": true
  },
  {
    "id": "restart-service",
    "execute-command": "/usr/local/bin/restart.sh",
    "async": false
  }
]
```

Resolve the effective mode in this exact order:

1. An explicitly supplied per-hook `async` value (`true` or `false`).
2. The global CLI mode (`--async` or `--sync`).
3. Synchronous mode when neither is specified.

An omitted per-hook property is not equivalent to `false`; it inherits the global mode. This permits mixed synchronous and asynchronous hooks in one server process. The compatibility audit must confirm how unknown hook fields are handled by the pinned upstream version and ensure this extension does not break legacy hook files.

#### Synchronous execution

- Execute the command as part of handling the HTTP request.
- Preserve upstream-compatible response status, headers, body, output and timing semantics.
- Do not enqueue the request.
- Do not require SQLite, Valkey, Redis, a queue schema, or a running worker.
- Synchronous-only installations must start and operate without configuring any queue backend.

#### Asynchronous execution

- Perform request validation, authentication, parsing and trigger evaluation before enqueueing.
- Persist the immutable execution snapshot and all required request artifacts before acknowledging acceptance.
- Return HTTP `202 Accepted` only after the selected backend confirms durable acceptance according to its documented durability guarantees.
- Return a documented error if storage is unavailable or capacity limits prevent acceptance; never return `202` for an uncommitted job.
- Execute the accepted job through a worker and expose its status/results through the management API and dashboard.
- The asynchronous response format must be documented and configurable only where doing so does not weaken the acceptance guarantee.

Example response:

```json
{
  "job_id": "uuid",
  "status": "queued",
  "message": "Webhook accepted for asynchronous execution"
}
```

#### Reload and mode stability

- A job accepted asynchronously retains its immutable execution snapshot and remains asynchronous regardless of later configuration reloads.
- A configuration reload must not change the mode of a request already being processed.
- New requests use the newly resolved configuration only after a successful, atomic configuration reload.
- Changing global mode requires an explicit restart or a separately specified administrative operation; do not silently reinterpret existing jobs.

#### Backward compatibility and acceptance criteria

- Existing hook definitions with no `async` property remain synchronous when no global `--async` flag is supplied.
- Running without flags must behave as a synchronous webhook-compatible server.
- The synchronous path must not initialize or connect to queue storage.
- `--async` must fail clearly at startup if no usable queue backend is configured.
- Per-hook `async: false` must override global `--async`; per-hook `async: true` must override global `--sync`.
- Add automated tests for default mode, each flag, conflicting flags, precedence, mixed-mode hooks, queue-backend outage, and unchanged synchronous response behavior.

### 4.1 Core semantics

The queue provides **at-least-once execution**, not exactly-once execution.

A worker may perform an external side effect and crash before acknowledging completion. The job may then be delivered again. Handlers should be idempotent, and the system must support idempotency keys and attempt history.

### 4.2 Job states

Canonical states:

- `queued`
- `running`
- `retry_wait`
- `succeeded`
- `failed`
- `cancel_requested`
- `cancelled`
- `dead_letter`

State transitions must be validated centrally. A backend must not permit illegal transitions.

Typical transitions:

- `queued -> running`
- `running -> succeeded`
- `running -> retry_wait`
- `retry_wait -> queued`
- `running -> failed`
- `failed -> queued` through an explicit retry operation
- `queued -> cancelled`
- `running -> cancel_requested -> cancelled` where cooperative cancellation succeeds
- `retry_wait -> cancelled`
- `failed/retry_wait -> dead_letter` after retry exhaustion or configured policy

The precise terminal/retry policy must be documented and tested.

### 4.3 Job record

A job must include, at minimum:

- Stable job ID.
- Hook ID/name.
- Task/executor identifier.
- State.
- Priority.
- Creation, availability, start and completion timestamps.
- Attempt count and maximum attempts.
- Next retry timestamp.
- Idempotency key, if supplied.
- Worker ID and lease token/ownership metadata.
- Lease expiry and heartbeat timestamp.
- Configuration version/snapshot reference.
- Validated execution specification.
- Required request metadata.
- Durable references to request-derived files/artifacts.
- Exit code and bounded output references.
- Error classification and safe error summary.
- Correlation/request ID.
- Audit metadata for administrative actions.

Sensitive request values must be encrypted or redacted according to policy. Never persist credentials unnecessarily.

### 4.4 Durable acceptance

For asynchronous mode:

1. Authenticate and validate the request.
2. Parse request data and evaluate trigger rules.
3. Build the immutable execution snapshot.
4. Persist the job and all required artifacts.
5. Commit the storage transaction.
6. Only then return HTTP `202 Accepted`.

If persistence fails, return a suitable server error and do not claim acceptance.

If the queue is full, return a documented capacity response (`429` or `503`) and do not accept the job.

### 4.5 Retries

Support:

- Configurable maximum attempts.
- Exponential backoff.
- Random jitter.
- Configurable minimum and maximum delay.
- Retryable versus non-retryable error classification.
- Manual retry.
- Dead-letter handling.
- Attempt history.

Do not retry authentication, configuration or validation failures as if they were transient execution failures.

### 4.6 Worker leases and recovery

Workers must claim jobs atomically and acquire ownership/lease metadata.

Implement:

- Heartbeats.
- Lease extension.
- Lease expiry.
- Stale-worker detection.
- Recovery of abandoned work.
- Ownership checks on acknowledgement.
- Protection against a former worker acknowledging a job after its lease was reassigned.
- Graceful shutdown and optional drain mode.

Lease durations and heartbeat intervals must be configurable and validated. Avoid aggressive expiry that causes duplicate execution during normal long-running tasks.

### 4.7 Concurrency and backpressure

- Worker concurrency must be bounded.
- Queue capacity must be configurable.
- Request body and artifact sizes must be limited.
- Process output capture must be bounded.
- Per-hook and global concurrency limits should be supported if feasible.
- Priority scheduling must have starvation protections or documented semantics.
- API acceptance must not spawn an unbounded Tokio task per webhook.

### 4.8 Cancellation

- Queued jobs can be cancelled.
- Delayed jobs can be cancelled.
- Running jobs receive a cancellation request.
- The executor attempts process termination and child cleanup where supported.
- Cancellation is not rollback: external side effects may already have occurred.
- UI/API must show whether cancellation was requested, completed or could not be confirmed.

### 4.9 Idempotency

Support an idempotency key supplied by the caller or generated according to documented rules.

Define:

- Scope (global, hook or tenant).
- Retention duration.
- Behavior for duplicate submissions.
- Behavior when the original job is running or terminal.
- Storage uniqueness constraints.

Idempotency prevents duplicate acceptance within its scope; it does not make external task side effects exactly-once.

---

## 5. Storage architecture

### 5.1 Supported backends

| Backend selector | Implementation | Role |
|---|---|---|
| `sqlite` | SQLite adapter | Default standalone backend |
| `valkey` | Redis-protocol adapter | Preferred self-hosted distributed backend |
| `redis` | Redis-protocol adapter | Supported alternative |

Use one shared Redis-protocol storage crate for Valkey and Redis. The crate name may be `webhook-async-storage-redis`; configuration selectors remain explicit.

Never silently fall back between backends. A selected backend failure must be visible and must prevent false acceptance.

### 5.2 Storage abstraction

Define a backend-neutral `QueueStorage` contract. It must cover all state transitions required by the worker and API, including:

- Enqueue.
- Atomic claim.
- Heartbeat/lease extension.
- Ownership-checked acknowledgement.
- Retry scheduling.
- Cancellation.
- Job retrieval and paginated listing.
- Dead-letter operations.
- Idempotency.
- Recovery.
- Health/capability reporting.
- Required audit operations.

Avoid exposing SQL rows, Redis keys, stream IDs or backend-specific data structures to domain and API crates.

The abstraction may expose explicit backend capabilities when semantics genuinely differ, but do not pretend unsupported guarantees are identical.

### 5.3 SQLite

Requirements:

- WAL mode where appropriate.
- Configurable busy timeout.
- Foreign keys enabled.
- Short write transactions.
- Atomic job claim/update.
- Appropriate indexes for status, availability, priority and timestamps.
- Serialized or carefully managed writes.
- No transaction held while a task runs.
- Durable artifact handling.
- Backup and restore documentation.
- Schema migrations.
- Recovery tests after process termination.

SQLite is a single-writer database. Do not claim it provides the same multi-host write scaling as Redis. Multi-process access may be supported on a suitable shared local filesystem, but network filesystems must not be assumed safe.

### 5.4 Valkey and Redis

Use a verified async Rust client compatible with the required commands and both target servers.

Preferred design:

- Streams and consumer groups for delivery.
- Pending Entry Lists for unacknowledged messages.
- Sorted sets or another explicit mechanism for delayed retries.
- Atomic scripts/transactions for state transitions where needed.
- Lease/ownership metadata.
- Reclaim/recovery of abandoned pending entries.
- Durable job metadata and attempt history.
- Stable key namespace/prefix.
- Explicit acknowledgement only after successful state persistence.

Do not use a simple Redis list as the complete queue implementation.

Verify every command, script, stream operation and recovery procedure against supported Valkey and Redis versions. Do not assume full compatibility merely because both implement the Redis protocol.

Document persistence, AOF/RDB configuration, replication, failover, backup and data-loss implications. Redis/Valkey durability depends on server configuration and deployment; the application must not overstate it.

### 5.5 Backend conformance

Create one shared storage conformance suite and run it against:

- SQLite.
- Supported Valkey version(s).
- Supported Redis version(s).

Include concurrent claims, duplicate idempotency keys, leases, expired leases, acknowledgement ownership, delayed retry, cancellation, pagination, dead-letter handling and crash recovery.

---

## 6. Rust architecture and crate boundaries

Use a Cargo workspace with small crates and explicit dependency direction.

Proposed layout:

```text
webhook-async/
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── SPECIFICATION.md
├── ARCHITECTURE.md
├── AGENTS.md
├── TASKS.md
├── README.md
├── docs/
│   ├── COMPATIBILITY.md
│   ├── API.md
│   ├── CONFIGURATION.md
│   ├── STORAGE.md
│   ├── SECURITY.md
│   ├── DEPLOYMENT.md
│   └── OPERATIONS.md
├── crates/
│   ├── webhook-async-core/
│   ├── webhook-async-config/
│   ├── webhook-async-compat/
│   ├── webhook-async-hook-engine/
│   ├── webhook-async-api/
│   ├── webhook-async-queue/
│   ├── webhook-async-storage/
│   ├── webhook-async-storage-sqlite/
│   ├── webhook-async-storage-redis/
│   ├── webhook-async-worker/
│   ├── webhook-async-executor/
│   ├── webhook-async-web/
│   └── webhook-async-cli/
├── templates/
│   ├── base.html
│   ├── dashboard.html
│   ├── jobs.html
│   ├── job_detail.html
│   ├── hooks.html
│   ├── workers.html
│   ├── dead_letters.html
│   ├── metrics.html
│   ├── settings.html
│   └── fragments/
├── static/
│   ├── css/
│   ├── js/
│   └── icons/
├── migrations/
│   └── sqlite/
├── config/
│   └── webhook-async.example.toml
├── tests/
│   ├── compatibility/
│   ├── integration/
│   ├── storage/
│   ├── recovery/
│   ├── security/
│   └── ui/
└── packaging/
    └── systemd/
```

Crate responsibilities:

- `core`: domain types, IDs, state machine, errors, shared traits.
- `config`: TOML and legacy hook configuration loading, validation and reload.
- `compat`: upstream-compatible parsing and translation.
- `hook-engine`: route matching, request context, value extraction and trigger evaluation.
- `api`: REST endpoints, authentication, authorization and OpenAPI.
- `queue`: backend-neutral orchestration and scheduling.
- `storage`: storage traits and capability definitions.
- `storage-sqlite`: SQLite implementation.
- `storage-redis`: shared Valkey/Redis protocol implementation.
- `worker`: claim loop, leases, heartbeats, recovery and shutdown.
- `executor`: safe process executor and future executor abstractions.
- `web`: Askama templates, HTMX fragments, SSE and static assets.
- `cli`: binary entry point and command-line interface.

The domain/core crate must not depend on Axum, SQLx, Redis client types or UI templates.

---

## 7. Dashboard and UI specification

### 7.1 Technology

The UI must be lightweight and Rust-first:

- Axum for routes and handlers.
- Askama for server-rendered HTML.
- HTMX for partial updates and actions.
- Lightweight custom CSS or Pico CSS.
- Server-Sent Events (SSE) for live updates.
- Minimal vanilla JavaScript only where necessary.
- Embedded static assets in the final executable.

Do not use React, Next.js, Vue, Angular, Svelte or a Node.js production runtime. Avoid a frontend build pipeline unless a concrete requirement justifies it.

The UI is served by the same executable and process as the API when enabled.

### 7.2 Pages

1. **Overview**
   - Queue depth by state.
   - Active and unavailable workers.
   - Success/failure counts and rates.
   - Execution duration/latency.
   - Recent jobs and failures.
   - Storage health.
   - Time-range selection.

2. **Jobs and queues**
   - Search.
   - Filters by state, hook, task, worker, date and attempt.
   - Sorting and pagination.
   - Queued, running, scheduled, succeeded, failed and dead-letter views.
   - Bulk operations only where safe and explicitly confirmed.

3. **Job detail**
   - Job metadata and state.
   - Request metadata and redacted payload.
   - Execution snapshot/config version.
   - Timeline.
   - Attempt history.
   - Worker identity.
   - Start/end/duration.
   - Exit code.
   - Bounded stdout/stderr.
   - Error classification.
   - Retry and cancellation actions.

4. **Hooks**
   - Configured hooks.
   - Endpoint, methods and execution mode.
   - Last triggered time.
   - Configuration validation.
   - Safe test request facility.
   - Configuration source and reload status.
   - Do not silently edit files or override externally managed configuration.

5. **Workers**
   - Worker identity.
   - Last heartbeat/seen time.
   - Current job.
   - Concurrency.
   - Completed/failed counts.
   - Drain and graceful-stop requests where supported.
   - Distinguish a requested operation from confirmed completion.

6. **Dead-letter queue**
   - Failed jobs and reason.
   - Attempt history.
   - Inspect and requeue.
   - Audit trail.

7. **Metrics and diagnostics**
   - Queue depth and throughput.
   - Execution latency.
   - Retry and failure rates.
   - Worker health.
   - Storage backend and connectivity.
   - Application version and readiness.

8. **Settings**
   - Read-only display of active configuration.
   - Queue limits.
   - Retry policy.
   - Retention policy.
   - Worker concurrency.
   - Storage health and capabilities.
   - Authentication and access controls.
   - Configuration validation.

### 7.3 UI interaction and security

- Use HTMX for filters, pagination, partial refreshes and simple actions.
- Use SSE for live updates, with reconnect and polling fallback.
- Use server-side pagination.
- Lazy-load large payloads and logs.
- Escape all user-controlled content.
- Redact secrets and sensitive payload fields by default.
- Enforce authorization in backend handlers.
- Protect cookie-authenticated mutations against CSRF.
- Audit administrative mutations.
- Require confirmation for destructive operations.
- Never expose arbitrary command execution in the UI.
- Clearly show loading, empty, stale, unavailable and error states.
- Do not display fake metrics or sample production records.

The dashboard must be optional. API and worker operation must not depend on the UI being enabled.

### 7.4 UI configuration

Example:

```toml
[ui]
enabled = true
bind = "127.0.0.1"
path_prefix = "/ui"
```

The actual schema must be validated and documented. Default to loopback binding. LAN exposure must be an explicit administrator choice.

---

## 8. HTTP API

Use versioned REST endpoints and publish an OpenAPI document.

Initial endpoints:

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/api/v1/hooks/{name}` | Receive configured webhook |
| GET | `/api/v1/jobs` | List/search jobs |
| GET | `/api/v1/jobs/{id}` | Job detail |
| POST | `/api/v1/jobs/{id}/retry` | Explicit retry |
| POST | `/api/v1/jobs/{id}/cancel` | Cancel/request cancellation |
| GET | `/api/v1/workers` | Worker list |
| GET | `/api/v1/hooks` | Hook list |
| POST | `/api/v1/config/validate` | Validate supplied config where authorized |
| POST | `/api/v1/config/reload` | Request safe reload |
| GET | `/api/v1/health` | Liveness |
| GET | `/api/v1/ready` | Readiness including selected storage |
| GET | `/api/v1/metrics` | Operational metrics |
| GET | `/api/v1/events` | SSE event stream |

Define consistent pagination, filtering, error bodies, authentication, authorization and rate limits.

Do not expose sensitive raw payloads or process output without authorization and redaction policy.

---

## 9. CLI and process modes

The executable name is `webhook-async`.

Required commands:

```bash
webhook-async server
webhook-async worker
webhook-async run
webhook-async config validate
webhook-async jobs list
webhook-async jobs get <job-id>
webhook-async jobs retry <job-id>
webhook-async storage status
webhook-async version
```

- `server`: API and configured listeners.
- `worker`: worker engine only.
- `run`: combined API and worker mode for simple installations.
- CLI management commands must use the same domain and storage semantics as the API.

Preserve applicable upstream CLI flags and behavior through a compatibility layer where practical. Document any command-name or flag conflicts.

---

## 10. Configuration

Use TOML for new application-level settings and preserve upstream hook configuration formats for compatibility.

Example baseline:

```toml
[server]
host = "127.0.0.1"
port = 8080
url_prefix = ""

[storage]
backend = "sqlite"

[storage.sqlite]
database_url = "sqlite:///var/lib/webhook-async/webhook-async.db"
wal_mode = true
busy_timeout_ms = 5000

[worker]
concurrency = 4
poll_interval_ms = 500
lease_duration_seconds = 120
heartbeat_interval_seconds = 20
shutdown_grace_seconds = 30

[queue]
max_pending_jobs = 10000
default_max_attempts = 5
retry_base_delay_seconds = 5
retry_max_delay_seconds = 900

[ui]
enabled = true
bind = "127.0.0.1"
path_prefix = "/ui"
```

Valkey:

```toml
[storage]
backend = "valkey"

[storage.valkey]
url = "redis://127.0.0.1:6379"
key_prefix = "webhook-async:"
stream_name = "webhook-async:jobs"
consumer_group = "webhook-async-workers"
```

Redis:

```toml
[storage]
backend = "redis"

[storage.redis]
url = "redis://127.0.0.1:6379"
key_prefix = "webhook-async:"
stream_name = "webhook-async:jobs"
consumer_group = "webhook-async-workers"
```

These are illustrative defaults. Validate all fields, secret sources, URL schemes, timeouts and cross-field constraints.

Do not commit credentials. Support environment variables or protected secret files. Do not log connection URLs containing credentials.

---

## 11. Security requirements

Security requirements are mandatory:

- Bind to loopback by default.
- Require explicit configuration for LAN/public exposure.
- Authenticate administrative API and UI operations.
- Enforce authorization on the server.
- Support upstream-compatible trigger/signature validation.
- Add optional HMAC-SHA256 and bearer-token authentication for new integrations.
- Use constant-time signature comparisons.
- Validate timestamps and prevent replay where the selected scheme supports it.
- Limit request size, multipart data, payload depth and process output.
- Never log authorization headers, secrets or raw sensitive values.
- Restrict task execution to administrator-defined executable/argument definitions.
- Never allow a webhook payload to select an arbitrary executable or shell command.
- Use direct process invocation and explicit argument vectors.
- Apply execution timeouts.
- Clean up temporary files securely.
- Run under a dedicated non-root account.
- Provide hardened systemd examples.
- Protect UI mutations against CSRF when cookie sessions are used.
- Escape HTML and prevent stored/reflected XSS.
- Audit retry, cancellation, configuration reload and administrative actions.
- Protect Redis/Valkey and SQLite files with appropriate permissions and network controls.
- Document TLS termination and reverse-proxy trust boundaries.
- Do not trust forwarded headers from untrusted proxies.

Security-sensitive defaults must be conservative and documented.

---

## 12. Observability and operations

Provide structured logs with correlation IDs and job IDs.

Metrics should include, where practical:

- Accepted/rejected webhook count.
- Queue depth by state.
- Enqueue latency.
- Job wait time.
- Execution duration.
- Success/failure/retry counts.
- Worker count and heartbeat age.
- Expired leases and recovered jobs.
- Storage errors and latency.
- Dead-letter count.
- Process timeouts and cancellations.

Do not use unbounded-cardinality metric labels such as arbitrary job IDs or raw hook payload values.

Health semantics:

- Liveness indicates the process is alive.
- Readiness indicates the selected storage and required application components are usable.
- A storage failure must not be hidden by a healthy liveness response.

---

## 13. Deployment and distribution

### 13.1 Supported initial targets

- Linux x86_64.
- Linux ARM64.
- macOS Intel x86_64.
- macOS Apple Silicon ARM64.

Validate actual builds and runtime tests. Do not claim support solely because compilation succeeds.

### 13.2 Distribution

- Standalone executable.
- Versioned archives.
- SHA-256 checksums.
- Example configuration.
- systemd service units for server and worker.
- Optional combined service.
- Non-root user/group guidance.
- Upgrade and rollback instructions.
- SQLite backup/restore.
- Valkey/Redis persistence and recovery guidance.
- Reverse-proxy guidance.
- No mandatory Docker runtime.

Do not promise static linking until verified for every target and dependency combination.

### 13.3 Shutdown

On SIGTERM or equivalent:

- Stop accepting new work.
- Stop claiming jobs.
- Drain or safely release active work according to configured grace period.
- Preserve lease correctness.
- Close listeners and storage cleanly.
- Flush logs.
- Exit with meaningful status.

Signal handling and socket behavior must be implemented per platform and tested.

---

## 14. Testing strategy

### 14.1 Compatibility tests

- Every documented hook configuration property.
- JSON and YAML parsing.
- Templates.
- Every request-value source.
- Every trigger-rule type and operator.
- Nested rule combinations.
- Signature validation.
- HTTP status/header/body behavior.
- Synchronous command execution.
- Temporary-file lifecycle.
- CLI flags and defaults.
- TLS and listener configuration.
- Reload and shutdown behavior.
- Platform-specific functionality.

Use upstream examples and tests as fixtures where licensing and terms permit. Otherwise create independently authored fixtures from documented behavior.

### 14.2 Queue and storage tests

- Concurrent enqueue.
- Concurrent claim.
- Duplicate idempotency keys.
- Queue saturation.
- Retry timing and exhaustion.
- Lease heartbeat and expiry.
- Stale worker acknowledgement rejection.
- Worker crash and recovery.
- Cancellation races.
- Dead-letter operations.
- Pagination and filtering.
- SQLite crash recovery.
- Valkey and Redis stream recovery.
- Storage outage and reconnect.
- Backend conformance suite.

### 14.3 Executor tests

- Allowlisted executable.
- Argument boundaries.
- Environment values.
- Working directory.
- Timeout.
- Child process cleanup.
- Bounded stdout/stderr.
- Malformed configuration.
- Attempt failure and retry.
- Temporary-file persistence and cleanup.
- Injection payloads.

### 14.4 UI tests

- Authentication and authorization.
- XSS escaping.
- CSRF protection where applicable.
- Pagination and filters.
- Stale/disconnected SSE.
- Retry and cancellation authorization.
- Redaction.
- Empty/error/loading states.
- Responsive layout smoke tests.
- No non-functional controls.

### 14.5 Quality gates

Before a phase is complete, run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Add integration, compatibility, UI and backend-specific tests as appropriate. Document any test that requires external Valkey/Redis services.

---

## 15. Implementation roadmap

### Phase 0 — Upstream audit and design

- Pin upstream compatibility baseline.
- Inspect docs, source behavior and tests.
- Create `docs/COMPATIBILITY.md`.
- Create architecture decision records for queue semantics, storage, process execution and config reload.
- Finalize crate boundaries and API contracts.

### Phase 1 — Domain and configuration

- Core models and state machine.
- Configuration parser and validation.
- Legacy config translation.
- Request context and trigger-rule model.

### Phase 2 — Synchronous compatibility

- Implement and verify the no-flag synchronous default and explicit `--sync` behavior.
- Ensure synchronous-only operation has no queue-backend dependency.
- HTTP listener and hook routing.
- Request parsing and value extraction.
- Trigger evaluation.
- Safe process execution.
- Response compatibility.
- Compatibility regression suite.

### Phase 3 — SQLite queue

- Schema and migrations.
- Durable enqueue.
- Atomic claim.
- Leases and heartbeats.
- Retry scheduling.
- Recovery and idempotency.

### Phase 4 — Redis-protocol backend

- Valkey support.
- Redis support.
- Streams and recovery.
- Delayed scheduling.
- Shared conformance suite.

### Phase 5 — Worker and asynchronous mode

- Implement `--async` global fallback and per-hook `async` overrides with tested precedence.
- Fail startup clearly when async execution is selected without a usable queue backend.
- Bounded workers.
- Execution snapshots.
- Durable request artifacts.
- Retry/dead-letter.
- Cancellation and graceful shutdown.

### Phase 6 — API and CLI

- Job APIs.
- Worker and hook APIs.
- Health/readiness/metrics.
- CLI management commands.
- OpenAPI documentation.

### Phase 7 — Integrated UI

- Askama templates.
- HTMX interactions.
- SSE.
- Dashboard and job explorer.
- Hook/worker/dead-letter views.
- Authentication, authorization and audit.

### Phase 8 — Hardening and distribution

- Security review.
- Crash/failure injection.
- Cross-platform builds.
- systemd packaging.
- Upgrade, backup and restore documentation.
- Release automation.

### Phase 9 — Compatibility sign-off

- Run all compatibility fixtures.
- Resolve all critical differences.
- Publish compatibility matrix.
- Mark only verified features as compatible.

---

## 16. Definition of done

A release candidate is acceptable only when:

1. The upstream compatibility inventory is complete for the selected baseline.
2. All applicable documented hook behavior is implemented and tested.
3. Synchronous mode preserves the verified legacy contract.
4. Asynchronous jobs are durably accepted before HTTP 202.
5. SQLite works without an external service.
6. Valkey and Redis both pass the shared backend conformance suite.
7. Workers recover safely after crashes and lease expiry.
8. Retry, idempotency, cancellation and dead-letter behavior are tested.
9. Arbitrary request-driven command execution is prevented.
10. The integrated UI uses Askama, HTMX and lightweight assets, not a SPA framework.
11. UI operations use real authenticated backend APIs.
12. Linux x86_64 and ARM64 builds and tests pass; macOS targets are validated.
13. Deployment, security, backup and recovery documentation is complete.
14. No feature is marked complete without implementation and verification evidence.

**Release rule:** Do not advertise “full adnanh/webhook compatibility” until the compatibility matrix shows all applicable features as verified or explicitly documents a platform limitation.
