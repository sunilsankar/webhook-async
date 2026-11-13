# AGENTS.md — Instructions for Coding Agents

This file defines mandatory engineering rules for all agents working on `webhook-async`.

## 1. Project mission

Build `webhook-async`, a standalone Rust webhook receiver and asynchronous job engine.

The project must:

1. Independently implement the verified externally observable feature set of `adnanh/webhook`.
2. Preserve legacy synchronous hook behavior.
3. Add durable asynchronous job execution.
4. Support SQLite, Valkey and Redis through selectable storage backends.
5. Provide a lightweight integrated management UI built primarily in Rust.
6. Ship as a standalone executable without a mandatory Docker or Node.js runtime.

Read `SPECIFICATION.md` before making architectural or implementation changes.

## 2. Mandatory references

- Specification: `SPECIFICATION.md`
- Architecture: `ARCHITECTURE.md` (create/update when needed)
- Compatibility inventory: `docs/COMPATIBILITY.md` (must be created in Phase 0)
- Task plan: `TASKS.md` (must be created and maintained)
- Upstream reference: https://github.com/adnanh/webhook
- UI references: https://github.com/mher/flower and https://github.com/hook0/hook0
- Valkey: https://valkey.io/
- Redis: https://redis.io/

Do not assume repository behavior from a README alone. Inspect upstream docs, configuration examples and tests. Pin the exact upstream release/commit used as the compatibility baseline.

## 3. Non-negotiable constraints

- Use stable Rust.
- Use Tokio for asynchronous runtime.
- Use Axum for HTTP.
- Use Askama for server-rendered templates.
- Use HTMX for ordinary UI interactions.
- Use SSE for live operational updates.
- Use lightweight CSS and minimal vanilla JavaScript.
- Do not introduce React, Next.js, Vue, Angular, Svelte or a frontend SPA framework.
- Do not require Node.js at runtime.
- Do not require Docker at runtime.
- SQLite is the default storage backend.
- Valkey is the preferred self-hosted Redis-compatible backend.
- Redis remains supported.
- Valkey and Redis share one protocol adapter where verified.
- Never silently switch storage backends.
- Preserve synchronous behavior for compatible legacy hooks.
- Default to synchronous execution; async is explicit opt-in through `server --async` or a per-hook `async: true` override.
- Support `server --sync`; reject simultaneous `--sync` and `--async`.
- Asynchronous acceptance must be durable before HTTP 202.
- Execution semantics are at-least-once, never claim exactly-once.
- Worker concurrency must be bounded.
- Do not spawn unbounded tasks per webhook.
- Do not hold database transactions while a command executes.
- Do not execute arbitrary commands supplied by webhook payloads.
- Do not claim compatibility without regression tests.
- Do not create UI controls that are not connected to real backend operations.

## 4. Work before coding

At the beginning of a task:

1. Read this file and the relevant sections of `SPECIFICATION.md`.
2. Inspect the current workspace and existing code.
3. Read `TASKS.md` and identify dependencies.
4. Check `docs/COMPATIBILITY.md` for relevant upstream behavior.
5. Identify affected crates and public interfaces.
6. Write a concise implementation plan for non-trivial changes.
7. Identify security, data migration and backward-compatibility implications.

Do not overwrite existing work or restructure the workspace without inspecting it first.

If a required behavior is ambiguous, inspect the pinned upstream source/docs/tests. If still ambiguous, record the question in the compatibility inventory rather than guessing.

## 5. Architecture and dependency rules

### 5.1 Dependency direction

Keep dependencies flowing inward:

- `core` contains domain models, IDs, state transitions and shared errors.
- `config` parses and validates configuration.
- `compat` translates upstream-compatible configuration and semantics.
- `hook-engine` handles request context, value extraction and trigger evaluation.
- `queue` coordinates backend-neutral queue behavior.
- `storage` defines storage contracts.
- `storage-sqlite` implements SQLite.
- `storage-redis` implements the shared Valkey/Redis protocol adapter.
- `worker` claims and executes jobs.
- `executor` runs allowlisted tasks.
- `api` exposes REST endpoints and authentication.
- `web` renders Askama templates and HTMX/SSE responses.
- `cli` wires the application together.

The core/domain crate must not depend on Axum, SQLx, Redis client types, Askama or UI code.

Avoid circular crate dependencies. Put shared interfaces in the narrowest appropriate crate.

### 5.2 Keep domain types backend-neutral

Never leak the following into core/domain APIs:

- SQL row types.
- SQLite connection/transaction types.
- Redis stream IDs.
- Redis keys.
- Redis client types.
- Backend-specific serialized payloads.

Backend-specific metadata may exist behind explicit adapter types, but domain semantics must remain backend-neutral.

### 5.3 Avoid unnecessary abstraction

Use traits at real architectural boundaries: storage, executor, clock where deterministic tests need it, and other justified seams.

Do not add abstraction layers solely for theoretical future extensibility. Document why each public trait exists.

## 6. Upstream compatibility rules

Full feature compatibility is a release requirement.

Before implementing an upstream behavior:

1. Locate its documentation and tests in the pinned upstream baseline.
2. Record exact syntax, defaults and edge cases in `docs/COMPATIBILITY.md`.
3. Add a fixture or test describing the expected behavior.
4. Implement the behavior in the compatibility layer or hook engine.
5. Run the regression test.
6. Mark the inventory item `TESTED_COMPATIBLE` only after it passes.

Preserve:

- Configuration field names and defaults where applicable.
- Request-value source semantics.
- Trigger-rule operators and nesting.
- Signature validation behavior.
- Synchronous response semantics.
- Command argument and environment construction.
- Temporary-file lifecycle.
- CLI flags and defaults where applicable.
- Reload, listener and signal behavior where supported.

Do not silently ignore unknown configuration fields or unsupported flags.

If a platform cannot support a feature, return a clear error or warning and document the limitation. Never report successful activation when a feature was ignored.

Do not copy upstream code unless license review confirms the intended use and all required notices/conditions are satisfied. Prefer independent implementation from documented behavior and tests.


## 7A. Execution-mode invariants

These rules are mandatory:

- Asynchronous execution is opt-in. The server defaults to synchronous execution when neither global flag nor per-hook override selects async.
- Implement `server --async` and `server --sync`; reject both flags together before binding the listener.
- Resolve mode using this exact precedence: explicit per-hook `async` boolean, then global CLI mode, then synchronous default.
- An omitted per-hook `async` value inherits the global mode; explicit `false` must override global `--async`.
- Preserve the synchronous response contract and execution timing required by the pinned upstream compatibility baseline.
- The synchronous code path must not enqueue jobs, initialize queue workers, or require SQLite/Valkey/Redis configuration or connectivity.
- Async mode must persist the immutable execution snapshot and required artifacts before returning HTTP 202.
- Never return HTTP 202 if enqueue/commit fails or queue capacity is exhausted.
- `--async` without a valid configured queue backend must fail clearly at startup.
- A queued job's execution mode and execution snapshot must not change after configuration reload.
- Add tests for no-flag default, both flags, mutual exclusion, per-hook/global precedence, mixed-mode operation, async storage failure, and synchronous operation with no queue service available.
- Do not add a configuration migration that silently turns existing synchronous hooks asynchronous.

## 7. Request processing rules

The request pipeline should have explicit stages:

1. Route and method match.
2. Apply request size and content-type limits.
3. Preserve raw bytes where required.
4. Authenticate/validate signatures.
5. Parse headers, query, body and multipart data.
6. Construct normalized request context.
7. Evaluate trigger rules.
8. Resolve arguments, environment and file artifacts.
9. Build an immutable execution specification.
10. Execute synchronously or durably enqueue asynchronously.

Do not enqueue a request that fails authentication, parsing, validation or trigger rules.

Do not mutate the request payload during signature verification.

Do not log raw request bodies or sensitive headers by default.

## 8. Process execution security

- Use direct process execution with an explicit executable and argument vector.
- Never interpolate request values into shell command strings.
- Only administrators may configure executable paths and allowed task definitions.
- Request payloads may fill validated arguments/environment values only.
- Validate working directories and file paths.
- Apply execution timeouts.
- Bound stdout and stderr capture.
- Avoid unbounded buffering.
- Clean up temporary files.
- Handle child processes/process groups where supported.
- Record exit status and safe error summaries.
- Never run the service as root by default.

If shell execution is required for a legacy configuration, make the shell executable explicit in administrator-controlled configuration. Never allow the request to select shell syntax or an executable.

## 9. Queue correctness rules

### 9.1 Delivery semantics

Guarantee at-least-once execution only. Exactly-once external side effects are impossible to guarantee in the general case.

Support idempotency keys, but document their scope and retention.

### 9.2 Durable acceptance

For asynchronous hooks, return HTTP 202 only after the job and required artifacts have been committed to the selected backend.

If the backend is unavailable, do not acknowledge acceptance.

If the queue is full, reject with the documented capacity response.

### 9.3 State transitions

All state transitions must be validated by shared domain logic.

Do not let each backend invent its own status semantics.

At minimum support:

- `queued`
- `running`
- `retry_wait`
- `succeeded`
- `failed`
- `cancel_requested`
- `cancelled`
- `dead_letter`

### 9.4 Leases

- Claims must be atomic.
- Each claim must have ownership metadata.
- Heartbeats extend leases.
- Acknowledgements must verify current ownership.
- A stale worker must not acknowledge work after reassignment.
- Expired jobs must be recovered according to documented policy.
- Lease durations must avoid false expiry for normal long-running tasks.
- Do not assume a missing heartbeat means a process has stopped immediately.

### 9.5 Retry and cancellation

- Use exponential backoff with jitter.
- Separate transient and permanent failures.
- Record every attempt.
- Enforce maximum attempts.
- Route exhausted jobs to dead-letter handling.
- Cancellation of running work is cooperative/best-effort and cannot undo external side effects.
- Make retry/cancel operations safe against concurrent updates.

## 10. Storage backend rules

### 10.1 SQLite

- Enable WAL where appropriate.
- Configure busy timeout.
- Enable foreign keys.
- Keep write transactions short.
- Use atomic claims.
- Index common status/scheduling queries.
- Never hold a transaction while executing a task.
- Respect SQLite's single-writer model.
- Provide migrations and backup/restore guidance.
- Test abrupt process termination and recovery.
- Do not claim equivalent distributed write scaling to Redis/Valkey.

### 10.2 Valkey and Redis

Use one shared Redis-protocol adapter for both, with explicit config selectors `valkey` and `redis`.

- Verify every command and script against supported versions of both servers.
- Prefer Streams and consumer groups for delivery.
- Handle pending entries and abandoned consumer recovery.
- Implement delayed retry scheduling explicitly.
- Use atomic transitions where required.
- Use a namespaced key prefix.
- Document server persistence and replication requirements.
- Test restart, failover assumptions and recovery.
- Do not assume protocol compatibility proves semantic compatibility.
- Do not silently downgrade to SQLite or another backend.

### 10.3 Storage conformance

Every storage implementation must pass the same conformance suite, including:

- Enqueue.
- Concurrent claim.
- Lease extension.
- Ownership-checked acknowledgement.
- Retry scheduling.
- Idempotency.
- Cancellation.
- Dead-letter transitions.
- Pagination.
- Expired lease recovery.
- Backend restart recovery.

If a backend cannot meet a required semantic, report the limitation and resolve the architecture before shipping it.

## 11. Configuration and secrets

- TOML is the new application-level configuration format.
- Preserve upstream JSON/YAML hook configuration support as required by the compatibility baseline.
- Validate configuration before starting listeners or workers.
- Provide useful field-specific errors.
- Do not log secrets or credential-bearing connection URLs.
- Support environment variables or protected secret files.
- Do not commit live credentials.
- Define reload behavior for in-flight jobs.
- Persist an immutable execution snapshot or version reference so reloads do not mutate already accepted jobs.
- Do not silently fall back to default values for invalid security-sensitive settings.

## 12. UI engineering rules

### 12.1 Technology

- Askama templates.
- HTMX for interactions.
- SSE for live updates.
- Lightweight CSS.
- Minimal vanilla JavaScript only when justified.
- Embed production assets in the executable.

No SPA framework and no Node.js production runtime.

### 12.2 UI behavior

All displayed production data must come from real APIs/domain services.

Never hardcode sample jobs or metrics into production views.

Use:

- Server-side pagination.
- Lazy loading for payloads/logs.
- Escaped templates.
- Clear loading, empty, error and stale states.
- Accessible labels and keyboard navigation.
- Responsive layouts.
- Light/dark themes.
- Explicit confirmation for destructive actions.

### 12.3 UI security

- Authentication and authorization are enforced by backend handlers.
- Protect cookie-based mutations against CSRF.
- Redact secrets and sensitive payload values.
- Do not expose arbitrary process execution.
- Audit administrative mutations.
- Do not trust identifiers or state supplied by the browser.
- Revalidate job state before retry/cancel operations.
- Do not claim a worker has stopped until confirmed.

### 12.4 UI performance

- Avoid frequent full-page refreshes.
- Use SSE with reconnect behavior and polling fallback.
- Avoid high-cardinality or unbounded event streams.
- Paginate large histories.
- Do not load full logs/payloads in list views.
- UI failure must not stop queue processing.

## 13. API rules

- Use versioned endpoints.
- Publish OpenAPI.
- Use consistent error response types.
- Validate pagination bounds.
- Authenticate administrative endpoints.
- Enforce authorization in the handler/service layer.
- Apply rate limits where appropriate.
- Avoid leaking internal errors, credentials or filesystem paths.
- Use request/job correlation IDs.
- Return HTTP 202 only after durable enqueue.
- Document synchronous versus asynchronous response behavior.

## 14. Rust coding standards

- Prefer safe Rust.
- Avoid `unsafe`; any required use needs a written justification and focused tests.
- Avoid `.unwrap()` and `.expect()` in production paths. They may be used in tests or proven invariants with clear rationale.
- Do not suppress compiler or Clippy warnings without justification.
- Use typed errors and preserve error sources.
- Do not use string matching as the primary error-control mechanism.
- Avoid blocking filesystem/process/database operations on Tokio executor threads.
- Use `tokio::process` for async child processes.
- Bound channels, queues and buffers.
- Avoid detached tasks without lifecycle ownership.
- Implement graceful cancellation and shutdown.
- Use structured tracing; avoid logging secrets and payload contents.
- Document public APIs and non-obvious invariants.
- Keep functions cohesive and avoid oversized modules.
- Minimize dependencies and review dependency maintenance/security.
- Pin or constrain dependencies responsibly; commit `Cargo.lock` for the application.

## 15. Testing requirements

Every feature must include tests at the appropriate level.

### Required checks

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Run relevant integration tests with actual SQLite and supported Valkey/Redis services.

### Required test categories

- Unit tests for domain and state transitions.
- Compatibility tests for upstream behavior.
- Configuration parsing and validation.
- Request parsing and trigger rules.
- API integration tests.
- Storage conformance tests.
- Concurrent queue tests.
- Crash/recovery tests.
- Lease and retry tests.
- Executor security tests.
- UI handler/template tests.
- Authentication, authorization, CSRF and XSS tests.
- Cross-platform build and runtime checks.

Do not weaken or delete a failing test merely to make CI green. Fix the defect or document a justified test correction.

## 16. Development workflow

Work in small, reviewable phases.

For each task:

1. Update `TASKS.md`.
2. Identify affected crates and interfaces.
3. Add or update tests first where practical.
4. Implement the smallest complete change.
5. Run formatting.
6. Run Clippy.
7. Run relevant unit/integration tests.
8. Run workspace tests when feasible.
9. Update documentation and compatibility inventory.
10. Summarize changed files, test commands/results and known limitations.

Do not mark work complete if tests were not run. State exactly which checks were skipped and why.

Do not implement multiple unrelated features in one large change.

Do not introduce speculative abstractions or unrelated refactors.

## 17. Documentation requirements

Keep these documents accurate:

- `SPECIFICATION.md`: product and technical requirements.
- `AGENTS.md`: engineering instructions.
- `ARCHITECTURE.md`: crate boundaries, data flow and decisions.
- `TASKS.md`: phased implementation checklist.
- `docs/COMPATIBILITY.md`: upstream feature inventory and verification evidence.
- `docs/API.md`: endpoints and schemas.
- `docs/CONFIGURATION.md`: all configuration fields and examples.
- `docs/STORAGE.md`: SQLite, Valkey and Redis behavior.
- `docs/SECURITY.md`: threat model and security controls.
- `docs/DEPLOYMENT.md`: installation and service operation.
- `docs/OPERATIONS.md`: monitoring, recovery, backup and troubleshooting.

Update documentation in the same task as behavior changes.

## 18. Definition of done

A task is complete only when:

- Implementation is present.
- Tests cover expected behavior and important failures.
- Formatting and Clippy pass for affected workspace scope.
- Relevant tests pass.
- Documentation is updated.
- Security and compatibility implications are addressed.
- No placeholder or fake production behavior remains.
- Known limitations are explicitly reported.

The project is not ready to claim full upstream compatibility until the compatibility inventory is complete and all applicable features are verified.

## 19. Agent response format

At the end of each implementation phase, report:

1. What was implemented.
2. Files/crates changed.
3. Compatibility features covered.
4. Tests and exact commands run.
5. Test results.
6. Security considerations.
7. Known limitations.
8. Next tasks from `TASKS.md`.

Be precise. Do not claim success based only on code generation or compilation.
