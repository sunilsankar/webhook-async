# Security Policy & Threat Model (`docs/SECURITY.md`)

This document details the security architecture, threat model, and protective controls implemented in `webhook-async`.

## 1. Threat Model & Trust Boundaries

```
[ Untrusted Internet / Webhook Senders ]
                   │
                   ▼ (HTTP/HTTPS)
    ┌──────────────────────────────┐
    │  Axum HTTP Webhook Receiver  │
    └──────────────┬───────────────┘
                   │  1. Size limits (max_body_bytes, max_multipart_mem)
                   │  2. Signature validation (HMAC-SHA1/256/512, Scalr)
                   │  3. Rule matching & IP whitelisting
                   ▼
    ┌──────────────────────────────┐
    │  Durable Job Storage Engine  │ (SQLite / Valkey / Redis)
    └──────────────┬───────────────┘
                   │  Immutable ExecutionSpec snapshot
                   ▼
    ┌──────────────────────────────┐
    │   Direct Process Executor    │ (No shell interpolation)
    └──────────────────────────────┘
                   ▲
                   │  Authenticated Admin Access (Bearer / Session)
[ Administrator / Operations Dashboard ]
```

### Trust Assumptions
- **Webhook Payloads**: Fully untrusted. May contain malicious payloads, malformed JSON/XML, or oversized bodies designed to cause denial of service.
- **Hook Definitions**: Controlled exclusively by system administrators via configuration files (`hooks.json`, `config.toml`). Payloads can supply argument and environment values, but **cannot select executables or shell command strings**.
- **Admin Endpoints**: Restricted to authenticated operators via Bearer token or Web UI session cookie.

## 2. Core Security Controls

### 2.1 Process Execution Security
- **Direct Process Execution**: Commands are executed directly via `tokio::process::Command` using explicit argument vectors (`args`). **No shell is invoked** by default, preventing command-injection attacks.
- **Working Directory Verification**: Working directories are validated before process execution; non-existent directories fail immediately.
- **Process Group Isolation & Timeout Termination**: Child processes are executed in their own Unix process group (`process_group(0)`). On timeout or cancellation, `SIGKILL` is sent to the negative process group (`-pgid`), terminating runaway child and descendant processes.
- **Output Bounding**: Captured stdout and stderr are strictly bounded (`max_output_bytes`, default 64 KiB), preventing memory exhaustion from noisy commands.

### 2.2 Storage & Queue Durability
- **Immutable Execution Snapshots**: Accepted asynchronous jobs store an immutable snapshot of all command arguments, environment variables, and temporary file contents before returning HTTP 202. Configuration reloads or disk edits cannot mutate in-flight jobs.
- **SQL Injection Prevention**: All SQLite queries use parameterized bindings (`sqlx::query(...).bind(...)`).
- **Atomic Claim & Ownership Tokens**: Workers claim jobs using atomic leases (`claim_due`) with unique UUID lease tokens. Heartbeats, acknowledgements, and cancellations verify lease ownership, preventing split-brain execution after lease reassignment.

### 2.3 Administrative REST API Security
- **Secure by Default**: If no admin token is configured in `[auth]`, administrative endpoints (`/api/v1/*`) reject all requests with `503 Service Unavailable`, preventing accidental open admin endpoints.
- **Constant-Time Token Comparison**: Admin authentication tokens are evaluated using `subtle::ConstantTimeEq`, preventing timing attacks.
- **Rate Limiting**: An in-memory fixed-window rate limiter bounds admin requests to 60 requests/minute per token, returning `429 Too Many Requests` with a `Retry-After: 60` header upon exhaustion.
- **Credential & Secret Redaction**: Hook definitions exposed via `GET /api/v1/hooks` redact trigger rule secrets, HMAC keys, and environment variables. Storage connection URLs mask credentials in logs and API responses.

### 2.4 Management Web UI Security
- **Askama Server-Side Escaping**: All dynamic content rendered through Askama templates is automatically HTML-entity escaped, preventing Cross-Site Scripting (XSS).
- **Session Protection**: Web UI sessions are stored in an in-memory session cache with 24-hour expiration. Session identifiers are set with `HttpOnly` and `SameSite=Lax` cookie flags.
- **CSRF Defense**: All mutating web endpoints (`/jobs/{id}/retry`, `/jobs/{id}/cancel`, `/logout`) require a matching per-session CSRF token passed via `X-CSRF-Token` header or form body. Requests without a valid token receive `403 Forbidden`.
- **Default Loopback Binding**: The management UI defaults to binding to `127.0.0.1:9001`, keeping operator traffic isolated from external webhook traffic on `0.0.0.0:9000`.

## 3. Cryptographic Implementations
- **HMAC Signatures**: Verified using standard HMAC-SHA1, HMAC-SHA256, and HMAC-SHA512 implementations. Comparison of calculated MAC and received signature is performed in constant time; expected MAC values are never printed in error messages.
- **Configuration Hashing**: Configuration snapshots use SHA-256 (`sha256:<hex>`).
