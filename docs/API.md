# API Reference (`docs/API.md`)

Current scope: **Phase 2 (Synchronous Webhook Receiver) and Phase 5 (Durable Asynchronous Receiver & Execution Engine)**. Management endpoints (`/api/v1/*`) are scheduled for Phase 6 (`TASKS.md`).

## 1. Webhook Receiver Endpoints

### `GET /` (and other methods to `/`)

Health/liveness probe compatible with `adnanh/webhook`.

- Response: `200 OK`, body `"OK"`, plus configured global response headers (`[server].response_headers` / `--header`).

### `/{url_prefix}/{hook_id}` (or `/{hook_id}` when `url_prefix` is empty)

Executes a matched hook loaded from configured JSON/YAML hook files. Default `url_prefix` is `hooks` (e.g. `POST /hooks/github`, `POST /hooks/sendgrid/dir`).

#### Request Pipeline & Status Codes

1. **Hook lookup**: Unknown hook ID returns `404 Not Found` (`"Hook not found."`).
2. **HTTP method check**: If `hook.http-methods` is non-empty, only those uppercased methods are allowed; otherwise `[server].default_http_methods` (`--http-methods`) applies when non-empty. Mismatch returns `405 Method Not Allowed` with empty body.
3. **Body size limit**: Request bodies exceeding `[server].max_body_bytes` (default `10 MiB`) return `413 Payload Too Large`.
4. **Payload parsing**: Uses `hook.incoming-payload-content-type` when set, else request `Content-Type`:
   - `application/json`: object or array (arrays wrapped at `payload.root`). Parse failure is logged and payload stays unset (matches `adnanh/webhook` v2.8.3).
   - `application/x-www-form-urlencoded`: first value per key.
   - `application/xml` / `text/xml`: `mxj` attributes (`-attr`), text (`#text`), repeated sibling tags as arrays.
   - `multipart/form-data`: parses form fields and JSON file parts (`application/json` or listed in `parse-parameters-as-json`) up to `[server].max_multipart_mem_bytes`; failure returns `500 Internal Server Error`.
   - `parse-parameters-as-json`: decodes embedded JSON strings in headers, query, or payload.
5. **Trigger rule evaluation**:
   - Missing parameter (`ParameterMissing`) or rule `false` returns `hook.trigger-rule-mismatch-http-response-code` (invalid HTTP status codes fall back to `200`) with body `"Hook rules were not satisfied."`.
   - Unswallowed evaluation errors (invalid regex, invalid IP/CIDR, invalid/missing HMAC secret, HMAC signature mismatch when not tolerated by `or` + `trigger-signature-soft-failures: true`) return `500 Internal Server Error` with body `"Error occurred while evaluating hook rules."`.
6. **Execution & response (Synchronous mode — Phase 2)**:
   - Global headers (`--header`) are applied first, then hook-level `response-headers`.
   - When `include-command-output-in-response: true`: waits for direct process execution up to `timeout-seconds` (or `[queue].default_timeout_seconds`).
     - Exit code `0`: returns `hook.success-http-response-code` (invalid codes fall back to `200`) with combined stdout+stderr.
     - Non-zero / execution error: returns `500 Internal Server Error` with command output if `include-command-output-in-response-on-error: true`, else `"Error occurred while executing the hook's command."`.
   - When `include-command-output-in-response: false`: acquires a bounded background permit (`DEFAULT_MAX_SYNC_BACKGROUND_TASKS = 64` in `webhook-async-api`) and returns `hook.success-http-response-code` (default `200`) with `hook.response-message` immediately. If the bounded concurrency limit is full, returns `503 Service Unavailable`.

## 2. Asynchronous Execution Pipeline (Phase 5)

When a hook executes asynchronously (via per-hook `async: true` or global `server --async`), the request follows the same validation, authentication, and parsing pipeline through step 5 above, then:

1. **Immutable Execution Snapshot**: An `ExecutionSpec` is constructed containing resolved argv, environment variables, temp-file payloads, working directory, timeout, max output bytes, and `config_version`.
2. **Idempotency Key Extraction**: If present in headers (`Idempotency-Key` or `X-Idempotency-Key`), the key is recorded with the job and enforces atomic deduplication within `[queue].idempotency_retention_seconds`. Duplicate requests return HTTP `202 Accepted` pointing to the existing job.
3. **Durable Acceptance**: The job is committed to storage before acknowledging acceptance.
   - On successful commit: returns `202 Accepted` (`Content-Type: application/json`):
     ```json
     {
       "job_id": "0194ff72-...",
       "status": "queued",
       "message": "Job enqueued successfully."
     }
     ```
   - On queue capacity saturation (`max_pending_jobs` reached): returns `429 Too Many Requests` (`"Queue capacity full."`).
   - On storage failure: returns `503 Service Unavailable` (`"Queue backend unavailable."`), never `202 Accepted`.
4. **Worker Processing**: Background workers claim due jobs with atomic leases, send periodic heartbeats, monitor cooperative cancellation (`CancelRequested`), execute tasks in direct child process groups, stream bounded output, and record attempts and transitions to `succeeded`, `retry_wait`, `failed`, `cancelled`, or `dead_letter`.

## 3. Management REST API Endpoints (Phase 6)

All administrative endpoints are mounted under `/api/v1/*`.

### Authentication & Authorization
- **Public endpoints** (no authentication required):
  - `GET /api/v1/health`: basic liveness check (`{"status":"ok","version":"0.1.0"}`).
  - `GET /api/v1/ready`: storage readiness probe (`{"ready":true,"backend":"sqlite"}`).
  - `GET /api/v1/openapi.json`: OpenAPI 3.1 specification schema.
- **Administrative endpoints** (require Bearer authentication):
  - Request header: `Authorization: Bearer <admin-token>`.
  - Admin token is loaded via `[auth].admin_token_env` or `[auth].admin_token_file`.
  - **Secure by default**: If no admin token is configured, administrative endpoints return `503 Service Unavailable` (`"Admin token not configured."`).
  - Missing token header: returns `401 Unauthorized` with `WWW-Authenticate: Bearer`.
  - Invalid token: returns `403 Forbidden` (`"Invalid admin authorization token."`) checked using constant-time comparison.
  - Rate limiting: in-memory fixed-window rate limiter permits up to 60 requests/minute per token; breaches return `429 Too Many Requests` with `Retry-After: 60`.

### Administrative Endpoints Summary

- `GET /api/v1/stats`: Returns current queue counts by state and pending job counts.
- `GET /api/v1/jobs?state=<state>&hook=<hook_id>&limit=<n>&offset=<m>`: Paginated list of jobs with filtering.
  - Query parameters:
    - `state`: optional job state (`queued`, `running`, `retry_wait`, `succeeded`, `failed`, `cancel_requested`, `cancelled`, `dead_letter`).
    - `hook`: optional hook ID string.
    - `limit`: items per page (1..=500, default 50).
    - `offset`: page offset (default 0).
  - Response: `JobPage` JSON containing `items`, `total`, `limit`, and `offset`.
- `GET /api/v1/jobs/{id}`: Detailed job record and attempt execution history.
  - Returns `404 Not Found` if job does not exist.
  - Response contains job metadata, execution spec summary, and an array of `attempts` with duration, worker ID, exit code, and stdout/stderr previews.
- `POST /api/v1/jobs/{id}/retry`: Retries a terminal job in `failed` or `dead_letter` state.
  - Transitions job to `queued` with attempt count monotonic and `max_attempts` bumped if needed.
  - Returns `409 Conflict` if job is not in a retryable state.
- `POST /api/v1/jobs/{id}/cancel`: Requests cancellation of a `queued`, `running`, or `retry_wait` job.
  - `queued` or `retry_wait` jobs transition immediately to `cancelled`.
  - `running` jobs transition to `cancel_requested`; the assigned worker detects this on its next heartbeat, terminates the process group via `SIGKILL`, and transitions to `cancelled`.
  - Returns `409 Conflict` if job is already in a terminal state.
- `GET /api/v1/hooks`: Lists all configured hooks. Sensitive trigger secrets and HMAC keys are omitted.
- `GET /api/v1/workers`: Lists active workers registered in the in-memory `WorkerRegistry`.
- `GET /api/v1/storage/status`: Storage backend kind, capabilities (`supports_priority`, `supports_multi_host`, `supports_idempotency`), ping status, and queue stats.
- `GET /api/v1/events`: Server-Sent Events (SSE) stream pushing queue stats snapshots every 2 seconds (`data: {"counts_by_state": ...}`).

## 4. Management Web UI (Phase 6)

The web dashboard is powered by Askama templates, HTMX, and minimal vanilla CSS:
- Binds to `[ui].bind` (default `127.0.0.1:9001`) under `[ui].path_prefix` (default `""`).
- Session-based authentication: logging in with the admin token sets an `HttpOnly`, `SameSite=Lax` session cookie (`session_id`).
- All state-mutating requests (`/jobs/{id}/retry`, `/jobs/{id}/cancel`, `/logout`) are protected with per-session CSRF tokens (`X-CSRF-Token` header or form field).
- Live metrics and job status indicators update automatically over SSE (`/events`).

