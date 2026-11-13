CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY NOT NULL,
    hook_id TEXT NOT NULL,
    state TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    next_retry_at TEXT,
    idempotency_key TEXT UNIQUE,
    idempotency_expires_at TEXT,
    worker_id TEXT,
    lease_token TEXT,
    lease_expires_at TEXT,
    heartbeat_at TEXT,
    execution_spec_json TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request_method TEXT NOT NULL,
    remote_addr TEXT,
    exit_code INTEGER,
    stdout_preview TEXT,
    stderr_preview TEXT,
    error_class TEXT,
    error_summary TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_due_claim
    ON jobs (available_at, created_at)
    WHERE state = 'queued';

CREATE INDEX IF NOT EXISTS idx_jobs_retry_due
    ON jobs (next_retry_at)
    WHERE state = 'retry_wait';

CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry
    ON jobs (lease_expires_at)
    WHERE state IN ('running', 'cancel_requested');

CREATE INDEX IF NOT EXISTS idx_jobs_list_created
    ON jobs (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_jobs_hook_state
    ON jobs (hook_id, state, created_at DESC);

CREATE TABLE IF NOT EXISTS job_attempts (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL,
    worker_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    exit_code INTEGER,
    error_class TEXT,
    error_summary TEXT,
    stdout_preview TEXT,
    stderr_preview TEXT,
    PRIMARY KEY (job_id, attempt)
);
