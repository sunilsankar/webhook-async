# Operations & Maintenance Guide for `webhook-async`

This document describes how to monitor, operate, reload, backup, and troubleshoot `webhook-async` in production.

---

## 1. Process Lifecycle & Signals

`webhook-async` provides signal handling for live reloads and graceful termination:

| Signal | Action | Description |
|---|---|---|
| `SIGHUP` | Reload Hooks | Re-reads and re-evaluates all configured hook definitions and templates. |
| `SIGUSR1` | Reload Hooks | Alternative hook reload signal (identical to `SIGHUP`). |
| `SIGINT` (`Ctrl+C`) | Graceful Shutdown | Stops accepting new requests, drains background tasks, allows workers up to `shutdown_grace_seconds` to finish, cleans up socket and pidfile. |
| `SIGTERM` | Graceful Shutdown | Standard system termination signal initiating graceful shutdown. |

### Hot Reload (`-hotreload` / Signals)
- When `-hotreload` is specified (or when `SIGHUP`/`SIGUSR1` is received), `webhook-async` re-reads hook definitions.
- **Atomic Swap**: New definitions are validated before activation. If parsing or validation fails, the error is logged and existing hooks remain completely active without downtime.
- **Execution Snapshot Invariance**: Any job already enqueued or executing retains its original, immutable `ExecutionSpec`. Config reloads never mutate in-flight or queued work.

---

## 2. Health Checks & Monitoring

The administrative REST API exposes dedicated monitoring endpoints:

### Liveness Probe (`GET /api/v1/health`)
Unauthenticated endpoint returning HTTP 200:
```bash
curl -i http://localhost:9000/api/v1/health
```
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

### Readiness Probe (`GET /api/v1/ready`)
Unauthenticated endpoint verifying storage backend connectivity:
```bash
curl -i http://localhost:9000/api/v1/ready
```
- Returns `HTTP 200` (`{"status":"ready","backend":"sqlite"}`) when healthy.
- Returns `HTTP 503` if the database or Valkey/Redis cluster cannot be reached.

### Queue Metrics (`GET /api/v1/stats`)
Requires Bearer admin token:
```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9000/api/v1/stats
```
```json
{
  "pending_count": 3,
  "counts_by_state": {
    "queued": 2,
    "running": 1,
    "retry_wait": 0,
    "succeeded": 1420,
    "failed": 3,
    "cancel_requested": 0,
    "cancelled": 2,
    "dead_letter": 1
  }
}
```

### Real-time SSE Stream (`GET /api/v1/events`)
Streams live JSON stats snapshots:
```bash
curl -N -H "Authorization: Bearer $ADMIN_TOKEN" http://localhost:9000/api/v1/events
```

---

## 3. Command Line Management

Manage jobs and inspect storage using the built-in CLI commands:

### Storage Health
```bash
# Formatted table output
webhook-async storage status --config /etc/webhook-async/config.toml

# Machine-readable JSON output
webhook-async storage status --config /etc/webhook-async/config.toml --json
```

### Listing Jobs
```bash
# List recent jobs
webhook-async jobs list --config /etc/webhook-async/config.toml

# Filter by state and limit
webhook-async jobs list --state failed --limit 10 --config /etc/webhook-async/config.toml
```

### Inspecting Job Details
```bash
webhook-async jobs get <job-id> --config /etc/webhook-async/config.toml
```

### Retrying and Cancelling Jobs
```bash
# Manually retry a failed or dead-letter job
webhook-async jobs retry <job-id> --config /etc/webhook-async/config.toml

# Cancel a queued or running job
webhook-async jobs cancel <job-id> --config /etc/webhook-async/config.toml
```

---

## 4. Logging & Diagnostics

- **Log Level**: Default is `info`. Pass `--verbose` for detailed execution logs or `--debug` for low-level trace diagnostics.
- **Log File Redirection**: Pass `--logfile /var/log/webhook-async/server.log` to append logs to a dedicated file instead of stdout.
- **Panic Protection**: Pass `--nopanic` to recover gracefully from unforeseen handler panics without terminating the service process.
- **PID File**: Pass `--pidfile /run/webhook-async/webhook.pid` for process managers that track PIDs via files.

---

## 5. Storage Backup & Recovery

### SQLite Hot Backup (`VACUUM INTO`)
SQLite databases in WAL mode can be safely backed up online without locking readers or writers:
```bash
sqlite3 /var/lib/webhook-async/webhook.db \
  "VACUUM INTO '/var/backups/webhook-async-$(date +%Y%m%d%H%M%S).db'"
```

To restore from a backup:
1. Stop the `webhook-async` service (`systemctl stop webhook-async`).
2. Replace `/var/lib/webhook-async/webhook.db` with the backup file.
3. Remove stale `-shm` and `-wal` auxiliary files if present.
4. Restart the service (`systemctl start webhook-async`).

### Valkey & Redis Persistence
For Valkey and Redis backends:
- Ensure AOF persistence is enabled: `appendonly yes` and `appendfsync everysec`.
- Snapshotting (`save 60 1000`) should be active as an additional recovery layer.
- Workers automatically recover expired leases (`recover_expired_leases`) and promote scheduled retries (`promote_due_retries`).

---

## 6. Troubleshooting

| Symptom | Cause | Resolution |
|---|---|---|
| HTTP 429 Too Many Requests | Queue capacity exceeded (`max_pending_jobs`). | Scale up worker concurrency or investigate downstream command bottlenecks. |
| HTTP 503 Service Unavailable | Storage backend unreachable or admin token unconfigured. | Verify database connectivity or ensure `admin_token_env` / `admin_token_file` is set. |
| HTTP 405 Method Not Allowed | Request HTTP method not permitted by hook. | Check `http-methods` on the hook or `--http-methods` global flag. |
| HTTP 400 Hook rules were not satisfied | Parameter missing or rule mismatch. | Verify request payload, headers, or signature HMAC secrets. |
| Jobs stuck in `running` after worker crash | Worker terminated abruptly without heartbeat. | Worker recovery loops automatically release expired leases after `lease_duration_seconds`. |
| Dead-lettered jobs | Non-existent executable, invalid working directory, or retries exhausted. | Check `error_class` and `error_summary` via `webhook-async jobs get <id>`. |
