# Configuration Reference (`docs/CONFIGURATION.md`)

`webhook-async` uses:
1. **Upstream JSON/YAML hook definition files** (`--hooks`, default `hooks.json`) for hook definitions (`adnanh/webhook` v2.8.3 compatible, plus optional `async`, `timeout-seconds`, `max-attempts` extensions).
2. **Optional TOML application config** (`--config webhook-async.toml`) for server, queue, storage, worker, UI, and auth settings.

All unknown TOML or hook JSON/YAML fields are rejected explicitly (`deny_unknown_fields`).

## 1. Execution Mode Precedence (`AGENTS.md` §7A)

1. Explicit per-hook `async: true | false` in hook JSON/YAML.
2. Global CLI mode flag (`server --async` or `server --sync`).
3. Synchronous default (`sync`).

Rules:
- Passing `--sync` and `--async` together fails validation before binding.
- Purely synchronous deployments do **not** require `[storage]` configuration or database/Valkey/Redis connectivity.
- Any configuration resolving at least one hook to async (via `--async` or `async: true`) requires a valid `[storage]` section.

## 2. CLI Flags & Upstream Flag Compatibility

`webhook-async server` accepts both standard `--flag` and `adnanh/webhook`-style single-dash flags (`-hooks`, `-ip`, `-port`, `-urlprefix`, `-header`, `-http-methods`, `-x-request-id`, `-x-request-id-limit`, `-max-multipart-mem`, `-verbose`, `-debug`).

| Flag | Default | Description |
|---|---|---|
| `--config <path>` | unset | Optional TOML application configuration file. |
| `--hooks <path>` | `hooks.json` | Repeatable JSON/YAML hook file path. |
| `--sync` / `--async` | `sync` | Global execution mode (mutually exclusive). |
| `--ip <addr>` | `0.0.0.0` | Bind IP address (overrides `[server].host`). |
| `--port <port>` | `9000` | Bind port (overrides `[server].port`). |
| `--urlprefix <prefix>` | `hooks` | URL prefix (`/{prefix}/{id}`; empty string routes `/{id}`). |
| `--header <Name=Value>` | empty | Repeatable global response header (accepts `Name=Value` or `Name: Value`). |
| `--http-methods <LIST>` | empty (all) | Comma-separated default allowed HTTP methods when hook omits `http-methods`. |
| `--x-request-id` | `false` | Use incoming `X-Request-Id` header when present. |
| `--x-request-id-limit <n>` | `0` (unlimited) | Truncate `X-Request-Id` to `n` bytes when `> 0`. |
| `--max-multipart-mem <n>` | `1048576` | Max bytes for `multipart/form-data` parsing. |

Other Phase 2 subcommands:
- `webhook-async config validate [--config <path>] [--hooks <path>...] [--sync|--async]`
- `webhook-async version` (`--version`, `-version`)

Deferred flags (Phase 7: `--template`, `--hotreload`, `--secure`, `--cert`, `--key`, `--tls-min-version`, `--cipher-suites`, `--list-cipher-suites`, `--socket`, `--pidfile`, `--setuid`, `--setgid`, `--logfile`, `--nopanic`) return a clear error instead of being silently ignored.

## 3. TOML Application Schema (`--config`)

### `[server]`
- `host`: bind host/IP (default `"0.0.0.0"`).
- `port`: bind port (default `9000`, `1..=65535`).
- `url_prefix`: webhook path prefix (default `"hooks"`).
- `max_body_bytes`: max HTTP body size in bytes (default `10485760` = 10 MiB, `> 0`).
- `max_multipart_mem_bytes`: max multipart body memory in bytes (default `1048576` = 1 MiB, `> 0`).
- `hook_files`: array of JSON/YAML hook file paths (default `[]`; CLI `--hooks` overrides when passed).
- `hot_reload`: boolean (default `false`; active in Phase 7).
- `default_http_methods`: default allowed HTTP methods list (default `[]` = all methods).
- `response_headers`: array of `{ name = "...", value = "..." }` global headers.
- `use_x_request_id`: boolean (default `false`).
- `x_request_id_limit`: integer bytes (default `0` = unlimited).
- `trust_proxy_headers`: boolean (default `false`).

### `[storage]` (Required only when async execution is enabled; SQLite active since Phase 3, Valkey/Redis active in Phase 4)
- `backend`: `"sqlite"` | `"valkey"` | `"redis"`.
- `[storage.sqlite]`: `database_url` (file path or SQLite URL, e.g. `"sqlite://webhook.db"` or `"/var/lib/webhook-async/queue.db"`), `wal_mode` (default `true`), `busy_timeout_ms` (default `5000`, `> 0`).
- `[storage.valkey]` / `[storage.redis]`: `url` (credential-bearing URLs are redacted in `Debug`/logs), `key_prefix` (default `"webhook_async"`), `stream_name` (default `"jobs"`), `consumer_group` (default `"workers"`). Note: `valkey` requires `[storage.valkey]`; `redis` requires `[storage.redis]`. (`stream_name` and `consumer_group` are accepted TOML schema fields retained for configuration stability while the active RESP adapter uses hash-tagged ZSETs + Lua scripts.)

### `[worker]`
- `concurrency` (default `4`, `>= 1`)
- `poll_interval_ms` (default `250`, `>= 10`)
- `lease_duration_seconds` (default `60`, strictly `> 2 * heartbeat_interval_seconds`)
- `heartbeat_interval_seconds` (default `15`, `>= 1`)
- `shutdown_grace_seconds` (default `30`, `>= 1`)

### `[queue]`
- `max_pending_jobs` (default `10000`, `>= 1`)
- `default_max_attempts` (default `5`, `>= 1`)
- `retry_base_delay_seconds` (default `5`, `>= 1`)
- `retry_max_delay_seconds` (default `300`, `>= retry_base_delay_seconds`)
- `default_timeout_seconds` (default `300`, `>= 1`)
- `max_output_bytes` (default `65536`, `>= 1024`)
- `idempotency_retention_seconds` (default `86400`, `>= 1`)

### `[ui]` and `[auth]` (Active in Phase 6)
- `[ui]`: `enabled` (default `false`), `bind` (default `"127.0.0.1"`, must be non-empty), `path_prefix` (default `"/ui"`, must start with `'/'`).
- `[auth]`: `admin_token` (direct literal token value in config), `admin_token_env` (env var holding bearer token, or file path), `admin_token_file` (file holding bearer token; specify at most one of the three). When `[ui].enabled = true`, auth configuration is mandatory.
