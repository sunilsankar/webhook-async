# webhook-async

[![Build & Release](https://github.com/sunilsankar/webhook-async/actions/workflows/build.yml/badge.svg)](https://github.com/sunilsankar/webhook-async/actions/workflows/build.yml)
[![Latest Release](https://img.shields.io/github/v/release/sunilsankar/webhook-async?include_prereleases)](https://github.com/sunilsankar/webhook-async/releases)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

A standalone Rust webhook receiver and asynchronous job engine.

- **Zero Runtime Dependencies**: No Node.js runtime, no Python, no frontend build tools, and no mandatory Docker daemon.
- **Upstream Feature Parity**: Full backward compatibility with `adnanh/webhook` v2.8.3 hook definitions, trigger rules, HMAC signatures, IP whitelists, and template preprocessing.
- **Dual Execution Engine**:
  - **Synchronous**: Direct process execution returning command stdout/stderr immediately; requires zero database or queue storage.
  - **Asynchronous**: Durable queue committing an immutable execution snapshot before returning HTTP `202 Accepted`.
- **Pluggable Storage Backends**:
  - **SQLite** (default): Embedded, WAL-mode, multi-threaded with busy timeouts.
  - **Valkey & Redis**: Distributed scale-out using atomic Lua scripts and clustered hash-tagged keys.
- **Integrated Web UI**: Lightweight Askama server-rendered templates, HTMX reactive components, and Server-Sent Events (SSE).
- **Process Sandboxing**: Commands run with an explicit argument vector (no shell string interpolation), process group isolation (`-pgid`), execution timeouts, and automatic temporary file cleanup.
- **Dynamic Hot Reloading**: Update hook definitions on the fly via `SIGHUP`, `SIGUSR1`, or `--hotreload` polling with zero downtime.

---

## Documentation

Full documentation is available in [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) and on the [Documentation Site](https://sunilsankar.github.io/webhook-async-docs/):

- [**Deployment Guide**](docs/DEPLOYMENT.md): Production setup for Linux (systemd), Docker Compose, Kubernetes, macOS (launchd), Windows Server (NSSM), and Reverse Proxies (Caddy/Nginx).
- [**Configuration Reference**](docs/CONFIGURATION.md): Complete schema for `config.toml`, hook definitions, trigger rules, and template preprocessing.
- [**API Reference**](docs/API.md): Receiver endpoints, health checks, administrative REST API (`/api/v1/*`), and SSE real-time stream.
- [**Operations & Backups**](docs/OPERATIONS.md): Signal handling, zero-downtime hot reload, online SQLite hot backups (`VACUUM INTO`), logging, and disaster recovery.
- [**Security Hardening**](docs/SECURITY.md): Threat model, sandboxing, timing-attack prevention, and secret management.
- [**Storage Architecture**](docs/STORAGE.md): Conformance suite and storage mechanics for SQLite, Valkey, and Redis.
- [**Compatibility Baseline**](docs/COMPATIBILITY.md): Feature inventory and verification evidence against `adnanh/webhook` v2.8.3.

---

## Quickstart

### 1. Download Release
Download the precompiled standalone binary for your architecture from [GitHub Releases](https://github.com/sunilsankar/webhook-async/releases):

```bash
# Example for Linux x86_64
curl -L -o webhook-async https://github.com/sunilsankar/webhook-async/releases/download/v1.2.0-beta/webhook-async-linux-amd64
chmod +x webhook-async
sudo mv webhook-async /usr/local/bin/
```

Or build from source:
```bash
cargo build --release
sudo cp target/release/webhook-async /usr/local/bin/
```

### 2. Create a Hook Definition (`hooks.json`)
```json
[
  {
    "id": "deploy-app",
    "execute-command": "/bin/echo",
    "pass-arguments-to-command": [
      { "source": "payload", "name": "ref" },
      { "source": "payload", "name": "commit" }
    ],
    "response-message": "Deployment initiated.",
    "include-command-output-in-response": true
  }
]
```

### 3. Run the Server
```bash
webhook-async server --port 9000 --hooks hooks.json
```

### 4. Trigger the Webhook
```bash
curl -X POST http://localhost:9000/hooks/deploy-app \
  -H "Content-Type: application/json" \
  -d '{"ref": "refs/heads/main", "commit": "05dc3f3"}'
```
Response:
```text
refs/heads/main 05dc3f3
```

---

## License

Licensed under either of [Apache License, Version 2.0](LICENSE) or [MIT License](LICENSE) at your option.
