# Webhook-Async Deployment & Operations Guide

A comprehensive, production-grade guide to deploying, configuring, and operating `webhook-async` across all environments.

---

## Table of Contents

1. [Architecture & Overview](#1-architecture--overview)
2. [Quickstart (60 Seconds)](#2-quickstart-60-seconds)
3. [Configuration Reference](#3-configuration-reference)
   - [Application Configuration (`config.toml`)](#application-configuration-configtoml)
   - [Hook Definitions (`hooks.json` / `hooks.yaml`)](#hook-definitions-hooksjson--hooksyaml)
   - [Execution Mode Precedence (Sync vs Async)](#execution-mode-precedence-sync-vs-async)
4. [Deployment Environments](#4-deployment-environments)
   - [Environment 1: Linux Bare Metal / VM (systemd)](#environment-1-linux-bare-metal--vm-systemd)
   - [Environment 2: Docker & Docker Compose](#environment-2-docker--docker-compose)
   - [Environment 3: Kubernetes](#environment-3-kubernetes)
   - [Environment 4: macOS (Bare Metal & launchd)](#environment-4-macos-bare-metal--launchd)
   - [Environment 5: Windows Server (Service via NSSM)](#environment-5-windows-server-service-via-nssm)
   - [Environment 6: Distributed Scale-Out (Dedicated Workers)](#environment-6-distributed-scale-out-dedicated-workers)
5. [Reverse Proxy & Edge Ingress](#5-reverse-proxy--edge-ingress)
   - [Caddy (Automatic HTTPS)](#caddy-automatic-https)
   - [Nginx (High-Performance Ingress)](#nginx-high-performance-ingress)
   - [Direct TLS & Unix Domain Sockets](#direct-tls--unix-domain-sockets)
6. [Complete API Reference & Usage](#6-complete-api-reference--usage)
   - [Authentication & Rate Limiting](#authentication--rate-limiting)
   - [Webhook Receiver Endpoints](#webhook-receiver-endpoints)
   - [System Health & OpenAPI](#system-health--openapi)
   - [Administrative REST Endpoints](#administrative-rest-endpoints)
   - [Real-Time SSE Events Stream](#real-time-sse-events-stream)
   - [Complete Curl Recipe Book](#complete-curl-recipe-book)
7. [Web Management UI](#7-web-management-ui)
8. [CLI Tool Reference](#8-cli-tool-reference)
9. [Operational Runbook & Day-2 Operations](#9-operational-runbook--day-2-operations)
   - [Zero-Downtime Reloads & Signals](#zero-downtime-reloads--signals)
   - [Zero-Downtime SQLite Hot Backups](#zero-downtime-sqlite-hot-backups)
   - [Valkey / Redis Persistence & Backups](#valkey--redis-persistence--backups)
   - [Logging, Crash Handling & Metrics](#logging-crash-handling--metrics)
10. [Security Hardening Checklist](#10-security-hardening-checklist)
11. [Troubleshooting Matrix](#11-troubleshooting-matrix)

---

## 1. Architecture & Overview

`webhook-async` is a standalone, single-binary Rust webhook server and asynchronous job engine.

### Key Characteristics
- **Zero Runtime Dependencies**: No Node.js runtime, no Python, no mandatory Docker engine, and no frontend SPA framework.
- **Upstream Feature Parity**: Full backward compatibility with `adnanh/webhook` v2.8.3 hook definitions, trigger rules, HMAC signatures, IP whitelists, and template preprocessing.
- **Dual Execution Engine**:
  - **Synchronous Path**: Direct process execution returning stdout/stderr immediately; requires zero database or queue storage.
  - **Asynchronous Path**: Persistent, transactional queue committing an immutable execution snapshot before returning `HTTP 202 Accepted`.
- **Pluggable Storage Backends**:
  - **SQLite** (default): Embedded, WAL-mode, multi-threaded with busy timeouts.
  - **Valkey & Redis**: Distributed scale-out using atomic Lua scripts and clustered hash-tagged keys.
- **Integrated Web UI**: Lightweight Askama server-rendered templates, HTMX reactive components, and Server-Sent Events (SSE).

```
                      [ Incoming Webhook / Client ]
                                   │
                                   ▼
                       ┌────────────────────────┐
                       │   Axum HTTP Receiver   │
                       │   (Auth, Trigger Rules)│
                       └───────────┬────────────┘
                                   │
                 ┌─────────────────┴─────────────────┐
                 │ Mode Resolution                   │
                 ▼                                   ▼
        [ Synchronous Hook ]               [ Asynchronous Hook ]
                 │                                   │
                 ▼                                   ▼
       ┌──────────────────┐               ┌─────────────────────┐
       │ Direct Executor  │               │ Storage / Queue     │
       │ (Process Group)  │               │ (SQLite / Valkey)   │
       └─────────┬────────┘               └──────────┬──────────┘
                 │                                   │ (HTTP 202)
                 ▼                                   ▼
         Combined Output                    ┌─────────────────┐
          (HTTP 200/500)                    │ Worker Pool     │
                                            │ (Heartbeat, Ack)│
                                            └────────┬────────┘
                                                     ▼
                                            [ Direct Executor ]
```

---

## 2. Quickstart (60 Seconds)

### Step 1: Download or Build the Binary
Download the prebuilt binary from [GitHub Releases](https://github.com/sunilsankar/webhook-async/releases):
```bash
# Example for Linux x86_64
curl -L -o webhook-async https://github.com/sunilsankar/webhook-async/releases/download/v0.1.0-beta/webhook-async-linux-amd64
chmod +x webhook-async
sudo mv webhook-async /usr/local/bin/
```

Or build from source with stable Rust:
```bash
cargo build --release
sudo cp target/release/webhook-async /usr/local/bin/
```

Verify installation:
```bash
webhook-async --version
```

### Step 2: Create a Minimal Hook Definition (`hooks.json`)
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

### Step 3: Run the Server
```bash
webhook-async server --port 9000 --hooks hooks.json
```

### Step 4: Trigger the Webhook
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

## 3. Configuration Reference

`webhook-async` uses TOML for server configuration (`config.toml`) and JSON/YAML for hook definitions (`hooks.json` or `hooks.yaml`).

### Application Configuration (`config.toml`)

```toml
[server]
host = "0.0.0.0"
port = 9000
url_prefix = "hooks"
max_body_bytes = 10485760               # 10 MB maximum request size
max_multipart_mem_bytes = 1048576        # 1 MB multipart in-memory buffer
hook_files = ["/etc/webhook/hooks.json"]
hot_reload = true                        # Watch hook files for changes
default_http_methods = ["POST"]
response_headers = [
  { name = "X-Service", value = "webhook-async" },
  { name = "Access-Control-Allow-Origin", value = "*" }
]
use_x_request_id = true
x_request_id_limit = 64
trust_proxy_headers = true               # Trust X-Forwarded-For

[storage]
backend = "sqlite"                       # "sqlite" | "valkey" | "redis"

[storage.sqlite]
database_url = "sqlite:///var/lib/webhook-async/data.db?mode=rwc"
wal_mode = true
busy_timeout_ms = 5000

# Alternative distributed Valkey/Redis storage:
# [storage]
# backend = "valkey"
# [storage.valkey]
# url = "redis://10.0.1.50:6379/0"
# key_prefix = "webhook_async:"

[worker]
concurrency = 8                          # Bounded concurrent tasks
poll_interval_ms = 250                   # Queue poll interval
lease_duration_seconds = 60              # Heartbeat extension window
heartbeat_interval_seconds = 15          # Frequency of lease renewal
shutdown_grace_seconds = 30              # Drain window on SIGTERM

[queue]
max_pending_jobs = 10000                 # Backpressure limit (returns 429 when full)
default_max_attempts = 5                 # Maximum retries before dead-letter
retry_base_delay_seconds = 5             # Exponential backoff base
retry_max_delay_seconds = 300            # Exponential backoff ceiling
default_timeout_seconds = 300            # Task process execution timeout
max_output_bytes = 65536                 # 64 KB stdout/stderr capture bound
idempotency_retention_seconds = 86400    # 24h idempotency key TTL

[ui]
enabled = true
bind = "127.0.0.1:9001"                  # Isolated management UI bind
path_prefix = ""

[auth]
admin_token = "your-secure-admin-password"  # Direct literal password in TOML
# Or use an environment variable:
# admin_token_env = "WEBHOOK_ADMIN_TOKEN"
# Or use a secret file:
# admin_token_file = "/run/secrets/admin_token"
```

### Hook Definitions (`hooks.json` / `hooks.yaml`)

Hooks define the trigger rules, parameter extraction, and executable command.

```json
[
  {
    "id": "github-deploy",
    "execute-command": "/opt/scripts/deploy.sh",
    "command-working-directory": "/opt/app",
    "http-methods": ["POST"],
    "response-message": "Deployment queued.",
    "success-http-response-code": 200,
    "trigger-rule-mismatch-http-response-code": 400,
    "include-command-output-in-response": false,
    
    "trigger-rule": {
      "and": [
        {
          "match": {
            "type": "payload-hmac-sha256",
            "secret": "{{ getenv \"GITHUB_WEBHOOK_SECRET\" }}",
            "parameter": {
              "source": "header",
              "name": "X-Hub-Signature-256"
            }
          }
        },
        {
          "match": {
            "type": "value",
            "value": "refs/heads/main",
            "parameter": {
              "source": "payload",
              "name": "ref"
            }
          }
        }
      ]
    },

    "pass-environment-to-command": [
      {
        "source": "payload",
        "name": "head_commit.id",
        "envname": "DEPLOY_COMMIT"
      },
      {
        "source": "payload",
        "name": "pusher.name",
        "envname": "DEPLOY_AUTHOR"
      }
    ],

    "pass-arguments-to-command": [
      { "source": "payload", "name": "repository.name" },
      { "source": "request", "name": "remote-addr" }
    ],

    "pass-file-to-command": [
      {
        "source": "entire-payload",
        "envname": "PAYLOAD_FILE"
      }
    ]
  }
]
```

#### Supported Parameter Sources
- `header`: HTTP request header (canonical MIME case).
- `url` / `query`: URL query string parameters.
- `payload`: Parsed JSON, XML, or Form-urlencoded body. Supports nested dot-paths (e.g. `repository.owner.name`).
- `raw-request-body`: Unmodified UTF-8 request payload string.
- `request`: Request metadata (`remote-addr`, `method`).
- `string`: Hardcoded static string passed directly into arguments or environment.
- `entire-payload`: Serialized full payload written to a file (`pass-file-to-command`).
- `entire-headers`: JSON representation of all request headers.
- `entire-query`: JSON representation of all query parameters.

#### Supported Trigger Rule Match Types
- `value`: Constant-time string equality.
- `regex`: Regular expression matching.
- `payload-hmac-sha1`, `payload-hmac-sha256`, `payload-hmac-sha512`: Constant-time HMAC hex verification with secret.
- `ip-whitelist`: Whitespace-delimited IPv4/IPv6 addresses and CIDR subnets.
- `scalr-signature`: Scalr-compliant HMAC-SHA1 timestamp and signature verification.

#### Template Preprocessing
When started with `--template` (or `-template`), hook files support Go-template syntax:
- `{{ getenv "ENV_VAR" "default" }}`: Injects environment variables.
- `{{ cat "/etc/secrets/token" }}`: Injects file contents (trims trailing newline).
- `{{ credential "secret_name" }}`: Injects systemd credentials (`$CREDENTIALS_DIRECTORY`).
- `{{ ... | js }}`: Safely escapes content for valid JSON string representation.

---

### Execution Mode Precedence (Sync vs Async)

`webhook-async` determines execution mode on a per-request basis with zero ambiguity:

1. **Per-Hook Override**: If the hook definition specifies `"async": true` or `"async": false`, that value is final.
2. **Server CLI Flag**: If omitted on the hook, `--async` or `--sync` CLI flag governs.
3. **Default**: If neither is set, execution defaults to **synchronous** (`--sync`).

| Hook `async` | CLI Flag | Resulting Mode | Storage Required? | Response Code |
| :---: | :---: | :---: | :---: | :---: |
| `false` | `--async` | **Synchronous** | No | 200 (Output/Message) |
| `true` | `--sync` | **Asynchronous** | Yes | 202 (Job Enqueued) |
| *Omitted* | `--async` | **Asynchronous** | Yes | 202 (Job Enqueued) |
| *Omitted* | *None* | **Synchronous** | No | 200 (Output/Message) |

---

## 4. Deployment Environments

---

### Environment 1: Linux Bare Metal / VM (systemd)

Standard deployment for Debian/Ubuntu, RHEL/Rocky, or Alpine Linux.

#### 1. Setup User and Directories
```bash
sudo useradd -r -s /usr/sbin/nologin -d /var/lib/webhook-async webhook
sudo mkdir -p /etc/webhook-async /var/lib/webhook-async /var/log/webhook-async
sudo chown -R webhook:webhook /var/lib/webhook-async /var/log/webhook-async
```

#### 2. Create `/etc/webhook-async/config.toml`
```toml
[server]
host = "127.0.0.1"
port = 9000
hook_files = ["/etc/webhook-async/hooks.json"]
hot_reload = true

[storage]
backend = "sqlite"

[storage.sqlite]
database_url = "sqlite:///var/lib/webhook-async/data.db?mode=rwc"

[worker]
concurrency = 4

[ui]
enabled = true
bind = "127.0.0.1:9001"

[auth]
admin_token_env = "WEBHOOK_ADMIN_TOKEN"
```

#### 3. Create systemd Unit `/etc/systemd/system/webhook-async.service`
```ini
[Unit]
Description=webhook-async standalone receiver and job engine
After=network.target

[Service]
Type=simple
User=webhook
Group=webhook
Environment="WEBHOOK_ADMIN_TOKEN=generate-a-strong-secret-token"
ExecStart=/usr/local/bin/webhook-async server --config /etc/webhook-async/config.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5s
KillMode=mixed
TimeoutStopSec=35s
LimitNOFILE=65536

# Sandboxing
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/webhook-async /var/log/webhook-async
PrivateTmp=true
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
```

#### 4. Enable and Start
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now webhook-async
sudo systemctl status webhook-async
```

---

### Environment 2: Docker & Docker Compose

Deploying using a multi-stage, scratch or alpine container.

#### 1. `Dockerfile`
```dockerfile
# Build Stage
FROM rust:1.89-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY . .
RUN cargo build --release

# Runtime Stage
FROM alpine:3.20
RUN apk add --no-cache bash ca-certificates curl jq git
RUN addgroup -S webhook && adduser -S webhook -G webhook
WORKDIR /app
COPY --from=builder /build/target/release/webhook-async /usr/local/bin/webhook-async
RUN mkdir -p /data && chown -R webhook:webhook /data
USER webhook
EXPOSE 9000 9001
VOLUME ["/data", "/config"]
ENTRYPOINT ["/usr/local/bin/webhook-async"]
CMD ["server", "--config", "/config/config.toml"]
```

#### 2. `docker-compose.yml` (With Valkey Distributed Storage)
```yaml
version: "3.8"

services:
  valkey:
    image: valkey/valkey:8-alpine
    restart: unless-stopped
    command: ["valkey-server", "--appendonly", "yes"]
    volumes:
      - valkey-data:/data
    networks:
      - webhook-net

  webhook-api:
    build: .
    restart: unless-stopped
    ports:
      - "9000:9000"
      - "127.0.0.1:9001:9001"
    environment:
      - WEBHOOK_ADMIN_TOKEN=super-secure-admin-token
    volumes:
      - ./config:/config:ro
      - webhook-scripts:/scripts:ro
    depends_on:
      - valkey
    networks:
      - webhook-net

  webhook-worker:
    build: .
    restart: unless-stopped
    command: ["worker", "--config", "/config/config.toml"]
    environment:
      - WEBHOOK_ADMIN_TOKEN=super-secure-admin-token
    volumes:
      - ./config:/config:ro
      - webhook-scripts:/scripts
    depends_on:
      - valkey
    networks:
      - webhook-net

volumes:
  valkey-data:
  webhook-scripts:

networks:
  webhook-net:
    driver: bridge
```

---

### Environment 3: Kubernetes

Deploy `webhook-async` with decoupled receiver and background worker pods.

#### 1. ConfigMap & Secret (`k8s-config.yaml`)
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: webhook-secrets
type: Opaque
stringData:
  admin-token: "replace-with-secure-bearer-token"
  github-secret: "my-github-hmac-secret"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: webhook-config
data:
  config.toml: |
    [server]
    host = "0.0.0.0"
    port = 9000
    hook_files = ["/etc/webhook/hooks.json"]

    [storage]
    backend = "valkey"
    [storage.valkey]
    url = "redis://valkey-service:6379/0"
    key_prefix = "k8s_webhook:"

    [worker]
    concurrency = 8
    shutdown_grace_seconds = 30

    [auth]
    admin_token_env = "WEBHOOK_ADMIN_TOKEN"

  hooks.json: |
    [
      {
        "id": "k8s-sync",
        "execute-command": "/bin/echo",
        "pass-arguments-to-command": [
          { "source": "payload", "name": "service" }
        ],
        "async": true
      }
    ]
```

#### 2. Deployment & Service (`k8s-deployment.yaml`)
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: webhook-receiver
spec:
  replicas: 3
  selector:
    matchLabels:
      app: webhook-receiver
  template:
    metadata:
      labels:
        app: webhook-receiver
    spec:
      containers:
        - name: receiver
          image: ghcr.io/sunilsankar/webhook-async:v0.1.0-beta
          args: ["server", "--config", "/etc/webhook/config.toml", "--async"]
          ports:
            - containerPort: 9000
          env:
            - name: WEBHOOK_ADMIN_TOKEN
              valueFrom:
                secretKeyRef:
                  name: webhook-secrets
                  key: admin-token
          livenessProbe:
            httpGet:
              path: /api/v1/health
              port: 9000
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /api/v1/ready
              port: 9000
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - name: config-vol
              mountPath: /etc/webhook
      volumes:
        - name: config-vol
          configMap:
            name: webhook-config
---
apiVersion: v1
kind: Service
metadata:
  name: webhook-service
spec:
  type: ClusterIP
  ports:
    - port: 80
      targetPort: 9000
  selector:
    app: webhook-receiver
```

---

### Environment 4: macOS (Bare Metal & launchd)

Ideal for developer machines or dedicated Mac mini build agents.

#### 1. Install Binary
```bash
curl -L -o /usr/local/bin/webhook-async https://github.com/sunilsankar/webhook-async/releases/download/v0.1.0-beta/webhook-async-macos-arm64
chmod +x /usr/local/bin/webhook-async
```

#### 2. Create launchd Agent `~/Library/LaunchAgents/com.sunilsankar.webhook-async.plist`
```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.sunilsankar.webhook-async</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/webhook-async</string>
        <string>server</string>
        <string>--config</string>
        <string>/Users/youruser/.config/webhook-async/config.toml</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/youruser/.config/webhook-async/stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/youruser/.config/webhook-async/stderr.log</string>
</dict>
</plist>
```

#### 3. Load the Agent
```bash
launchctl load ~/Library/LaunchAgents/com.sunilsankar.webhook-async.plist
```

---

### Environment 5: Windows Server (Service via NSSM)

Deploying as a managed Windows Service using [NSSM](https://nssm.cc/).

1. Download `webhook-async-windows-amd64.exe` to `C:\webhook-async\webhook-async.exe`.
2. Create `C:\webhook-async\config.toml` and `C:\webhook-async\hooks.json`.
3. Open an Administrator PowerShell prompt and run:
```powershell
nssm install WebhookAsync "C:\webhook-async\webhook-async.exe" "server --config C:\webhook-async\config.toml"
nssm set WebhookAsync AppDirectory "C:\webhook-async"
nssm set WebhookAsync AppStdout "C:\webhook-async\logs\stdout.log"
nssm set WebhookAsync AppStderr "C:\webhook-async\logs\stderr.log"
nssm set WebhookAsync Start SERVICE_AUTO_START
nssm start WebhookAsync
```

---

### Environment 6: Distributed Scale-Out (Dedicated Workers)

For high-throughput environments processing thousands of hooks per minute:

```
                          [ Ingress Load Balancer ]
                                     │
                 ┌───────────────────┴───────────────────┐
                 ▼                                       ▼
       [ API Pod / Instance 1 ]                [ API Pod / Instance 2 ]
       (webhook-async server --async)          (webhook-async server --async)
                 │                                       │
                 └───────────────────┬───────────────────┘
                                     │ (Enqueues Execution Spec)
                                     ▼
                        [ Valkey / Redis Cluster ]
                                     │
                 ┌───────────────────┴───────────────────┐
                 │ (Claims via Atomic Lua Leases)        │
                 ▼                                       ▼
      [ Worker Instance 1 ]                   [ Worker Instance 2 ]
      (webhook-async worker)                  (webhook-async worker)
      - Concurrency: 32                       - Concurrency: 32
      - Direct Process Isolation              - Direct Process Isolation
```

- **API Nodes**: Run `webhook-async server --async --config /etc/config.toml` with `worker.concurrency = 0`.
- **Worker Nodes**: Run `webhook-async worker --config /etc/config.toml` with `worker.concurrency = 32`.

---

## 5. Reverse Proxy & Edge Ingress

Always terminate public web traffic through a production reverse proxy or ingress controller.

### Caddy (Automatic HTTPS)

Caddy automatically provisions Let's Encrypt TLS certificates:

```caddyfile
webhook.example.com {
    # Proxy hook triggers to webhook-async
    reverse_proxy 127.0.0.1:9000

    # Optional: Proxy web management UI with internal access only
    handle_path /admin/* {
        @internal remote_ip 10.0.0.0/8 192.168.0.0/16 127.0.0.1
        reverse_proxy @internal 127.0.0.1:9001
    }
}
```

### Nginx (High-Performance Ingress)

```nginx
upstream webhook_backend {
    server 127.0.0.1:9000;
    keepalive 32;
}

server {
    listen 443 ssl http2;
    server_name webhook.example.com;

    ssl_certificate /etc/letsencrypt/live/webhook.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/webhook.example.com/privkey.pem;

    client_max_body_size 10M;

    location / {
        proxy_pass http://webhook_backend;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Request-Id $request_id;
    }

    # Enable SSE streaming for live events
    location /api/v1/events {
        proxy_pass http://webhook_backend;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 24h;
    }
}
```

### Direct TLS & Unix Domain Sockets

`webhook-async` supports direct TLS and Unix domain sockets natively:

```bash
# Direct TLS listening on port 9443
webhook-async server \
  --port 9443 \
  --secure \
  --cert /etc/ssl/certs/webhook.crt \
  --key /etc/ssl/private/webhook.key \
  --hooks /etc/webhook/hooks.json

# Direct Unix domain socket (ideal for local Nginx proxy)
webhook-async server \
  --socket /run/webhook-async.sock \
  --hooks /etc/webhook/hooks.json
```

---

## 6. Complete API Reference & Usage

All administrative and operational endpoints are namespaced under `/api/v1`.

### Authentication & Rate Limiting

- **Header**: `Authorization: Bearer <token>`
- **Token Configuration**: Set via `[auth].admin_token_env` or `[auth].admin_token_file`.
- **Constant-Time Verification**: Uses `subtle::ConstantTimeEq` to prevent timing attacks.
- **Fail-Secure**: If unconfigured, authenticated endpoints return `503 Service Unavailable`.
- **Rate Limit**: Built-in 60 requests/minute per token. Returns `HTTP 429` with `Retry-After: 60`.

---

### Webhook Receiver Endpoints

#### Trigger a Hook
```http
POST /{url_prefix}/{hook_id}
```
Triggers the specified hook. Supports URL sub-paths if the hook ID contains slashes (e.g. `sendgrid/inbound`).

- **Synchronous Hook**: Returns `200 OK` (with stdout/stderr if `include-command-output-in-response: true`).
- **Asynchronous Hook**: Returns `202 Accepted` after committing to storage.
- **Trigger Rule Failure**: Returns `400 Bad Request` (or custom `trigger-rule-mismatch-http-response-code`).
- **Backpressure**: Returns `429 Too Many Requests` when `queue.max_pending_jobs` is reached.
- **Backend Outage**: Returns `503 Service Unavailable` if the queue database is down.

---

### System Health & OpenAPI

#### 1. Liveness Probe (Unauthenticated)
```http
GET /api/v1/health
```
Response `200 OK`:
```json
{
  "status": "ok",
  "version": "0.1.0"
}
```

#### 2. Readiness Probe (Unauthenticated)
```http
GET /api/v1/ready
```
Performs a live storage ping (`SELECT 1` or Redis `PING`).
Response `200 OK`:
```json
{
  "status": "ready",
  "storage": "connected"
}
```
If storage is unavailable, returns `503 Service Unavailable`.

#### 3. OpenAPI 3.1 Specification (Unauthenticated)
```http
GET /api/v1/openapi.json
```
Returns the full OpenAPI 3.1 JSON schema describing all endpoints.

---

### Administrative REST Endpoints

*(Requires `Authorization: Bearer <token>`)*

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/v1/stats` | Queue counts across all 8 states + total pending |
| `GET` | `/api/v1/jobs` | Paginated job list (`state`, `hook_id`, `limit`, `offset`) |
| `GET` | `/api/v1/jobs/{id}` | Detailed job record with attempt history & previews |
| `POST` | `/api/v1/jobs/{id}/retry` | Re-queue a failed or dead-letter job |
| `POST` | `/api/v1/jobs/{id}/cancel` | Cancel a queued or running job |
| `GET` | `/api/v1/hooks` | Redacted list of loaded hooks (secrets masked) |
| `GET` | `/api/v1/workers` | Active worker nodes, concurrency, and heartbeats |
| `GET` | `/api/v1/storage/status` | Storage backend kind, capabilities, and connectivity |
| `GET` | `/api/v1/events` | Real-time Server-Sent Events (SSE) stats stream |

---

### Complete Curl Recipe Book

#### 1. Check Health & Readiness
```bash
curl -s http://localhost:9000/api/v1/health
curl -s http://localhost:9000/api/v1/ready
```

#### 2. Trigger an Asynchronous Hook with Idempotency Key
```bash
curl -X POST http://localhost:9000/hooks/github-deploy \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: commit-sha-05dc3f3" \
  -d '{"ref": "refs/heads/main"}'
```
Response:
```json
{
  "job_id": "8a01cb6b-4e12-4f10-9c41-89e70197da96",
  "status": "queued",
  "message": "Deployment queued."
}
```

#### 3. Inspect Queue Statistics
```bash
curl -s http://localhost:9000/api/v1/stats \
  -H "Authorization: Bearer $WEBHOOK_ADMIN_TOKEN"
```
Response:
```json
{
  "counts_by_state": {
    "queued": 0,
    "running": 1,
    "succeeded": 1420,
    "failed": 2,
    "dead_letter": 1
  },
  "pending_count": 1
}
```

#### 4. Filter and Paginate Jobs
```bash
# Get the 10 most recent failed jobs
curl -s "http://localhost:9000/api/v1/jobs?state=failed&limit=10" \
  -H "Authorization: Bearer $WEBHOOK_ADMIN_TOKEN"
```

#### 5. Inspect Job Details and Output
```bash
curl -s http://localhost:9000/api/v1/jobs/8a01cb6b-4e12-4f10-9c41-89e70197da96 \
  -H "Authorization: Bearer $WEBHOOK_ADMIN_TOKEN"
```
Response:
```json
{
  "job": {
    "id": "8a01cb6b-4e12-4f10-9c41-89e70197da96",
    "hook_id": "github-deploy",
    "state": "succeeded",
    "attempt": 1,
    "max_attempts": 5,
    "created_at": "2026-11-13T10:15:30.123456789Z",
    "started_at": "2026-11-13T10:15:30.130000000Z",
    "completed_at": "2026-11-13T10:15:34.250000000Z",
    "stdout_preview": "Deploying commit 05dc3f3...\nDone.",
    "stderr_preview": ""
  },
  "attempts": [
    {
      "attempt": 1,
      "worker_id": "worker-pool-88021",
      "exit_code": 0,
      "stdout_preview": "Deploying commit 05dc3f3...\nDone."
    }
  ]
}
```

#### 6. Retry a Dead-Lettered Job
```bash
curl -X POST http://localhost:9000/api/v1/jobs/8a01cb6b-4e12-4f10-9c41-89e70197da96/retry \
  -H "Authorization: Bearer $WEBHOOK_ADMIN_TOKEN"
```

#### 7. Cancel an In-Flight Job
```bash
curl -X POST http://localhost:9000/api/v1/jobs/8a01cb6b-4e12-4f10-9c41-89e70197da96/cancel \
  -H "Authorization: Bearer $WEBHOOK_ADMIN_TOKEN"
```

---

## 7. Web Management UI

`webhook-async` embeds a zero-dependency web interface powered by Askama templates and HTMX.

### Features
- **Dashboard**: Live queue counts, active worker list, and recent executions.
- **Jobs Explorer**: Filter by status and hook ID, paginate history, and inspect stdout/stderr.
- **Job Controls**: One-click Retry and Cancel with CSRF validation.
- **Live Stream**: Reactive SSE channel updating counters automatically.
- **Hooks & Storage**: Inspect active hooks and storage connection status.

### Security Controls
- **Isolated Binding**: Can be bound to a separate IP/port (e.g. `127.0.0.1:9001`) away from public webhooks.
- **Session Authentication**: Token login creates an encrypted, `HttpOnly`, `SameSite=Lax` session cookie.
- **CSRF Protection**: All mutating requests require a valid CSRF token.
- **XSS Prevention**: Askama strictly auto-escapes all HTML variables.

---

## 8. CLI Tool Reference

The `webhook-async` binary provides direct management tools for automation and operators.

### Validate Configuration
```bash
# Validate config.toml and hooks
webhook-async config validate --config config.toml --hooks hooks.json

# Validate templated hooks
webhook-async config validate --template --hooks hooks.json
```

### Inspect Storage Status
```bash
webhook-async storage status --config config.toml
webhook-async storage status --config config.toml --json
```

### Manage Jobs from Terminal
```bash
# List jobs
webhook-async jobs list --config config.toml --limit 20
webhook-async jobs list --config config.toml --state dead_letter

# Get job details
webhook-async jobs get <job-id> --config config.toml

# Retry a failed job
webhook-async jobs retry <job-id> --config config.toml

# Cancel a running job
webhook-async jobs cancel <job-id> --config config.toml
```

---

## 9. Operational Runbook & Day-2 Operations

### Zero-Downtime Reloads & Signals

`webhook-async` responds to standard POSIX signals:

| Signal | Action | Behavior |
| :--- | :--- | :--- |
| `SIGHUP` | Reload Hooks | Re-reads and re-validates hook files. Active hooks remain running if parsing fails. |
| `SIGUSR1` | Reload Hooks | Upstream-compatible reload signal. |
| `SIGTERM` | Graceful Shutdown | Stops accepting new HTTP requests, drains workers for `shutdown_grace_seconds`, and cleans up PID/socket files. |
| `SIGINT` | Graceful Shutdown | Intercepts `Ctrl+C` for clean termination. |

#### Triggering a Live Reload
```bash
kill -HUP $(cat /var/run/webhook-async.pid)
# Or with systemd:
sudo systemctl reload webhook-async
```

---

### Zero-Downtime SQLite Hot Backups

Because SQLite operates in `WAL` mode with `busy_timeout`, backups can be taken while the server is accepting requests without table locks.

#### Run a Hot Backup
```bash
sqlite3 /var/lib/webhook-async/data.db "VACUUM INTO '/var/backups/webhook-async-$(date +%F-%H%M).db'"
```

#### Automated Nightly Backup Cron
Create `/etc/cron.daily/webhook-async-backup`:
```bash
#!/bin/bash
BACKUP_DIR="/var/backups/webhook-async"
mkdir -p "$BACKUP_DIR"
sqlite3 /var/lib/webhook-async/data.db "VACUUM INTO '$BACKUP_DIR/backup-$(date +%Y%m%d).db'"
find "$BACKUP_DIR" -type f -name "*.db" -mtime +14 -delete
```

---

### Valkey / Redis Persistence & Backups

For Redis and Valkey deployments:
1. Ensure `appendonly yes` is enabled in `valkey.conf` / `redis.conf`.
2. Schedule regular `BGSAVE` triggers to generate `dump.rdb` snapshots.
3. For point-in-time recovery, back up both the AOF file and RDB snapshot.

---

### Logging, Crash Handling & Metrics

- **File Logging**: Use `--logfile /var/log/webhook-async.log` to write structured operational logs to disk.
- **Log Levels**: Use `--verbose` for detailed request logging or `--debug` for full diagnostic traces.
- **Crash Immunity (`-nopanic`)**: Pass `--nopanic` in production to prevent unexpected panics in trigger evaluation from crashing the daemon.

---

## 10. Security Hardening Checklist

- [ ] **Run Unprivileged**: Never run `webhook-async` as `root`. Run as dedicated `webhook` user.
- [ ] **Direct Process Execution**: `webhook-async` executes commands directly via `execve` (arguments array). Never use shell wrapper scripts that interpolate unvalidated request strings into bash strings (`sh -c "$VAR"`).
- [ ] **Admin Authentication**: Configure `WEBHOOK_ADMIN_TOKEN` with a strong random secret (32+ characters).
- [ ] **Bind UI Safely**: Keep `[ui].bind = "127.0.0.1:9001"` behind a firewall or reverse proxy with authentication.
- [ ] **HMAC Signatures**: Always use `payload-hmac-sha256` rules for public webhooks (GitHub, GitLab, Stripe).
- [ ] **Enforce Timeouts**: Set `queue.default_timeout_seconds` (e.g. 300s) to kill stuck worker tasks automatically.
- [ ] **Process Group Sandboxing**: Timeouts and cancellations issue `SIGKILL` to the process group (`-pgid`), preventing orphaned subprocesses.

---

## 11. Troubleshooting Matrix

| Issue | Root Cause | Solution |
| :--- | :--- | :--- |
| **`400 Hook rules were not satisfied.`** | Trigger rule mismatch (e.g. invalid HMAC secret, missing header, wrong branch). | Run server with `--verbose` to inspect trigger evaluation details. Verify HMAC secret matches provider. |
| **`405 Method Not Allowed`** | Request sent with method not in `http-methods`. | Update hook's `http-methods` array or send with `POST`. |
| **`413 Payload Too Large`** | Request body exceeds `server.max_body_bytes`. | Increase `max_body_bytes` in `config.toml`. |
| **`429 Queue capacity full`** | In-flight jobs exceed `queue.max_pending_jobs`. | Increase worker concurrency, add more worker nodes, or raise `max_pending_jobs`. |
| **`503 Service Unavailable` on Webhooks** | Async hook received but storage backend is unreachable. | Check SQLite filesystem permissions or verify Valkey/Redis network connectivity. |
| **`503 Admin token not configured`** | Accessing `/api/v1/*` without configuring `admin_token_env`. | Set `WEBHOOK_ADMIN_TOKEN` in the environment or set `admin_token_file`. |
| **`Command not found` on Worker** | Executable is not in system `$PATH` or relative path invalid. | Use absolute path for `execute-command` (e.g. `/usr/local/bin/deploy.sh`). |
| **Job immediately `dead_letter`** | Executable or working directory does not exist. | `webhook-async` classifies missing binaries as Configuration errors and dead-letters immediately to prevent infinite retry loops. |
| **`database is locked` on SQLite** | Too many concurrent write attempts without timeout. | Ensure `wal_mode = true` and `busy_timeout_ms = 5000` in `[storage.sqlite]`. |
