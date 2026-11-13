# Deployment Guide for `webhook-async`

`webhook-async` is a standalone Rust binary that executes webhook triggers synchronously or dispatches durable asynchronous jobs via SQLite or Valkey/Redis. It requires no Node.js runtime, no frontend build tools, and no mandatory Docker engine.

---

## 1. Installation & Binary Distribution

### Standalone Executable
Build or download the standalone executable:
```bash
cargo build --release
cp target/release/webhook-async /usr/local/bin/webhook-async
chmod +x /usr/local/bin/webhook-async
```

Verify the installation:
```bash
webhook-async --version
```

---

## 2. Systemd Service Deployment

Create `/etc/webhook-async/config.toml` (and `/etc/webhook-async/hooks.json`). Then create the systemd unit `/etc/systemd/system/webhook-async.service`:

```ini
[Unit]
Description=webhook-async job receiver and execution engine
After=network.target

[Service]
Type=simple
User=webhook
Group=webhook
ExecStart=/usr/local/bin/webhook-async server --config /etc/webhook-async/config.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5s
KillMode=mixed
TimeoutStopSec=35s
LimitNOFILE=65536

# Security hardening
ProtectSystem=full
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

Enable and start the service:
```bash
systemctl daemon-reload
systemctl enable --now webhook-async
```

---

## 3. Dedicated Worker Deployment

In distributed deployments with Valkey or Redis, workers can run on dedicated compute instances separate from the HTTP receiver:

```bash
webhook-async worker --config /etc/webhook-async/worker-config.toml
```

Example worker configuration:
```toml
[storage]
backend = "valkey"

[storage.valkey]
url = "redis://10.0.1.50:6379"
key_prefix = "webhook_async:"

[worker]
concurrency = 16
poll_interval_ms = 200
lease_duration_seconds = 60
heartbeat_interval_seconds = 15
shutdown_grace_seconds = 30
```

---

## 4. TLS & Unix Domain Socket Deployment

### TLS HTTPS Serving
Enable TLS using custom certificate and private key files:
```bash
webhook-async server \
  --port 9443 \
  --secure \
  --cert /etc/ssl/certs/webhook.crt \
  --key /etc/ssl/private/webhook.key \
  --hooks /etc/webhook/hooks.json
```
If `--secure` is passed without `--cert` and `--key`, `webhook-async` generates a temporary self-signed certificate for local development.

### Unix Domain Socket
Bind directly to a Unix domain socket for local inter-process communication:
```bash
webhook-async server \
  --socket /run/webhook-async/webhook.sock \
  --hooks /etc/webhook/hooks.json
```
The socket file is automatically unlinked on startup (if stale) and cleaned up on shutdown.

### Privilege Dropping
When binding privileged ports as root, drop privileges before serving:
```bash
webhook-async server \
  --port 443 \
  --setuid 1000 \
  --setgid 1000 \
  --hooks /etc/webhook/hooks.json
```
*Note: `--setuid` and `--setgid` must be supplied together and cannot be combined with `--socket`.*

---

## 5. Reverse Proxy Configuration

### Caddy
```caddy
webhook.example.com {
    reverse_proxy 127.0.0.1:9000 {
        header_up X-Forwarded-For {remote_host}
        header_up X-Forwarded-Proto {scheme}
    }
}
```

### Nginx
```nginx
server {
    listen 443 ssl http2;
    server_name webhook.example.com;

    ssl_certificate /etc/letsencrypt/live/webhook.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/webhook.example.com/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:9000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # SSE streaming support for live UI updates
        proxy_set_header Connection '';
        proxy_http_version 1.1;
        chunked_transfer_encoding off;
        proxy_buffering off;
        proxy_cache off;
    }
}
```

---

## 6. Sizing & Resource Planning

- **Memory**: Base server process consumes ~15–30 MB RSS. Plan memory for child processes executed by your webhook commands.
- **SQLite Disk**: Ensure SQLite databases reside on fast local storage (NVMe/SSD). Do not place SQLite WAL files on NFS or network shares.
- **File Descriptors**: Recommended `LimitNOFILE=65536` under systemd.
- **Worker Concurrency**: Set `[worker].concurrency` based on CPU cores and external subprocess I/O characteristics.
