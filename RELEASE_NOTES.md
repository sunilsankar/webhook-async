# webhook-async v1.2.0-beta Release Notes

`webhook-async` is a standalone Rust webhook receiver and asynchronous job engine compatible with `adnanh/webhook` v2.8.3, built with Tokio, Axum, Askama, HTMX, and Server-Sent Events.

---

## 🌟 What's New in v1.2.0-beta

### 1. 🐧 Static Linux Musl Binaries (Zero GLIBC Dependency)
- Primary Linux releases for both **ARM64** (`aarch64`) and **x86_64** (`amd64`) are now compiled statically with **musl libc**.
- **No more `version 'GLIBC_2.38' not found` errors.** Runs out-of-the-box on Debian 10/11/12, Ubuntu 18.04/20.04/22.04/24.04, Alpine Linux, CentOS/RHEL, and Raspberry Pi OS.
- Companion GNU builds are also provided (`webhook-async-linux-arm64-gnu`, `webhook-async-linux-amd64-gnu`).

### 2. 🔐 Simplified Authentication (`[auth]`)
- **Direct Token Configuration**: Configure `admin_token` directly in `config.toml` without needing environment variables:
  ```toml
  [auth]
  admin_token = "password@123"
  ```
- **File Path Fallback**: `admin_token_env` now checks if the value is an existing file path before checking environment variable names.
- **Protected File Support**: `admin_token_file = "/etc/webhook/secret.key"`.
- **Secure by Default**: Public webhook endpoints remain operational, while management UI and `/api/v1/*` admin endpoints return HTTP 503 if unconfigured.

### 3. ⚡ Discrete Redis & Valkey Storage Configuration
- Configure Redis or Valkey via individual fields instead of a connection string:
  ```toml
  [storage]
  backend = "valkey" # or "redis"

  [storage.valkey]
  host = "127.0.0.1"
  port = 6379
  database = 0
  username = "default"
  password = "your-password"
  tls = false
  key_prefix = "webhook_async"
  ```
- Supports single `url` connection string for backwards compatibility.
- Automatic RFC 3986 percent-encoding for usernames and passwords with special characters.
- Strict credential redaction in `Debug` logging and error messages.

### 4. 🖥️ Web Management UI Improvements & Footer
- **Fixed Dashboard 404**: Resolved 404 routing error when navigating back to the Dashboard from the Jobs view when configured with a `path_prefix` (e.g. `/ui` and `/ui/`).
- **Footer**: Added a clean, responsive UI footer displaying version (`v1.2.0-beta`), author (`Sunil Sankar`), and GitHub repository link.
- **Real-Time Live Updates**: Dashboard and Job details stream state changes via Server-Sent Events (SSE).

### 5. 📖 Comprehensive Documentation
- Live documentation site: **[https://sunilsankar.github.io/webhook-async-docs/](https://sunilsankar.github.io/webhook-async-docs/)**
- Includes a 10-step step-by-step `hooks.json` tutorial, request execution pipeline breakdown, API reference, and deployment recipes for systemd, Docker, Kubernetes, and reverse proxies.

---

## 📦 Binary Downloads

| Platform | Architecture | Binary Name | Linking |
| :--- | :--- | :--- | :--- |
| **Linux** | ARM64 (`aarch64`) | [`webhook-async-linux-arm64`](https://github.com/sunilsankar/webhook-async/releases/download/v1.2.0-beta/webhook-async-linux-arm64) | **Static (musl)** |
| **Linux** | ARM64 (`aarch64`) | [`webhook-async-linux-arm64-gnu`](https://github.com/sunilsankar/webhook-async/releases/download/v1.2.0-beta/webhook-async-linux-arm64-gnu) | Dynamic (gnu) |
| **Linux** | x86_64 (`amd64`) | [`webhook-async-linux-amd64`](https://github.com/sunilsankar/webhook-async/releases/download/v1.2.0-beta/webhook-async-linux-amd64) | **Static (musl)** |
| **Linux** | x86_64 (`amd64`) | [`webhook-async-linux-amd64-gnu`](https://github.com/sunilsankar/webhook-async/releases/download/v1.2.0-beta/webhook-async-linux-amd64-gnu) | Dynamic (gnu) |
| **macOS** | Apple Silicon (`arm64`) | [`webhook-async-macos-arm64`](https://github.com/sunilsankar/webhook-async/releases/download/v1.2.0-beta/webhook-async-macos-arm64) | Native |
| **macOS** | Intel (`x86_64`) | [`webhook-async-macos-amd64`](https://github.com/sunilsankar/webhook-async/releases/download/v1.2.0-beta/webhook-async-macos-amd64) | Native |
| **Windows** | x86_64 (`amd64`) | [`webhook-async-windows-amd64.exe`](https://github.com/sunilsankar/webhook-async/releases/download/v1.2.0-beta/webhook-async-windows-amd64.exe) | Native |

---

## 🚀 Quick Install (Linux ARM64 / amd64)

```bash
# For ARM64 (e.g. Raspberry Pi 4/5, AWS Graviton, Apple Silicon Linux VM)
curl -sSL -H "Cache-Control: no-cache" -o /usr/local/bin/webhook-async \
  https://github.com/sunilsankar/webhook-async/releases/download/v1.2.0-beta/webhook-async-linux-arm64

# For x86_64
curl -sSL -H "Cache-Control: no-cache" -o /usr/local/bin/webhook-async \
  https://github.com/sunilsankar/webhook-async/releases/download/v1.2.0-beta/webhook-async-linux-amd64

chmod +x /usr/local/bin/webhook-async
webhook-async --version
```
