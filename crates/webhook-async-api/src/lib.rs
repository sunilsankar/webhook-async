//! Axum HTTP webhook receiver (Phase 2 synchronous baseline).
//!
//! Management REST endpoints, OpenAPI, and durable async acceptance are wired in
//! Phases 5-6 while keeping synchronous hook behavior free of queue/storage dependencies.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{ConnectInfo, Path, Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use chrono::Utc;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use uuid::Uuid;
use webhook_async_config::{ResolvedHook, RuntimeConfig};
use webhook_async_core::ResponseHeader;
use webhook_async_hook_engine::{
    RequestContext, TriggerOutcome, eval_trigger, prepare_execution_spec,
};
use webhook_async_queue::{EnqueueMetadata, QueueCoordinator, QueueError};

pub mod admin;
pub mod tls;

const DEFAULT_MAX_BACKGROUND_SYNC_TASKS: usize = 64;

/// Errors raised when initializing or serving the HTTP API layer.
#[derive(Debug, Error)]
pub enum ApiError {
    #[error(
        "async hook execution is requested, but durable queue storage is not wired yet (Phases 3-5); start with --sync or remove async overrides"
    )]
    AsyncNotSupportedYet,

    #[error("async execution requested but no queue coordinator was configured")]
    AsyncQueueRequired,

    #[error("invalid listener configuration: {0}")]
    InvalidConfiguration(String),

    #[error("failed to drop privileges: {0}")]
    PrivilegeDrop(String),

    #[error("TLS configuration error: {0}")]
    Tls(String),

    #[error("unsupported platform feature: {0}")]
    UnsupportedPlatform(String),

    #[error("invalid server bind address '{addr}': {source}")]
    InvalidBindAddress {
        addr: String,
        #[source]
        source: std::io::Error,
    },

    #[error("HTTP server failed: {0}")]
    Serve(#[from] std::io::Error),
}

/// Shared state for the HTTP webhook receiver.
#[derive(Clone)]
pub struct ApiState {
    pub(crate) runtime: Arc<RuntimeConfig>,
    pub(crate) hooks: Arc<std::sync::RwLock<HashMap<String, ResolvedHook>>>,
    pub(crate) background_permits: Arc<Semaphore>,
    pub(crate) max_background_permits: u32,
    pub(crate) coordinator: Option<QueueCoordinator>,
    pub(crate) admin_token: Option<String>,
    pub(crate) rate_limiter: admin::AdminRateLimiter,
    pub(crate) worker_registry: Option<webhook_async_worker::WorkerRegistry>,
}

impl ApiState {
    /// Builds [`ApiState`] for synchronous webhook serving.
    ///
    /// Rejects any runtime configuration where global or per-hook execution mode resolves
    /// to `Async` if no queue coordinator is provided.
    pub fn new(runtime: RuntimeConfig) -> Result<Self, ApiError> {
        Self::with_queue(runtime, None)
    }

    /// Builds [`ApiState`] with optional queue coordinator.
    pub fn with_queue(
        runtime: RuntimeConfig,
        coordinator: Option<QueueCoordinator>,
    ) -> Result<Self, ApiError> {
        Self::with_queue_and_workers(runtime, coordinator, None)
    }

    /// Builds [`ApiState`] with optional queue coordinator and worker registry.
    pub fn with_queue_and_workers(
        runtime: RuntimeConfig,
        coordinator: Option<QueueCoordinator>,
        worker_registry: Option<webhook_async_worker::WorkerRegistry>,
    ) -> Result<Self, ApiError> {
        if runtime.requires_queue_backend() && coordinator.is_none() {
            return Err(ApiError::AsyncNotSupportedYet);
        }

        let admin_token = runtime
            .app
            .auth
            .resolve_admin_token()
            .map_err(ApiError::Serve)?;

        let hooks_map: HashMap<String, ResolvedHook> = runtime
            .hooks
            .iter()
            .cloned()
            .map(|h| (h.definition.id.clone(), h))
            .collect();

        // Bound in-flight background sync executions (`include-command-output-in-response: false`)
        // to avoid unbounded task spawning under request bursts.
        let max_permits = runtime
            .app
            .worker
            .concurrency
            .max(DEFAULT_MAX_BACKGROUND_SYNC_TASKS as u32);

        Ok(Self {
            runtime: Arc::new(runtime),
            hooks: Arc::new(std::sync::RwLock::new(hooks_map)),
            background_permits: Arc::new(Semaphore::new(max_permits as usize)),
            max_background_permits: max_permits,
            coordinator,
            admin_token,
            rate_limiter: admin::AdminRateLimiter::default(),
            worker_registry,
        })
    }

    /// Returns a reference to the runtime configuration.
    #[must_use]
    pub fn runtime(&self) -> &RuntimeConfig {
        &self.runtime
    }

    /// Returns the resolved admin token, if configured.
    #[must_use]
    pub fn admin_token(&self) -> Option<&str> {
        self.admin_token.as_deref()
    }

    /// Returns a reference to the admin rate limiter.
    #[must_use]
    pub fn rate_limiter(&self) -> &admin::AdminRateLimiter {
        &self.rate_limiter
    }

    /// Returns a reference to the worker registry, if configured.
    #[must_use]
    pub fn worker_registry(&self) -> Option<&webhook_async_worker::WorkerRegistry> {
        self.worker_registry.as_ref()
    }

    /// Returns a reference to the optional queue coordinator.
    #[must_use]
    pub fn coordinator(&self) -> Option<&QueueCoordinator> {
        self.coordinator.as_ref()
    }

    /// Returns the resolved hook by ID, if registered.
    #[must_use]
    pub fn get_hook(&self, id: &str) -> Option<ResolvedHook> {
        self.hooks
            .read()
            .expect("lock not poisoned")
            .get(id)
            .cloned()
    }

    /// Returns a list of all currently registered hooks.
    #[must_use]
    pub fn list_hooks(&self) -> Vec<ResolvedHook> {
        self.hooks
            .read()
            .expect("lock not poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Atomically replaces the registered hooks table during configuration reload.
    ///
    /// In-flight executions and queued jobs retain their immutable snapshots.
    pub fn reload_hooks(&self, new_hooks: Vec<ResolvedHook>) {
        let mut map = self.hooks.write().expect("lock not poisoned");
        *map = new_hooks
            .into_iter()
            .map(|h| (h.definition.id.clone(), h))
            .collect();
    }

    /// Returns a clone of the internal hooks handle for shared state across subsystems.
    #[must_use]
    pub fn hooks_handle(&self) -> Arc<std::sync::RwLock<HashMap<String, ResolvedHook>>> {
        self.hooks.clone()
    }

    /// Waits for all in-flight background sync executions to complete.
    pub async fn drain_background_tasks(&self) {
        let _ = self
            .background_permits
            .acquire_many(self.max_background_permits)
            .await;
    }
}

/// Constructs the Axum [`Router`] matching upstream `adnanh/webhook` routes
/// and management REST routes under `/api/v1`.
pub fn build_router(state: ApiState) -> Router {
    let prefix = state.runtime.app.server.url_prefix.trim_matches('/');
    let hook_route = if prefix.is_empty() {
        "/{*hook_id}".to_string()
    } else {
        format!("/{prefix}/{{*hook_id}}")
    };

    Router::new()
        .nest("/api/v1", admin::routes())
        .route("/", any(handle_root))
        .route(&hook_route, any(handle_hook))
        .with_state(state)
}

/// Binds TCP listener from `runtime.app.server` and serves requests until `shutdown` resolves,
/// then drains in-flight background hook executions.
pub async fn serve_with_shutdown<F>(runtime: RuntimeConfig, shutdown: F) -> Result<(), ApiError>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_with_queue_and_shutdown(runtime, None, shutdown).await
}

/// Binds TCP listener from `runtime.app.server` with an optional queue coordinator,
/// serving requests until `shutdown` resolves, then drains in-flight background hook executions.
pub async fn serve_with_queue_and_shutdown<F>(
    runtime: RuntimeConfig,
    coordinator: Option<QueueCoordinator>,
    shutdown: F,
) -> Result<(), ApiError>
where
    F: Future<Output = ()> + Send + 'static,
{
    serve_with_queue_and_workers_and_shutdown(runtime, coordinator, None, shutdown).await
}

/// Configuration for the server network listener.
#[derive(Debug, Clone, Default)]
pub struct ListenerConfig {
    pub unix_socket: Option<PathBuf>,
    pub secure: bool,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub setuid: Option<u32>,
    pub setgid: Option<u32>,
}

#[cfg(unix)]
fn drop_privileges(setuid: Option<u32>, setgid: Option<u32>) -> Result<(), ApiError> {
    if let (Some(gid), Some(uid)) = (setgid, setuid) {
        // SAFETY: Invoked on startup immediately after binding listening sockets.
        unsafe {
            if libc::setgid(gid as libc::gid_t) != 0 {
                return Err(ApiError::PrivilegeDrop(format!(
                    "failed to setgid to {gid}: {}",
                    std::io::Error::last_os_error()
                )));
            }
            if libc::setuid(uid as libc::uid_t) != 0 {
                return Err(ApiError::PrivilegeDrop(format!(
                    "failed to setuid to {uid}: {}",
                    std::io::Error::last_os_error()
                )));
            }
        }
        info!(uid, gid, "dropped process privileges");
    }
    Ok(())
}

/// Binds network listener according to `listener_cfg` and serves requests until `shutdown` resolves,
/// then drains in-flight background hook executions.
pub async fn serve_full<F>(
    state: ApiState,
    listener_cfg: ListenerConfig,
    shutdown: F,
) -> Result<(), ApiError>
where
    F: Future<Output = ()> + Send + 'static,
{
    if listener_cfg.unix_socket.is_some()
        && (listener_cfg.setuid.is_some() || listener_cfg.setgid.is_some())
    {
        return Err(ApiError::InvalidConfiguration(
            "-setuid and -setgid cannot be used with -socket".to_string(),
        ));
    }
    if listener_cfg.setuid.is_some() ^ listener_cfg.setgid.is_some() {
        return Err(ApiError::InvalidConfiguration(
            "-setuid and -setgid must be specified together".to_string(),
        ));
    }

    let tls_acceptor = if listener_cfg.secure {
        Some(
            tls::create_tls_acceptor(
                listener_cfg.cert_file.as_deref(),
                listener_cfg.key_file.as_deref(),
            )
            .map_err(|e| ApiError::Tls(e.to_string()))?,
        )
    } else {
        None
    };

    let drain_state = state.clone();
    let app = build_router(state);

    if let Some(socket_path) = listener_cfg.unix_socket {
        #[cfg(unix)]
        {
            if socket_path.exists() {
                let _ = std::fs::remove_file(&socket_path);
            }
            let listener = tokio::net::UnixListener::bind(&socket_path).map_err(ApiError::Serve)?;
            info!(socket = %socket_path.display(), "webhook-async listening on unix domain socket");

            let make_svc = app.into_make_service();
            let result = if let Some(acceptor) = tls_acceptor {
                let tls_listener = tls::TlsListener::new(listener, acceptor);
                axum::serve(tls_listener, make_svc)
                    .with_graceful_shutdown(shutdown)
                    .await
            } else {
                axum::serve(listener, make_svc)
                    .with_graceful_shutdown(shutdown)
                    .await
            };

            let _ = std::fs::remove_file(&socket_path);
            drain_state.drain_background_tasks().await;
            result.map_err(ApiError::Serve)
        }
        #[cfg(not(unix))]
        {
            let _ = socket_path;
            let _ = drain_state;
            let _ = app;
            Err(ApiError::UnsupportedPlatform(
                "-socket is only supported on Unix".to_string(),
            ))
        }
    } else {
        let host = drain_state.runtime.app.server.host.trim();
        let addr = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]:{}", drain_state.runtime.app.server.port)
        } else {
            format!("{host}:{}", drain_state.runtime.app.server.port)
        };

        let listener =
            TcpListener::bind(&addr)
                .await
                .map_err(|source| ApiError::InvalidBindAddress {
                    addr: addr.clone(),
                    source,
                })?;

        #[cfg(unix)]
        drop_privileges(listener_cfg.setuid, listener_cfg.setgid)?;

        info!(
            bind_addr = %addr,
            secure = listener_cfg.secure,
            "webhook-async HTTP receiver listening"
        );

        let result = if let Some(acceptor) = tls_acceptor {
            let tls_listener = tls::TlsListener::new(listener, acceptor);
            axum::serve(tls_listener, app.into_make_service())
                .with_graceful_shutdown(shutdown)
                .await
        } else {
            let make_svc = app.into_make_service_with_connect_info::<SocketAddr>();
            axum::serve(listener, make_svc)
                .with_graceful_shutdown(shutdown)
                .await
        };

        drain_state.drain_background_tasks().await;
        result.map_err(ApiError::Serve)
    }
}

/// Binds TCP listener with queue coordinator and worker registry.
pub async fn serve_with_queue_and_workers_and_shutdown<F>(
    runtime: RuntimeConfig,
    coordinator: Option<QueueCoordinator>,
    worker_registry: Option<webhook_async_worker::WorkerRegistry>,
    shutdown: F,
) -> Result<(), ApiError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let state = ApiState::with_queue_and_workers(runtime, coordinator, worker_registry)?;
    serve_full(state, ListenerConfig::default(), shutdown).await
}

async fn handle_root(State(state): State<ApiState>) -> Response {
    let mut headers = HeaderMap::new();
    apply_response_headers(&mut headers, &state.runtime.app.server.response_headers);
    (StatusCode::OK, headers, "OK").into_response()
}

async fn handle_hook(
    State(state): State<ApiState>,
    Path(raw_hook_id): Path<String>,
    req: Request,
) -> Response {
    let hook_id = raw_hook_id.trim_start_matches('/').to_string();
    let mut resp_headers = HeaderMap::new();
    apply_response_headers(
        &mut resp_headers,
        &state.runtime.app.server.response_headers,
    );

    let req_id = resolve_request_id(
        req.headers(),
        state.runtime.app.server.use_x_request_id,
        state.runtime.app.server.x_request_id_limit,
    );
    let method = req.method().clone();
    let uri = req.uri().clone();

    let Some(resolved) = state.get_hook(&hook_id) else {
        warn!(request_id = %req_id, hook_id = %hook_id, "hook not found");
        return (StatusCode::NOT_FOUND, resp_headers, "Hook not found.").into_response();
    };
    let hook = &resolved.definition;

    // Method allowlist: hook-level `http-methods` overrides server-level `-http-methods`.
    let allowed_methods = if !hook.http_methods.is_empty() {
        &hook.http_methods
    } else {
        &state.runtime.app.server.default_http_methods
    };
    if !is_method_allowed(&method, allowed_methods) {
        warn!(
            request_id = %req_id,
            hook_id = %hook.id,
            method = %method,
            "HTTP method not allowed for hook"
        );
        return (StatusCode::METHOD_NOT_ALLOWED, resp_headers, "").into_response();
    }

    let remote_addr = resolve_remote_addr(
        req.extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|c| c.0),
        req.headers(),
        state.runtime.app.server.trust_proxy_headers,
    );
    let req_content_type = req
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let req_headers_snapshot = req.headers().clone();
    let max_body = state.runtime.app.server.max_body_bytes;
    let body_bytes = match axum::body::to_bytes(req.into_body(), max_body).await {
        Ok(b) => b,
        Err(err) => {
            warn!(request_id = %req_id, hook_id = %hook.id, error = %err, "request body exceeds limit or failed to read");
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                resp_headers,
                "Request body too large.",
            )
                .into_response();
        }
    };

    let mut ctx = RequestContext::new(
        req_id.clone(),
        method.as_str(),
        remote_addr.clone(),
        body_bytes.to_vec(),
    );
    for (name, val) in &req_headers_snapshot {
        if let Ok(s) = val.to_str() {
            ctx.insert_header(name.as_str(), s);
        }
    }
    for (k, v) in form_urlencoded::parse(uri.query().unwrap_or("").as_bytes()) {
        ctx.insert_query_if_absent(k.into_owned(), v.into_owned());
    }

    let effective_ct = hook
        .incoming_payload_content_type
        .as_deref()
        .filter(|s| !s.is_empty())
        .or(req_content_type.as_deref());

    if let Some(ct) = effective_ct {
        let essence = ct.split(';').next().unwrap_or("").trim();
        if essence.eq_ignore_ascii_case("application/json") {
            if let Err(err) = ctx.parse_json_payload() {
                warn!(request_id = %req_id, hook_id = %hook.id, error = %err, "error parsing JSON payload");
            }
        } else if essence.eq_ignore_ascii_case("application/x-www-form-urlencoded") {
            if let Err(err) = ctx.parse_form_payload() {
                warn!(request_id = %req_id, hook_id = %hook.id, error = %err, "error parsing form payload");
            }
        } else if essence.eq_ignore_ascii_case("application/xml")
            || essence.eq_ignore_ascii_case("text/xml")
        {
            if let Err(err) = ctx.parse_xml_payload() {
                warn!(request_id = %req_id, hook_id = %hook.id, error = %err, "error parsing XML payload");
            }
        } else if essence.eq_ignore_ascii_case("multipart/form-data")
            && let Err(err) = ctx
                .parse_multipart_payload(
                    ct,
                    &hook.parse_parameters_as_json,
                    state.runtime.app.server.max_multipart_mem_bytes,
                )
                .await
        {
            warn!(request_id = %req_id, hook_id = %hook.id, error = %err, "error parsing multipart form");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                resp_headers,
                "Error occurred while parsing multipart form.",
            )
                .into_response();
        }
    }

    if !hook.parse_parameters_as_json.is_empty()
        && let Err(err) = ctx.parse_parameters_as_json(&hook.parse_parameters_as_json)
    {
        warn!(request_id = %req_id, hook_id = %hook.id, error = %err, "error parsing parameters as JSON");
    }

    if hook.trigger_rule.is_some() {
        match eval_trigger(hook, &ctx) {
            Ok(TriggerOutcome::Matched) => {}
            Ok(TriggerOutcome::Mismatch { status_code, body }) => {
                let status = resolve_http_status(Some(status_code));
                info!(request_id = %req_id, hook_id = %hook.id, status = %status.as_u16(), "hook rules were not satisfied");
                return (status, resp_headers, body).into_response();
            }
            Err(err) => {
                error!(request_id = %req_id, hook_id = %hook.id, error = %err, "error evaluating hook rules");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    resp_headers,
                    "Error occurred while evaluating hook rules.",
                )
                    .into_response();
            }
        }
    }

    apply_response_headers(&mut resp_headers, &hook.response_headers);

    let spec = match prepare_execution_spec(
        hook,
        &ctx,
        state.runtime.app.queue.default_timeout_seconds,
        state.runtime.app.queue.max_output_bytes,
        &state.runtime.config_version,
    ) {
        Ok(s) => s,
        Err(err) => {
            error!(request_id = %req_id, hook_id = %hook.id, error = %err, "failed to prepare execution spec");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                resp_headers,
                "Error occurred while preparing hook command.",
            )
                .into_response();
        }
    };

    let success_status = resolve_http_status(hook.success_http_response_code);

    if resolved.mode.is_async() {
        let Some(coord) = &state.coordinator else {
            error!(request_id = %req_id, hook_id = %hook.id, "async hook triggered without queue coordinator");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                resp_headers,
                "Queue backend unavailable.",
            )
                .into_response();
        };

        let idempotency_key = req_headers_snapshot
            .get("idempotency-key")
            .or_else(|| req_headers_snapshot.get("x-idempotency-key"))
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);

        let meta = EnqueueMetadata {
            request_id: req_id.clone(),
            request_method: method.as_str().to_string(),
            remote_addr,
            idempotency_key,
            priority: 0,
            max_attempts: hook.max_attempts,
        };

        match coord.enqueue(spec, meta, Utc::now()).await {
            Ok(outcome) => {
                let deduplicated = matches!(
                    outcome,
                    webhook_async_storage::EnqueueOutcome::Deduplicated(_)
                );
                let job = outcome.job();
                info!(
                    request_id = %req_id,
                    hook_id = %hook.id,
                    job_id = %job.id,
                    deduplicated,
                    "hook enqueued asynchronously"
                );
                let message = hook
                    .response_message
                    .clone()
                    .unwrap_or_else(|| "Job enqueued successfully.".to_string());
                let body = serde_json::json!({
                    "job_id": job.id.to_string(),
                    "status": job.state.as_str(),
                    "message": message,
                });
                resp_headers.insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
                (StatusCode::ACCEPTED, resp_headers, body.to_string()).into_response()
            }
            Err(QueueError::CapacityFull { pending, limit }) => {
                warn!(
                    request_id = %req_id,
                    hook_id = %hook.id,
                    pending,
                    limit,
                    "queue capacity exceeded"
                );
                (
                    StatusCode::TOO_MANY_REQUESTS,
                    resp_headers,
                    "Queue capacity full.",
                )
                    .into_response()
            }
            Err(err) => {
                error!(
                    request_id = %req_id,
                    hook_id = %hook.id,
                    error = %err,
                    "failed to enqueue job into storage backend"
                );
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    resp_headers,
                    "Queue backend unavailable.",
                )
                    .into_response()
            }
        }
    } else if hook.include_command_output_in_response {
        match webhook_async_executor::execute(&spec).await {
            Ok(outcome) if outcome.is_success() => {
                info!(request_id = %req_id, hook_id = %hook.id, "hook executed synchronously");
                (
                    success_status,
                    resp_headers,
                    Body::from(outcome.combined_output),
                )
                    .into_response()
            }
            Ok(outcome) => {
                error!(
                    request_id = %req_id,
                    hook_id = %hook.id,
                    exit_code = ?outcome.exit_code,
                    timed_out = outcome.timed_out,
                    "hook command failed"
                );
                if hook.include_command_output_in_response_on_error {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        resp_headers,
                        Body::from(outcome.combined_output),
                    )
                        .into_response()
                } else {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        resp_headers,
                        "Error occurred while executing the hook's command. Please check your logs for more details.",
                    )
                        .into_response()
                }
            }
            Err(err) => {
                error!(request_id = %req_id, hook_id = %hook.id, error = %err, "failed to execute hook command");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    resp_headers,
                    "Error occurred while executing the hook's command. Please check your logs for more details.",
                )
                    .into_response()
            }
        }
    } else {
        let Ok(permit) = state.background_permits.clone().try_acquire_owned() else {
            warn!(request_id = %req_id, hook_id = %hook.id, "background sync execution concurrency limit reached");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                resp_headers,
                "Server busy: in-flight hook limit reached.",
            )
                .into_response();
        };

        let bg_req_id = req_id.clone();
        let bg_hook_id = hook.id.clone();
        tokio::spawn(async move {
            let _permit = permit;
            match webhook_async_executor::execute(&spec).await {
                Ok(outcome) if outcome.is_success() => {
                    info!(request_id = %bg_req_id, hook_id = %bg_hook_id, "hook finished");
                }
                Ok(outcome) => {
                    error!(
                        request_id = %bg_req_id,
                        hook_id = %bg_hook_id,
                        exit_code = ?outcome.exit_code,
                        timed_out = outcome.timed_out,
                        "hook command failed"
                    );
                }
                Err(err) => {
                    error!(request_id = %bg_req_id, hook_id = %bg_hook_id, error = %err, "failed to execute hook command");
                }
            }
        });

        let body = hook.response_message.clone().unwrap_or_default();
        (success_status, resp_headers, body).into_response()
    }
}

trait QueryInsertExt {
    fn insert_query_if_absent(&mut self, key: String, val: String);
}

impl QueryInsertExt for RequestContext {
    fn insert_query_if_absent(&mut self, key: String, val: String) {
        self.query
            .entry(key)
            .or_insert_with(|| serde_json::Value::String(val));
    }
}

fn resolve_http_status(code: Option<u16>) -> StatusCode {
    let Some(code) = code else {
        return StatusCode::OK;
    };
    if code == 0 {
        return StatusCode::OK;
    }
    StatusCode::from_u16(code)
        .ok()
        .filter(|s| s.canonical_reason().is_some())
        .unwrap_or(StatusCode::OK)
}

fn is_method_allowed(method: &Method, allowed: &[String]) -> bool {
    if allowed.is_empty() {
        return true;
    }
    allowed
        .iter()
        .any(|m| m.trim().eq_ignore_ascii_case(method.as_str()))
}

fn apply_response_headers(target: &mut HeaderMap, headers: &[ResponseHeader]) {
    for h in headers {
        let Ok(name) = HeaderName::from_bytes(h.name.as_bytes()) else {
            warn!(header_name = %h.name, "ignoring invalid response header name");
            continue;
        };
        let Ok(val) = HeaderValue::from_str(&h.value) else {
            warn!(header_name = %h.name, "ignoring invalid response header value");
            continue;
        };
        target.insert(name, val);
    }
}

fn resolve_request_id(headers: &HeaderMap, use_header: bool, limit: usize) -> String {
    if use_header && let Some(val) = headers.get("x-request-id").and_then(|v| v.to_str().ok()) {
        let trimmed = val.trim();
        if !trimmed.is_empty() {
            if limit > 0 && trimmed.len() > limit {
                // Truncate at UTF-8 char boundary so valid UTF-8 header values never split mid-char.
                let mut end = limit;
                while end > 0 && !trimmed.is_char_boundary(end) {
                    end -= 1;
                }
                return trimmed[..end].to_string();
            }
            return trimmed.to_string();
        }
    }
    let id = Uuid::new_v4().simple().to_string();
    id[..6].to_string()
}

fn resolve_remote_addr(
    socket_addr: Option<SocketAddr>,
    headers: &HeaderMap,
    trust_proxy_headers: bool,
) -> Option<String> {
    if trust_proxy_headers {
        if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok())
            && let Some(first) = xff.split(',').next().map(str::trim)
            && !first.is_empty()
        {
            return Some(first.to_string());
        }
        if let Some(xri) = headers.get("x-real-ip").and_then(|v| v.to_str().ok()) {
            let trimmed = xri.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    socket_addr.map(|a| a.to_string())
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr};

    use axum::extract::ConnectInfo;
    use axum::http::Request;
    use hmac::{Hmac, KeyInit, Mac};
    use http_body_util::BodyExt;
    use sha1::Sha1;
    use tower::ServiceExt;
    use webhook_async_compat::parse_hooks_json;
    use webhook_async_config::AppConfig;
    use webhook_async_core::{GlobalExecutionMode, HookDefinition, ResponseHeader};

    use super::*;

    fn build_test_router(hooks: Vec<HookDefinition>, mut app: AppConfig) -> Router {
        app.server.url_prefix = "hooks".to_string();
        let runtime = app
            .build_runtime(hooks, GlobalExecutionMode::Unspecified)
            .expect("build sync runtime");
        build_router(ApiState::new(runtime).expect("sync api state"))
    }

    async fn read_body(resp: Response) -> (StatusCode, HeaderMap, String) {
        let status = resp.status();
        let headers = resp.headers().clone();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        (
            status,
            headers,
            String::from_utf8_lossy(&bytes).into_owned(),
        )
    }

    #[tokio::test]
    async fn root_returns_ok_with_global_headers() {
        let mut app = AppConfig::default();
        app.server.response_headers.push(ResponseHeader {
            name: "X-Global".to_string(),
            value: "yes".to_string(),
        });
        let router = build_test_router(vec![], app);

        let resp = router
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let (status, headers, body) = read_body(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "OK");
        assert_eq!(headers.get("X-Global").unwrap(), "yes");
    }

    #[tokio::test]
    async fn unknown_hook_returns_404() {
        let router = build_test_router(vec![], AppConfig::default());
        let resp = router
            .oneshot(
                Request::builder()
                    .uri("/hooks/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _, body) = read_body(resp).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, "Hook not found.");
    }

    #[tokio::test]
    async fn rejects_async_mode_at_startup_until_queue_is_wired() {
        let hooks =
            parse_hooks_json(r#"[{"id":"a","execute-command":"/bin/echo","async":true}]"#).unwrap();
        let app = AppConfig {
            storage: Some(webhook_async_config::StorageConfig {
                backend: webhook_async_config::StorageBackendKind::Sqlite,
                sqlite: Some(webhook_async_config::SqliteStorageConfig {
                    database_url: "sqlite::memory:".to_string(),
                    wal_mode: true,
                    busy_timeout_ms: 5000,
                }),
                valkey: None,
                redis: None,
            }),
            ..Default::default()
        };
        let runtime = app
            .build_runtime(hooks, GlobalExecutionMode::Unspecified)
            .expect("runtime");
        let err = ApiState::new(runtime).err().expect("must fail startup");
        assert!(matches!(err, ApiError::AsyncNotSupportedYet));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn github_hmac_and_sendgrid_slash_hook_and_999_status_fallback() {
        let hooks_json = r#"[
          {
            "id": "github",
            "execute-command": "/bin/echo",
            "http-methods": ["Post "],
            "include-command-output-in-response": true,
            "trigger-rule-mismatch-http-response-code": 400,
            "pass-arguments-to-command": [
              {"source": "payload", "name": "head_commit.id"}
            ],
            "trigger-rule": {
              "and": [
                {
                  "match": {
                    "type": "payload-hmac-sha1",
                    "secret": "mysecret",
                    "parameter": {"source": "header", "name": "X-Hub-Signature"}
                  }
                },
                {
                  "match": {
                    "type": "value",
                    "value": "refs/heads/master",
                    "parameter": {"source": "payload", "name": "ref"}
                  }
                }
              ]
            }
          },
          {
            "id": "sendgrid/dir",
            "execute-command": "/bin/echo",
            "response-message": "success",
            "trigger-rule-mismatch-http-response-code": 999,
            "trigger-rule": {
              "match": {
                "type": "value",
                "parameter": {"source": "payload", "name": "root.0.event"},
                "value": "processed"
              }
            }
          }
        ]"#;
        let hooks = parse_hooks_json(hooks_json).unwrap();
        let router = build_test_router(hooks, AppConfig::default());

        // Method mismatch (GET vs "Post ") -> 405
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/hooks/github")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);

        // Valid GitHub HMAC-SHA1 POST -> 200 with echo output
        let payload = br#"{"ref":"refs/heads/master","head_commit":{"id":"abc123"}}"#;
        let mut mac = Hmac::<Sha1>::new_from_slice(b"mysecret").unwrap();
        mac.update(payload);
        let sig = format!(
            "sha1={}",
            mac.finalize()
                .into_bytes()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        );
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hooks/github")
                    .header("Content-Type", "application/json")
                    .header("X-Hub-Signature", sig)
                    .body(Body::from(&payload[..]))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _, body) = read_body(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "abc123\n");

        // Sendgrid slash hook mismatch with invalid HTTP code 999 -> falls back to 200
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/hooks/sendgrid/dir")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"[{"event":"bounce"}]"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _, body) = read_body(resp).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "Hook rules were not satisfied.");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn capture_output_on_error_and_ip_whitelist_and_empty_prefix() {
        let hooks_json = r#"[
          {
            "id": "fail-hidden",
            "execute-command": "/bin/sh",
            "include-command-output-in-response": true,
            "pass-arguments-to-command": [
              {"source": "string", "name": "-c"},
              {"source": "string", "name": "echo secret-err; exit 1"}
            ]
          },
          {
            "id": "fail-shown",
            "execute-command": "/bin/sh",
            "include-command-output-in-response": true,
            "include-command-output-in-response-on-error": true,
            "pass-arguments-to-command": [
              {"source": "string", "name": "-c"},
              {"source": "string", "name": "echo visible-err; exit 1"}
            ]
          },
          {
            "id": "ip-guarded",
            "execute-command": "/bin/echo",
            "response-message": "allowed",
            "trigger-rule-mismatch-http-response-code": 403,
            "trigger-rule": {
              "match": {"type": "ip-whitelist", "ip-range": "10.0.0.0/8"}
            }
          }
        ]"#;
        let hooks = parse_hooks_json(hooks_json).unwrap();
        let mut app = AppConfig::default();
        app.server.url_prefix = String::new();
        let runtime = app
            .build_runtime(hooks, GlobalExecutionMode::Unspecified)
            .unwrap();
        let state = ApiState::new(runtime).unwrap();
        let router = build_router(state.clone());

        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/fail-hidden")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _, body) = read_body(resp).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.contains("Error occurred while executing"));

        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/fail-shown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, _, body) = read_body(resp).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body, "visible-err\n");

        // IP whitelist via ConnectInfo extension
        let mut req_denied = Request::builder()
            .method("POST")
            .uri("/ip-guarded")
            .body(Body::empty())
            .unwrap();
        req_denied
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(192, 168, 1, 5)),
                12345,
            )));
        let (status, _, _) = read_body(router.clone().oneshot(req_denied).await.unwrap()).await;
        assert_eq!(status, StatusCode::FORBIDDEN);

        let mut req_ok = Request::builder()
            .method("POST")
            .uri("/ip-guarded")
            .body(Body::empty())
            .unwrap();
        req_ok.extensions_mut().insert(ConnectInfo(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)),
            12345,
        )));
        let (status, _, body) = read_body(router.oneshot(req_ok).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "allowed");

        state.drain_background_tasks().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn handles_xml_form_multipart_and_background_sync_execution() {
        let hooks_json = r#"[
          {
            "id": "xml-hook",
            "execute-command": "/bin/echo",
            "include-command-output-in-response": true,
            "pass-arguments-to-command": [
              {"source": "payload", "name": "app.users.user.0.-name"},
              {"source": "payload", "name": "app.messages.message.#text"}
            ]
          },
          {
            "id": "bitbucket-form",
            "execute-command": "/bin/echo",
            "include-command-output-in-response": true,
            "parse-parameters-as-json": [
              {"source": "payload", "name": "payload"}
            ],
            "pass-arguments-to-command": [
              {"source": "payload", "name": "payload.commits.0.branch"}
            ]
          },
          {
            "id": "plex-multipart",
            "execute-command": "/bin/echo",
            "include-command-output-in-response": true,
            "parse-parameters-as-json": [
              {"source": "payload", "name": "payload"}
            ],
            "trigger-rule": {
              "match": {
                "type": "value",
                "parameter": {"source": "payload", "name": "payload.event"},
                "value": "media.play"
              }
            },
            "pass-arguments-to-command": [
              {"source": "payload", "name": "payload.event"}
            ]
          },
          {
            "id": "bg-hook",
            "execute-command": "/bin/true",
            "response-message": "accepted-sync",
            "success-http-response-code": 201
          }
        ]"#;

        let hooks = parse_hooks_json(hooks_json).unwrap();
        let runtime = AppConfig::default()
            .build_runtime(hooks, GlobalExecutionMode::Unspecified)
            .unwrap();
        let state = ApiState::new(runtime).unwrap();
        let router = build_router(state.clone());

        // XML
        let xml_body = r#"<app><users><user name="Jeff"/><user name="Alice"/></users><messages><message id="1">Hello!!</message></messages></app>"#;
        let (status, _, body) = read_body(
            router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/xml-hook")
                        .header("Content-Type", "application/xml")
                        .body(Body::from(xml_body))
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "Jeff Hello!!\n");

        // Form URL-encoded + parse-parameters-as-json
        let (status, _, body) = read_body(
            router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/bitbucket-form")
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .body(Body::from(
                            "payload=%7B%22commits%22%3A%5B%7B%22branch%22%3A%22master%22%7D%5D%7D",
                        ))
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "master\n");

        // Multipart form-data + parse-parameters-as-json
        let multipart_body = "--B\r\nContent-Disposition: form-data; name=\"payload\"\r\n\r\n{\"event\":\"media.play\"}\r\n--B--\r\n";
        let (status, _, body) = read_body(
            router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/plex-multipart")
                        .header("Content-Type", "multipart/form-data; boundary=B")
                        .body(Body::from(multipart_body))
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "media.play\n");

        // Non-capturing sync hook returns configured response immediately and drains cleanly
        let (status, _, body) = read_body(
            router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/bg-hook")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body, "accepted-sync");

        state.drain_background_tasks().await;
    }

    async fn sqlite_coordinator(
        max_pending_jobs: u64,
    ) -> (
        tempfile::TempDir,
        Arc<webhook_async_storage_sqlite::SqliteStorage>,
        QueueCoordinator,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir
            .path()
            .join("api_async.db")
            .to_string_lossy()
            .into_owned();
        let storage = Arc::new(
            webhook_async_storage_sqlite::SqliteStorage::connect(
                &webhook_async_config::SqliteStorageConfig {
                    database_url: db_path,
                    wal_mode: true,
                    busy_timeout_ms: 5000,
                },
            )
            .await
            .expect("sqlite connect"),
        );
        let queue_cfg = webhook_async_config::QueueConfig {
            max_pending_jobs,
            ..Default::default()
        };
        let coord = QueueCoordinator::new(storage.clone(), queue_cfg);
        (dir, storage, coord)
    }

    fn async_sqlite_app(max_pending_jobs: u64) -> AppConfig {
        AppConfig {
            storage: Some(webhook_async_config::StorageConfig {
                backend: webhook_async_config::StorageBackendKind::Sqlite,
                sqlite: Some(webhook_async_config::SqliteStorageConfig {
                    database_url: "sqlite::memory:".to_string(),
                    wal_mode: true,
                    busy_timeout_ms: 5000,
                }),
                valkey: None,
                redis: None,
            }),
            queue: webhook_async_config::QueueConfig {
                max_pending_jobs,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn async_hook_returns_202_after_durable_enqueue_and_deduplicates_idempotency() {
        let hooks = parse_hooks_json(
            r#"[{
              "id": "deploy",
              "execute-command": "/bin/echo",
              "async": true,
              "response-message": "queued-ok",
              "pass-arguments-to-command": [{"source": "payload", "name": "env"}]
            }]"#,
        )
        .unwrap();
        let (_dir, storage, coord) = sqlite_coordinator(100).await;
        let runtime = async_sqlite_app(100)
            .build_runtime(hooks, GlobalExecutionMode::Unspecified)
            .unwrap();
        let state = ApiState::with_queue(runtime, Some(coord)).unwrap();
        let router = build_router(state);

        let (status1, headers1, body1) = read_body(
            router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/deploy")
                        .header("Content-Type", "application/json")
                        .header("Idempotency-Key", "req-42")
                        .body(Body::from(r#"{"env":"prod"}"#))
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status1, StatusCode::ACCEPTED);
        assert_eq!(
            headers1.get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let parsed1: serde_json::Value = serde_json::from_str(&body1).unwrap();
        assert_eq!(parsed1["status"], "queued");
        assert_eq!(parsed1["message"], "queued-ok");
        let job_id: webhook_async_core::JobId =
            parsed1["job_id"].as_str().unwrap().parse().unwrap();

        // Persisted snapshot matches request before any worker runs
        let stored = webhook_async_storage::QueueStorage::get_job(storage.as_ref(), job_id)
            .await
            .unwrap()
            .expect("job persisted");
        assert_eq!(stored.execution_spec.args, vec!["prod".to_string()]);
        assert_eq!(stored.idempotency_key.as_deref(), Some("req-42"));

        // Duplicate Idempotency-Key returns same job_id without creating a second job
        let (status2, _, body2) = read_body(
            router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/deploy")
                        .header("Content-Type", "application/json")
                        .header("Idempotency-Key", "req-42")
                        .body(Body::from(r#"{"env":"staging"}"#))
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status2, StatusCode::ACCEPTED);
        let parsed2: serde_json::Value = serde_json::from_str(&body2).unwrap();
        assert_eq!(parsed2["job_id"], parsed1["job_id"]);
        assert_eq!(
            webhook_async_storage::QueueStorage::count_pending(storage.as_ref())
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn async_hook_returns_429_when_queue_capacity_full() {
        let hooks =
            parse_hooks_json(r#"[{"id":"a","execute-command":"/bin/echo","async":true}]"#).unwrap();
        let (_dir, _storage, coord) = sqlite_coordinator(1).await;
        let runtime = async_sqlite_app(1)
            .build_runtime(hooks, GlobalExecutionMode::Unspecified)
            .unwrap();
        let router = build_router(ApiState::with_queue(runtime, Some(coord)).unwrap());

        let (s1, _, _) = read_body(
            router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/a")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(s1, StatusCode::ACCEPTED);

        let (s2, _, b2) = read_body(
            router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/a")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(s2, StatusCode::TOO_MANY_REQUESTS);
        assert!(b2.contains("capacity"));
    }

    #[tokio::test]
    async fn async_hook_returns_503_when_storage_unavailable_and_never_202() {
        let hooks =
            parse_hooks_json(r#"[{"id":"a","execute-command":"/bin/echo","async":true}]"#).unwrap();
        let (_dir, storage, coord) = sqlite_coordinator(10).await;
        // Close the underlying pool so enqueue fails cleanly
        storage.close().await;

        let runtime = async_sqlite_app(10)
            .build_runtime(hooks, GlobalExecutionMode::Unspecified)
            .unwrap();
        let router = build_router(ApiState::with_queue(runtime, Some(coord)).unwrap());

        let (status, _, body) = read_body(
            router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/a")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("unavailable"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn mixed_sync_and_async_hooks_honor_per_hook_override_under_global_async() {
        let hooks = parse_hooks_json(
            r#"[
              {
                "id": "sync-override",
                "execute-command": "/bin/echo",
                "async": false,
                "include-command-output-in-response": true,
                "pass-arguments-to-command": [{"source": "string", "name": "inline"}]
              },
              {
                "id": "default-async",
                "execute-command": "/bin/echo",
                "pass-arguments-to-command": [{"source": "string", "name": "queued"}]
              }
            ]"#,
        )
        .unwrap();
        let (_dir, storage, coord) = sqlite_coordinator(10).await;
        let runtime = async_sqlite_app(10)
            .build_runtime(hooks, GlobalExecutionMode::Async)
            .unwrap();
        let router = build_router(ApiState::with_queue(runtime, Some(coord)).unwrap());

        let (s_sync, _, b_sync) = read_body(
            router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/sync-override")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(s_sync, StatusCode::OK);
        assert_eq!(b_sync, "inline\n");
        assert_eq!(
            webhook_async_storage::QueueStorage::count_pending(storage.as_ref())
                .await
                .unwrap(),
            0
        );

        let (s_async, _, b_async) = read_body(
            router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/default-async")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(s_async, StatusCode::ACCEPTED);
        let parsed: serde_json::Value = serde_json::from_str(&b_async).unwrap();
        assert_eq!(parsed["status"], "queued");
        assert_eq!(
            webhook_async_storage::QueueStorage::count_pending(storage.as_ref())
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn state_reload_hooks_updates_registered_hooks_dynamically() {
        let hooks_v1 = parse_hooks_json(
            r#"[{
              "id": "dynamic-hook",
              "execute-command": "/bin/echo",
              "response-message": "version-1"
            }]"#,
        )
        .unwrap();
        let runtime = AppConfig::default()
            .build_runtime(hooks_v1, GlobalExecutionMode::Sync)
            .unwrap();
        let state = ApiState::new(runtime).unwrap();
        let router = build_router(state.clone());

        let (_, _, body1) = read_body(
            router
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/dynamic-hook")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(body1, "version-1");

        // Dynamically reload with version 2 definition
        let hooks_v2 = parse_hooks_json(
            r#"[{
              "id": "dynamic-hook",
              "execute-command": "/bin/echo",
              "response-message": "version-2"
            }]"#,
        )
        .unwrap();
        let runtime_v2 = AppConfig::default()
            .build_runtime(hooks_v2, GlobalExecutionMode::Sync)
            .unwrap();
        state.reload_hooks(runtime_v2.hooks);

        let (_, _, body2) = read_body(
            router
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/hooks/dynamic-hook")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(body2, "version-2");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn serve_full_binds_unix_socket_and_cleans_up_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("webhook.sock");

        let runtime = AppConfig::default()
            .build_runtime(Vec::new(), GlobalExecutionMode::Sync)
            .unwrap();
        let state = ApiState::new(runtime).unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();

        let sock_clone = socket_path.clone();
        let server_handle = tokio::spawn(async move {
            serve_full(
                state,
                ListenerConfig {
                    unix_socket: Some(sock_clone),
                    ..Default::default()
                },
                async move {
                    let _ = rx.await;
                },
            )
            .await
        });

        // Poll until socket is bound
        for _ in 0..50 {
            if socket_path.exists() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(socket_path.exists());

        // Connect via UnixStream to test listener
        let mut stream = tokio::net::UnixStream::connect(&socket_path)
            .await
            .expect("connect to unix socket");
        tokio::io::AsyncWriteExt::write_all(
            &mut stream,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
        let mut resp_buf = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut resp_buf)
            .await
            .unwrap();
        let resp_str = String::from_utf8_lossy(&resp_buf);
        assert!(resp_str.starts_with("HTTP/1.1 200 OK"));

        // Trigger shutdown and verify socket file removal
        let _ = tx.send(());
        let server_res = server_handle.await.unwrap();
        assert!(server_res.is_ok());
        assert!(!socket_path.exists());
    }

    #[tokio::test]
    async fn serve_full_validates_listener_flags() {
        let runtime = AppConfig::default()
            .build_runtime(Vec::new(), GlobalExecutionMode::Sync)
            .unwrap();
        let state = ApiState::new(runtime).unwrap();

        // Incompatible: socket + setuid
        let res = serve_full(
            state.clone(),
            ListenerConfig {
                unix_socket: Some(PathBuf::from("/tmp/test.sock")),
                setuid: Some(1000),
                setgid: Some(1000),
                ..Default::default()
            },
            async {},
        )
        .await;
        assert!(matches!(res, Err(ApiError::InvalidConfiguration(_))));

        // Unpaired setuid without setgid
        let res2 = serve_full(
            state,
            ListenerConfig {
                setuid: Some(1000),
                setgid: None,
                ..Default::default()
            },
            async {},
        )
        .await;
        assert!(matches!(res2, Err(ApiError::InvalidConfiguration(_))));
    }
}
