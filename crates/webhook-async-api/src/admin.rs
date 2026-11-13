//! Administrative REST API endpoints for webhook-async (v1).
//!
//! Provides operational endpoints under `/api/v1/...`:
//! - Public: `/health`, `/ready`, `/openapi.json`
//! - Authenticated & rate-limited: `/stats`, `/jobs`, `/jobs/{id}`, `/jobs/{id}/retry`,
//!   `/jobs/{id}/cancel`, `/hooks`, `/workers`, `/storage/status`, `/events` (SSE)

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use chrono::Utc;
use futures_util::stream;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;
use webhook_async_core::JobState;
use webhook_async_core::hook::TriggerRule;
use webhook_async_core::job::{JobAttempt, JobId, JobRecord, WorkerInfo};
use webhook_async_queue::QueueError;
use webhook_async_storage::{JobFilter, JobPage, QueueStats, StorageError};

use crate::ApiState;

const ADMIN_RATE_LIMIT_PER_MINUTE: u32 = 60;
const OPENAPI_JSON: &str = include_str!("openapi.json");

/// In-memory fixed-window rate limiter for administrative endpoints.
#[derive(Clone, Default)]
pub struct AdminRateLimiter {
    // Key (token or client identifier) -> (window_start_sec, request_count)
    windows: Arc<Mutex<HashMap<String, (u64, u32)>>>,
}

impl AdminRateLimiter {
    pub async fn check(&self, key: &str) -> bool {
        let now_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut lock = self.windows.lock().await;

        // Bound memory if lots of distinct keys appear
        if lock.len() > 1000 {
            lock.retain(|_, (win_start, _)| now_sec.saturating_sub(*win_start) < 120);
        }

        let entry = lock.entry(key.to_string()).or_insert((now_sec, 0));
        if now_sec.saturating_sub(entry.0) < 60 {
            if entry.1 >= ADMIN_RATE_LIMIT_PER_MINUTE {
                return false;
            }
            entry.1 += 1;
            true
        } else {
            *entry = (now_sec, 1);
            true
        }
    }
}

/// JSON error response structure.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl ErrorResponse {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { error: msg.into() }
    }

    pub fn response(status: StatusCode, msg: impl Into<String>) -> Response {
        (status, Json(Self::new(msg))).into_response()
    }
}

/// Verifies Bearer authentication and enforces fixed-window rate limits.
#[allow(clippy::result_large_err)]
pub async fn authorize_admin_request(
    state: &ApiState,
    headers: &HeaderMap,
) -> Result<String, Response> {
    let configured_token = match state.admin_token() {
        Some(token) => token,
        None => {
            return Err(ErrorResponse::response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Admin token not configured. Set [auth].admin_token, [auth].admin_token_env, or [auth].admin_token_file in configuration.",
            ));
        }
    };

    let auth_header = headers.get(header::AUTHORIZATION).ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            Json(ErrorResponse::new("Missing Bearer authorization header.")),
        )
            .into_response()
    })?;

    let auth_str = auth_header.to_str().map_err(|_| {
        ErrorResponse::response(
            StatusCode::UNAUTHORIZED,
            "Authorization header contains invalid characters.",
        )
    })?;

    let token = if let Some(stripped) = auth_str.strip_prefix("Bearer ") {
        stripped.trim()
    } else if let Some(stripped) = auth_str.strip_prefix("bearer ") {
        stripped.trim()
    } else {
        return Err(ErrorResponse::response(
            StatusCode::UNAUTHORIZED,
            "Invalid authorization scheme; Bearer token is required.",
        ));
    };

    let matches: bool = token.as_bytes().ct_eq(configured_token.as_bytes()).into();
    if !matches {
        return Err(ErrorResponse::response(
            StatusCode::FORBIDDEN,
            "Invalid admin authorization token.",
        ));
    }

    if !state.rate_limiter().check(token).await {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, "60")],
            Json(ErrorResponse::new(
                "Admin rate limit exceeded. Maximum 60 requests per minute.",
            )),
        )
            .into_response());
    }

    Ok(token.to_string())
}

/// Builds administrative API routes mounted at `/api/v1`.
pub fn routes() -> Router<ApiState> {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/openapi.json", get(openapi))
        .route("/stats", get(stats))
        .route("/jobs", get(jobs))
        .route("/jobs/{id}", get(job_detail))
        .route("/jobs/{id}/retry", post(job_retry))
        .route("/jobs/{id}/cancel", post(job_cancel))
        .route("/hooks", get(hooks))
        .route("/workers", get(workers))
        .route("/storage/status", get(storage_status))
        .route("/events", get(events))
}

#[derive(Serialize)]
struct HealthStatus {
    status: &'static str,
    version: &'static str,
}

async fn health() -> Json<HealthStatus> {
    Json(HealthStatus {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

#[derive(Serialize)]
struct ReadyStatus {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

async fn ready(State(state): State<ApiState>) -> Response {
    if let Some(coord) = state.coordinator() {
        match coord.storage().ping().await {
            Ok(()) => Json(ReadyStatus {
                status: "ready",
                error: None,
            })
            .into_response(),
            Err(e) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ReadyStatus {
                    status: "not_ready",
                    error: Some(e.to_string()),
                }),
            )
                .into_response(),
        }
    } else if state.runtime().requires_queue_backend() {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadyStatus {
                status: "not_ready",
                error: Some("Queue coordinator not initialized".to_string()),
            }),
        )
            .into_response()
    } else {
        Json(ReadyStatus {
            status: "ready",
            error: None,
        })
        .into_response()
    }
}

async fn openapi() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        OPENAPI_JSON,
    )
        .into_response()
}

async fn stats(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    if let Err(resp) = authorize_admin_request(&state, &headers).await {
        return resp;
    }

    if let Some(coord) = state.coordinator() {
        match coord.storage().stats().await {
            Ok(stats) => Json(stats).into_response(),
            Err(e) => ErrorResponse::response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Failed to retrieve queue statistics: {e}"),
            ),
        }
    } else {
        Json(QueueStats {
            counts_by_state: BTreeMap::new(),
            pending_count: 0,
        })
        .into_response()
    }
}

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    pub state: Option<String>,
    pub hook_id: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u64>,
}

async fn jobs(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Query(query): Query<ListJobsQuery>,
) -> Response {
    if let Err(resp) = authorize_admin_request(&state, &headers).await {
        return resp;
    }

    let parsed_state = if let Some(s) = &query.state {
        match s.parse::<JobState>() {
            Ok(st) => Some(st),
            Err(_) => {
                return ErrorResponse::response(
                    StatusCode::BAD_REQUEST,
                    format!("Invalid job state filter '{s}'"),
                );
            }
        }
    } else {
        None
    };

    let limit = query.limit.unwrap_or(50);
    let offset = query.offset.unwrap_or(0);

    let filter = JobFilter {
        state: parsed_state,
        hook_id: query.hook_id,
        limit,
        offset,
    };

    if let Err(err) = filter.validate() {
        return ErrorResponse::response(StatusCode::BAD_REQUEST, err.to_string());
    }

    if let Some(coord) = state.coordinator() {
        match coord.storage().list_jobs(filter).await {
            Ok(page) => Json(page).into_response(),
            Err(e) => ErrorResponse::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to list jobs: {e}"),
            ),
        }
    } else {
        Json(JobPage {
            items: Vec::new(),
            total: 0,
            limit,
            offset,
        })
        .into_response()
    }
}

#[derive(Serialize)]
pub struct JobDetailResponse {
    #[serde(flatten)]
    pub job: JobRecord,
    pub attempts: Vec<JobAttempt>,
}

async fn job_detail(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(resp) = authorize_admin_request(&state, &headers).await {
        return resp;
    }

    let job_id = match id.parse::<JobId>() {
        Ok(id) => id,
        Err(_) => {
            return ErrorResponse::response(
                StatusCode::BAD_REQUEST,
                format!("Invalid job ID format '{id}'"),
            );
        }
    };

    let coord = match state.coordinator() {
        Some(c) => c,
        None => return ErrorResponse::response(StatusCode::NOT_FOUND, "Job not found"),
    };

    let job = match coord.storage().get_job(job_id).await {
        Ok(Some(j)) => j,
        Ok(None) | Err(StorageError::NotFound(_)) => {
            return ErrorResponse::response(StatusCode::NOT_FOUND, "Job not found");
        }
        Err(e) => {
            return ErrorResponse::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to retrieve job: {e}"),
            );
        }
    };

    let attempts = match coord.storage().list_attempts(job_id).await {
        Ok(a) => a,
        Err(e) => {
            return ErrorResponse::response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to retrieve job attempts: {e}"),
            );
        }
    };

    Json(JobDetailResponse { job, attempts }).into_response()
}

async fn job_retry(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(resp) = authorize_admin_request(&state, &headers).await {
        return resp;
    }

    let job_id = match id.parse::<JobId>() {
        Ok(id) => id,
        Err(_) => {
            return ErrorResponse::response(
                StatusCode::BAD_REQUEST,
                format!("Invalid job ID format '{id}'"),
            );
        }
    };

    let coord = match state.coordinator() {
        Some(c) => c,
        None => {
            return ErrorResponse::response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Queue coordinator not configured",
            );
        }
    };

    match coord.retry_job(job_id, Utc::now()).await {
        Ok(job) => Json(job).into_response(),
        Err(QueueError::Storage(StorageError::NotFound(_))) => {
            ErrorResponse::response(StatusCode::NOT_FOUND, "Job not found")
        }
        Err(QueueError::Storage(StorageError::InvalidTransition { from, to, .. })) => {
            ErrorResponse::response(
                StatusCode::CONFLICT,
                format!(
                    "Job in state '{from}' cannot be retried (target transition to '{to}' is invalid)"
                ),
            )
        }
        Err(e) => ErrorResponse::response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to retry job: {e}"),
        ),
    }
}

async fn job_cancel(
    State(state): State<ApiState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let Err(resp) = authorize_admin_request(&state, &headers).await {
        return resp;
    }

    let job_id = match id.parse::<JobId>() {
        Ok(id) => id,
        Err(_) => {
            return ErrorResponse::response(
                StatusCode::BAD_REQUEST,
                format!("Invalid job ID format '{id}'"),
            );
        }
    };

    let coord = match state.coordinator() {
        Some(c) => c,
        None => {
            return ErrorResponse::response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Queue coordinator not configured",
            );
        }
    };

    match coord.cancel_job(job_id, Utc::now()).await {
        Ok(job) => Json(job).into_response(),
        Err(QueueError::Storage(StorageError::NotFound(_))) => {
            ErrorResponse::response(StatusCode::NOT_FOUND, "Job not found")
        }
        Err(QueueError::Storage(StorageError::InvalidTransition { from, to, .. })) => {
            ErrorResponse::response(
                StatusCode::CONFLICT,
                format!(
                    "Job in state '{from}' cannot be cancelled (target transition to '{to}' is invalid)"
                ),
            )
        }
        Err(e) => ErrorResponse::response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to cancel job: {e}"),
        ),
    }
}

/// Redacted summary of a configured hook safe for administrative display.
///
/// Secrets in trigger rules and command arguments are never exposed.
#[derive(Debug, Serialize)]
pub struct HookSummary {
    pub id: String,
    pub execution_mode: String,
    pub http_methods: Vec<String>,
    pub max_attempts: Option<u32>,
    pub command_working_directory: Option<String>,
    pub response_message: Option<String>,
    pub include_command_output_in_response: bool,
    pub trigger_rule_count: usize,
}

fn count_trigger_rules(rule: &TriggerRule) -> usize {
    match rule {
        TriggerRule::And(items) | TriggerRule::Or(items) => {
            items.iter().map(count_trigger_rules).sum()
        }
        TriggerRule::Not(inner) => count_trigger_rules(inner),
        TriggerRule::Match(_) => 1,
        TriggerRule::Empty => 0,
    }
}

async fn hooks(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    if let Err(resp) = authorize_admin_request(&state, &headers).await {
        return resp;
    }

    let mut hooks_list: Vec<HookSummary> = state
        .list_hooks()
        .iter()
        .map(|h| {
            let def = &h.definition;
            let trigger_rule_count = def
                .trigger_rule
                .as_ref()
                .map(count_trigger_rules)
                .unwrap_or(0);
            let execution_mode = if h.mode.is_async() {
                "async".to_string()
            } else {
                "sync".to_string()
            };
            HookSummary {
                id: def.id.clone(),
                execution_mode,
                http_methods: def.http_methods.clone(),
                max_attempts: def.max_attempts,
                command_working_directory: def.command_working_directory.clone(),
                response_message: def.response_message.clone(),
                include_command_output_in_response: def.include_command_output_in_response,
                trigger_rule_count,
            }
        })
        .collect();

    hooks_list.sort_by(|a, b| a.id.cmp(&b.id));
    Json(hooks_list).into_response()
}

#[derive(Serialize)]
struct WorkersResponse {
    workers: Vec<WorkerInfo>,
}

async fn workers(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    if let Err(resp) = authorize_admin_request(&state, &headers).await {
        return resp;
    }

    let workers = if let Some(registry) = state.worker_registry() {
        registry.list().await
    } else {
        Vec::new()
    };

    Json(WorkersResponse { workers }).into_response()
}

#[derive(Serialize)]
struct StorageStatusResponse {
    backend: &'static str,
    supports_priority: bool,
    supports_multi_host: bool,
    supports_idempotency: bool,
    connected: bool,
    pending_jobs: u64,
}

async fn storage_status(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    if let Err(resp) = authorize_admin_request(&state, &headers).await {
        return resp;
    }

    if let Some(coord) = state.coordinator() {
        let caps = coord.storage().capabilities();
        let connected = coord.storage().ping().await.is_ok();
        let pending = coord.storage().count_pending().await.unwrap_or(0);

        Json(StorageStatusResponse {
            backend: caps.backend,
            supports_priority: caps.supports_priority,
            supports_multi_host: caps.supports_multi_host,
            supports_idempotency: caps.supports_idempotency,
            connected,
            pending_jobs: pending,
        })
        .into_response()
    } else {
        Json(StorageStatusResponse {
            backend: "none",
            supports_priority: false,
            supports_multi_host: false,
            supports_idempotency: false,
            connected: false,
            pending_jobs: 0,
        })
        .into_response()
    }
}

async fn events(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    if let Err(resp) = authorize_admin_request(&state, &headers).await {
        return resp;
    }

    let stream = stream::unfold(state, |state| async move {
        tokio::time::sleep(Duration::from_secs(2)).await;

        let stats_json = if let Some(coord) = state.coordinator() {
            coord
                .storage()
                .stats()
                .await
                .ok()
                .and_then(|s| serde_json::to_string(&s).ok())
                .unwrap_or_else(|| "{}".to_string())
        } else {
            "{}".to_string()
        };

        let event = Event::default().event("stats").data(stats_json);
        Some((Ok::<_, Infallible>(event), state))
    });

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;
    use webhook_async_config::{AppConfig, AuthConfig, ResolvedHook, RuntimeConfig, ServerConfig};
    use webhook_async_core::hook::HookDefinition;
    use webhook_async_core::mode::{EffectiveExecutionMode, GlobalExecutionMode};
    use webhook_async_queue::{EnqueueMetadata, QueueCoordinator};
    use webhook_async_storage_sqlite::SqliteStorage;
    use webhook_async_worker::WorkerRegistry;

    use crate::{ApiState, build_router};

    fn make_test_runtime(admin_token: Option<&str>) -> RuntimeConfig {
        let mut app = AppConfig::default();
        if let Some(token) = admin_token {
            let var_name = format!("TEST_ADMIN_TOKEN_{}", uuid::Uuid::new_v4().simple());
            unsafe {
                std::env::set_var(&var_name, token);
            }
            app.auth = AuthConfig {
                admin_token: None,
                admin_token_env: Some(var_name),
                admin_token_file: None,
            };
        }
        app.server = ServerConfig {
            host: "127.0.0.1".into(),
            port: 9000,
            url_prefix: "hooks".into(),
            max_body_bytes: 1048576,
            max_multipart_mem_bytes: 1048576,
            hook_files: vec![],
            hot_reload: false,
            default_http_methods: vec!["POST".into()],
            response_headers: vec![],
            use_x_request_id: false,
            x_request_id_limit: 0,
            trust_proxy_headers: false,
        };

        let hook = HookDefinition {
            id: "deploy".into(),
            execute_command: "/bin/echo".into(),
            command_working_directory: Some("/tmp".into()),
            response_message: Some("ok".into()),
            response_headers: vec![],
            include_command_output_in_response: false,
            include_command_output_in_response_on_error: false,
            pass_environment_to_command: vec![],
            pass_arguments_to_command: vec![],
            pass_file_to_command: vec![],
            parse_parameters_as_json: vec![],
            trigger_rule: None,
            trigger_rule_mismatch_http_response_code: None,
            trigger_signature_soft_failures: false,
            incoming_payload_content_type: None,
            success_http_response_code: None,
            http_methods: vec!["POST".into()],
            async_mode: Some(true),
            timeout_seconds: None,
            max_attempts: Some(3),
        };

        RuntimeConfig {
            app,
            hooks: vec![ResolvedHook {
                definition: hook,
                mode: EffectiveExecutionMode::Sync,
            }],
            global_mode: GlobalExecutionMode::Sync,
            config_version: "v1".into(),
        }
    }

    #[tokio::test]
    async fn public_endpoints_do_not_require_auth() {
        let runtime = make_test_runtime(None);
        let state = ApiState::with_queue_and_workers(runtime, None, None).unwrap();
        let app = build_router(state);

        // GET /api/v1/health
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let val: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(val["status"], "ok");

        // GET /api/v1/ready
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // GET /api/v1/openapi.json
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let val: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(val["openapi"], "3.1.0");
    }

    #[tokio::test]
    async fn admin_auth_and_rate_limiting() {
        // 1. Without admin token configured -> 503
        let runtime_no_auth = make_test_runtime(None);
        let state_no_auth = ApiState::with_queue_and_workers(runtime_no_auth, None, None).unwrap();
        let app_no_auth = build_router(state_no_auth);

        let res = app_no_auth
            .oneshot(
                Request::builder()
                    .uri("/api/v1/stats")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);

        // 2. With admin token configured
        let runtime = make_test_runtime(Some("secret-token-123"));
        let state = ApiState::with_queue_and_workers(runtime, None, None).unwrap();
        let app = build_router(state);

        // Missing Authorization -> 401
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Bearer"
        );

        // Invalid token -> 403
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/stats")
                    .header(header::AUTHORIZATION, "Bearer wrong-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        // Valid token -> 200
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/stats")
                    .header(header::AUTHORIZATION, "Bearer secret-token-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn admin_rate_limiter_enforces_limit() {
        let runtime = make_test_runtime(Some("rate-limit-token"));
        let state = ApiState::with_queue_and_workers(runtime, None, None).unwrap();
        let app = build_router(state);

        for _ in 0..60 {
            let res = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/v1/stats")
                        .header(header::AUTHORIZATION, "Bearer rate-limit-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        }

        // 61st request in the same minute -> 429 Too Many Requests
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/stats")
                    .header(header::AUTHORIZATION, "Bearer rate-limit-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(res.headers().get(header::RETRY_AFTER).unwrap(), "60");
    }

    #[tokio::test]
    async fn hooks_metadata_redacts_secrets() {
        let runtime = make_test_runtime(Some("secret-token-123"));
        let state = ApiState::with_queue_and_workers(runtime, None, None).unwrap();
        let app = build_router(state);

        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/hooks")
                    .header(header::AUTHORIZATION, "Bearer secret-token-123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let val: Value = serde_json::from_slice(&body).unwrap();
        let arr = val.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], "deploy");
        assert_eq!(arr[0]["execution_mode"], "sync");
        // Verify sensitive command arguments are not serialized
        assert!(arr[0].get("execute_command").is_none());
        assert!(arr[0].get("pass_arguments_to_command").is_none());
    }

    #[tokio::test]
    async fn jobs_management_and_workers_registry() {
        let storage = Arc::new(
            SqliteStorage::connect(&webhook_async_config::SqliteStorageConfig {
                database_url: "sqlite::memory:".to_string(),
                wal_mode: false,
                busy_timeout_ms: 5000,
            })
            .await
            .unwrap(),
        );
        let coord = QueueCoordinator::new(storage.clone(), Default::default());
        let registry = WorkerRegistry::new();

        let runtime = make_test_runtime(Some("admin-token"));
        let state =
            ApiState::with_queue_and_workers(runtime, Some(coord.clone()), Some(registry.clone()))
                .unwrap();
        let app = build_router(state);

        // 1. Enqueue a test job
        let spec = webhook_async_core::job::ExecutionSpec {
            hook_id: "deploy".into(),
            executable: "/bin/echo".into(),
            args: vec!["test".into()],
            env: Default::default(),
            working_dir: None,
            temp_files: vec![],
            timeout_seconds: 30,
            capture_output: true,
            max_output_bytes: 65536,
            config_version: "v1".into(),
        };
        let meta = EnqueueMetadata {
            request_id: "req-1".into(),
            request_method: "POST".into(),
            remote_addr: None,
            idempotency_key: None,
            priority: 1,
            max_attempts: Some(3),
        };
        let outcome = coord.enqueue(spec, meta, chrono::Utc::now()).await.unwrap();
        let job_id = outcome.job().id;

        // 2. GET /api/v1/jobs
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs")
                    .header(header::AUTHORIZATION, "Bearer admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let page: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(page["total"], 1);
        assert_eq!(page["items"][0]["id"], job_id.to_string());

        // 3. GET /api/v1/jobs/{id}
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/jobs/{job_id}"))
                    .header(header::AUTHORIZATION, "Bearer admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let detail: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(detail["id"], job_id.to_string());
        assert_eq!(detail["state"], "queued");
        assert!(detail["attempts"].as_array().unwrap().is_empty());

        // 4. POST /api/v1/jobs/{id}/cancel
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/jobs/{job_id}/cancel"))
                    .header(header::AUTHORIZATION, "Bearer admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let cancelled: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(cancelled["state"], "cancelled");

        // 5. POST /api/v1/jobs/{id}/cancel again -> 409 Conflict
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/v1/jobs/{job_id}/cancel"))
                    .header(header::AUTHORIZATION, "Bearer admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::CONFLICT);

        // 6. Register a worker and check GET /api/v1/workers
        registry
            .update(webhook_async_core::job::WorkerInfo {
                id: "worker-1".into(),
                hostname: "node-a".into(),
                concurrency: 4,
                active_jobs: 1,
                completed_count: 5,
                failed_count: 0,
                last_heartbeat_at: chrono::Utc::now(),
                draining: false,
            })
            .await;

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/workers")
                    .header(header::AUTHORIZATION, "Bearer admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let val: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(val["workers"].as_array().unwrap().len(), 1);
        assert_eq!(val["workers"][0]["id"], "worker-1");

        // 7. GET /api/v1/storage/status
        let res = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/storage/status")
                    .header(header::AUTHORIZATION, "Bearer admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let val: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(val["backend"], "sqlite");
        assert_eq!(val["connected"], true);

        storage.close().await;
    }
}
