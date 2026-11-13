//! Server-rendered management UI using Askama, HTMX, and SSE.

use askama::Template;
use axum::{
    Router,
    extract::{Form, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{
        Html, IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;
use uuid::Uuid;
use webhook_async_config::{ResolvedHook, RuntimeConfig};
use webhook_async_core::{
    job::{ExecutionSpec, JobAttempt, JobRecord},
    state::JobState,
};
use webhook_async_queue::QueueCoordinator;
use webhook_async_storage::{BackendCapabilities, JobFilter, QueueStats};
use webhook_async_worker::WorkerRegistry;

pub const STATIC_STYLE_CSS: &str = include_str!("../static/style.css");
pub const STATIC_HTMX_JS: &str = include_str!("../static/htmx.min.js");

#[derive(Clone, Debug)]
pub struct WebSession {
    pub session_id: String,
    pub csrf_token: String,
    pub created_at: Instant,
    pub expires_at: Instant,
}

#[derive(Clone)]
pub struct WebState {
    pub runtime: Arc<RuntimeConfig>,
    pub hooks: Arc<std::sync::RwLock<HashMap<String, ResolvedHook>>>,
    pub coordinator: Option<QueueCoordinator>,
    pub worker_registry: Option<WorkerRegistry>,
    pub admin_token: Option<String>,
    pub sessions: Arc<RwLock<HashMap<String, WebSession>>>,
    pub path_prefix: String,
}

impl WebState {
    pub fn new(
        runtime: Arc<RuntimeConfig>,
        coordinator: Option<QueueCoordinator>,
        worker_registry: Option<WorkerRegistry>,
    ) -> Self {
        let admin_token = runtime.app.auth.resolve_admin_token().ok().flatten();
        let path_prefix = runtime.app.ui.path_prefix.trim_end_matches('/').to_string();
        let hooks_map = runtime
            .hooks
            .iter()
            .cloned()
            .map(|h| (h.definition.id.clone(), h))
            .collect();

        Self {
            runtime,
            hooks: Arc::new(std::sync::RwLock::new(hooks_map)),
            coordinator,
            worker_registry,
            admin_token,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            path_prefix,
        }
    }

    pub fn new_with_hooks(
        runtime: Arc<RuntimeConfig>,
        coordinator: Option<QueueCoordinator>,
        worker_registry: Option<WorkerRegistry>,
        hooks: Arc<std::sync::RwLock<HashMap<String, ResolvedHook>>>,
    ) -> Self {
        let admin_token = runtime.app.auth.resolve_admin_token().ok().flatten();
        let path_prefix = runtime.app.ui.path_prefix.trim_end_matches('/').to_string();

        Self {
            runtime,
            hooks,
            coordinator,
            worker_registry,
            admin_token,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            path_prefix,
        }
    }

    /// Atomically updates registered hooks table for dynamic reloads.
    pub fn reload_hooks(&self, new_hooks: Vec<ResolvedHook>) {
        let mut map = self.hooks.write().expect("lock not poisoned");
        *map = new_hooks
            .into_iter()
            .map(|h| (h.definition.id.clone(), h))
            .collect();
    }

    /// Returns a list of currently registered hooks.
    #[must_use]
    pub fn list_hooks(&self) -> Vec<ResolvedHook> {
        self.hooks
            .read()
            .expect("lock not poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn with_admin_token(mut self, token: Option<String>) -> Self {
        self.admin_token = token;
        self
    }

    async fn authenticate_session(&self, headers: &HeaderMap) -> Option<WebSession> {
        let cookie_header = headers.get(header::COOKIE)?.to_str().ok()?;
        let session_id = parse_cookie_value(cookie_header, "session_id")?;
        let now = Instant::now();
        let sessions = self.sessions.read().await;
        let session = sessions.get(&session_id)?;
        if session.expires_at > now {
            Some(session.clone())
        } else {
            None
        }
    }

    async fn create_session(&self) -> WebSession {
        let session_id = Uuid::new_v4().to_string();
        let csrf_token = Uuid::new_v4().to_string();
        let now = Instant::now();
        let session = WebSession {
            session_id: session_id.clone(),
            csrf_token,
            created_at: now,
            expires_at: now + Duration::from_secs(86400),
        };
        let mut sessions = self.sessions.write().await;
        sessions.insert(session_id, session.clone());
        session
    }

    async fn destroy_session(&self, session_id: &str) {
        let mut sessions = self.sessions.write().await;
        sessions.remove(session_id);
    }
}

fn parse_cookie_value(header: &str, name: &str) -> Option<String> {
    for part in header.split(';') {
        let trimmed = part.trim();
        if let Some((k, v)) = trimmed.split_once('=')
            && k.trim() == name
        {
            return Some(v.trim().to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Template View Models
// ---------------------------------------------------------------------------

#[derive(Template)]
#[template(path = "login.html")]
pub struct LoginTemplate<'a> {
    pub prefix: &'a str,
    pub error: &'a str,
}

#[derive(Template)]
#[template(path = "dashboard.html")]
pub struct DashboardTemplate<'a> {
    pub prefix: &'a str,
    pub csrf_token: &'a str,
    pub current_nav: &'a str,
    pub queued_count: u64,
    pub running_count: u64,
    pub retry_wait_count: u64,
    pub succeeded_count: u64,
    pub failed_count: u64,
    pub cancelled_count: u64,
    pub dead_letter_count: u64,
    pub pending_count: u64,
    pub workers: Vec<WorkerView>,
    pub recent_jobs: Vec<JobSummaryView>,
}

#[derive(Clone, Debug)]
pub struct WorkerView {
    pub worker_id: String,
    pub concurrency: u32,
    pub active_jobs: u32,
    pub completed_count: u64,
    pub failed_count: u64,
    pub draining: bool,
}

#[derive(Clone, Debug)]
pub struct JobSummaryView {
    pub id: String,
    pub id_prefix: String,
    pub hook_id: String,
    pub state_str: String,
    pub attempt: u32,
    pub max_attempts: u32,
    pub created_at_fmt: String,
}

#[derive(Template)]
#[template(path = "jobs.html")]
pub struct JobsTemplate<'a> {
    pub prefix: &'a str,
    pub csrf_token: &'a str,
    pub current_nav: &'a str,
    pub filter_state: &'a str,
    pub filter_hook_id: &'a str,
    pub total_count: u64,
    pub jobs: Vec<JobRowView>,
    pub limit: u32,
    pub offset: u64,
    pub has_prev: bool,
    pub prev_offset: u64,
    pub has_next: bool,
    pub next_offset: u64,
    pub current_range_start: u64,
    pub current_range_end: u64,
}

#[derive(Clone, Debug)]
pub struct JobRowView {
    pub id: String,
    pub id_prefix: String,
    pub hook_id: String,
    pub state_str: String,
    pub priority: i32,
    pub attempt: u32,
    pub max_attempts: u32,
    pub created_at_fmt: String,
    pub can_retry: bool,
    pub can_cancel: bool,
}

#[derive(Template)]
#[template(path = "job_detail.html")]
pub struct JobDetailTemplate<'a> {
    pub prefix: &'a str,
    pub csrf_token: &'a str,
    pub current_nav: &'a str,
    pub job: JobDetailView,
    pub spec: &'a ExecutionSpec,
    pub spec_args_str: String,
    pub spec_working_dir_str: String,
    pub env_keys_str: String,
    pub attempts: Vec<AttemptView>,
}

#[derive(Clone, Debug)]
pub struct JobDetailView {
    pub id: String,
    pub id_prefix: String,
    pub hook_id: String,
    pub state_str: String,
    pub priority: i32,
    pub attempt: u32,
    pub max_attempts: u32,
    pub worker_id_str: String,
    pub idempotency_key_str: String,
    pub created_at_fmt: String,
    pub duration_str: String,
    pub exit_code: Option<i32>,
    pub error_summary_str: String,
    pub can_retry: bool,
    pub can_cancel: bool,
}

#[derive(Clone, Debug)]
pub struct AttemptView {
    pub attempt: u32,
    pub worker_id: String,
    pub finished_at_fmt: String,
    pub duration_str: String,
    pub exit_code: Option<i32>,
    pub is_exit_zero: bool,
    pub error_class_str: Option<String>,
    pub error_summary_str: String,
    pub stdout_preview_str: String,
    pub stderr_preview_str: String,
}

#[derive(Template)]
#[template(path = "hooks.html")]
pub struct HooksTemplate<'a> {
    pub prefix: &'a str,
    pub csrf_token: &'a str,
    pub current_nav: &'a str,
    pub hooks: Vec<HookItemView>,
}

#[derive(Clone, Debug)]
pub struct HookItemView {
    pub id: String,
    pub is_async: bool,
    pub methods_str: String,
    pub command: String,
    pub working_dir: String,
    pub max_attempts: u32,
    pub include_output: bool,
}

#[derive(Template)]
#[template(path = "storage.html")]
pub struct StorageTemplate<'a> {
    pub prefix: &'a str,
    pub csrf_token: &'a str,
    pub current_nav: &'a str,
    pub backend_name: &'a str,
    pub is_healthy: bool,
    pub supports_priority: bool,
    pub supports_multi_host: bool,
    pub supports_idempotency: bool,
    pub pending_count: u64,
    pub counts: Vec<StateCountRow>,
}

#[derive(Clone, Debug)]
pub struct StateCountRow {
    pub state: String,
    pub count: u64,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn format_duration(start: DateTime<Utc>, end: Option<DateTime<Utc>>) -> String {
    let finished = end.unwrap_or_else(Utc::now);
    let dur = finished.signed_duration_since(start);
    let total_secs = dur.num_seconds().max(0);
    if total_secs < 60 {
        format!("{total_secs}s")
    } else if total_secs < 3600 {
        format!("{}m {}s", total_secs / 60, total_secs % 60)
    } else {
        format!("{}h {}m", total_secs / 3600, (total_secs % 3600) / 60)
    }
}

fn to_job_summary_view(job: &JobRecord) -> JobSummaryView {
    let id_str = job.id.to_string();
    let id_prefix = if id_str.len() > 8 {
        id_str[..8].to_string()
    } else {
        id_str.clone()
    };
    JobSummaryView {
        id: id_str,
        id_prefix,
        hook_id: job.hook_id.clone(),
        state_str: job.state.as_str().to_string(),
        attempt: job.attempt,
        max_attempts: job.max_attempts,
        created_at_fmt: job.created_at.format("%Y-%m-%d %H:%M:%S").to_string(),
    }
}

fn to_job_row_view(job: &JobRecord) -> JobRowView {
    let id_str = job.id.to_string();
    let id_prefix = if id_str.len() > 8 {
        id_str[..8].to_string()
    } else {
        id_str.clone()
    };
    JobRowView {
        id: id_str,
        id_prefix,
        hook_id: job.hook_id.clone(),
        state_str: job.state.as_str().to_string(),
        priority: job.priority,
        attempt: job.attempt,
        max_attempts: job.max_attempts,
        created_at_fmt: job.created_at.format("%Y-%m-%d %H:%M:%S").to_string(),
        can_retry: matches!(job.state, JobState::Failed | JobState::DeadLetter),
        can_cancel: matches!(job.state, JobState::Queued | JobState::Running),
    }
}

fn to_job_detail_view(job: &JobRecord) -> JobDetailView {
    let id_str = job.id.to_string();
    let id_prefix = if id_str.len() > 8 {
        id_str[..8].to_string()
    } else {
        id_str.clone()
    };
    JobDetailView {
        id: id_str,
        id_prefix,
        hook_id: job.hook_id.clone(),
        state_str: job.state.as_str().to_string(),
        priority: job.priority,
        attempt: job.attempt,
        max_attempts: job.max_attempts,
        worker_id_str: job.worker_id.clone().unwrap_or_else(|| "-".to_string()),
        idempotency_key_str: job.idempotency_key.clone().unwrap_or_default(),
        created_at_fmt: job.created_at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        duration_str: format_duration(job.created_at, job.completed_at),
        exit_code: job.exit_code,
        error_summary_str: job.error_summary.clone().unwrap_or_default(),
        can_retry: matches!(job.state, JobState::Failed | JobState::DeadLetter),
        can_cancel: matches!(job.state, JobState::Queued | JobState::Running),
    }
}

fn to_attempt_view(att: &JobAttempt) -> AttemptView {
    AttemptView {
        attempt: att.attempt,
        worker_id: att.worker_id.clone(),
        finished_at_fmt: att
            .finished_at
            .map(|f| f.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "-".to_string()),
        duration_str: format_duration(att.started_at, att.finished_at),
        exit_code: att.exit_code,
        is_exit_zero: att.exit_code == Some(0),
        error_class_str: att.error_class.map(|c| c.as_str().to_string()),
        error_summary_str: att.error_summary.clone().unwrap_or_default(),
        stdout_preview_str: att.stdout_preview.clone().unwrap_or_default(),
        stderr_preview_str: att.stderr_preview.clone().unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn handle_style() -> Response {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        STATIC_STYLE_CSS,
    )
        .into_response()
}

async fn handle_htmx() -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        STATIC_HTMX_JS,
    )
        .into_response()
}

async fn handle_login_page(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if state.authenticate_session(&headers).await.is_some() {
        return redirect_to(&format!("{}/", state.path_prefix));
    }
    let html = LoginTemplate {
        prefix: &state.path_prefix,
        error: "",
    }
    .render()
    .unwrap_or_default();
    Html(html).into_response()
}

#[derive(Deserialize)]
pub struct LoginForm {
    pub token: String,
}

async fn handle_login_submit(
    State(state): State<WebState>,
    Form(form): Form<LoginForm>,
) -> Response {
    let Some(admin_token) = &state.admin_token else {
        let html = LoginTemplate {
            prefix: &state.path_prefix,
            error: "Admin token is not configured on server.",
        }
        .render()
        .unwrap_or_default();
        return (StatusCode::SERVICE_UNAVAILABLE, Html(html)).into_response();
    };

    let token_bytes = form.token.trim().as_bytes();
    let expected_bytes = admin_token.trim().as_bytes();

    if token_bytes.ct_eq(expected_bytes).into() {
        let session = state.create_session().await;
        let cookie = format!(
            "session_id={}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400",
            session.session_id
        );
        (
            StatusCode::SEE_OTHER,
            [
                (header::LOCATION, format!("{}/", state.path_prefix)),
                (header::SET_COOKIE, cookie),
            ],
            (),
        )
            .into_response()
    } else {
        let html = LoginTemplate {
            prefix: &state.path_prefix,
            error: "Invalid admin token.",
        }
        .render()
        .unwrap_or_default();
        (StatusCode::UNAUTHORIZED, Html(html)).into_response()
    }
}

#[derive(Deserialize)]
pub struct CsrfForm {
    pub csrf_token: Option<String>,
}

async fn handle_logout(
    State(state): State<WebState>,
    headers: HeaderMap,
    Form(form): Form<CsrfForm>,
) -> Response {
    if let Some(session) = state.authenticate_session(&headers).await
        && let Some(csrf) = form.csrf_token
        && csrf.as_bytes().ct_eq(session.csrf_token.as_bytes()).into()
    {
        state.destroy_session(&session.session_id).await;
    }
    let expired_cookie = "session_id=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0";
    (
        StatusCode::SEE_OTHER,
        [
            (header::LOCATION, format!("{}/login", state.path_prefix)),
            (header::SET_COOKIE, expired_cookie.to_string()),
        ],
        (),
    )
        .into_response()
}

fn redirect_to(location: &str) -> Response {
    (
        StatusCode::SEE_OTHER,
        [(header::LOCATION, location.to_string())],
        (),
    )
        .into_response()
}

fn verify_csrf(session: &WebSession, headers: &HeaderMap) -> bool {
    if let Some(header_val) = headers.get("X-CSRF-Token")
        && let Ok(token_str) = header_val.to_str()
    {
        return token_str
            .as_bytes()
            .ct_eq(session.csrf_token.as_bytes())
            .into();
    }
    false
}

async fn handle_dashboard(State(state): State<WebState>, headers: HeaderMap) -> Response {
    let Some(session) = state.authenticate_session(&headers).await else {
        return redirect_to(&format!("{}/login", state.path_prefix));
    };

    let (stats, recent_jobs) = if let Some(coord) = &state.coordinator {
        let stats = coord
            .storage()
            .stats()
            .await
            .unwrap_or_else(|_| QueueStats {
                counts_by_state: Default::default(),
                pending_count: 0,
            });
        let filter = JobFilter {
            state: None,
            hook_id: None,
            limit: 10,
            offset: 0,
        };
        let items = coord
            .storage()
            .list_jobs(filter)
            .await
            .map(|p| p.items)
            .unwrap_or_default();
        (stats, items)
    } else {
        (
            QueueStats {
                counts_by_state: Default::default(),
                pending_count: 0,
            },
            Vec::new(),
        )
    };

    let workers = if let Some(reg) = &state.worker_registry {
        reg.list()
            .await
            .into_iter()
            .map(|w| WorkerView {
                worker_id: w.id,
                concurrency: w.concurrency,
                active_jobs: w.active_jobs,
                completed_count: w.completed_count,
                failed_count: w.failed_count,
                draining: w.draining,
            })
            .collect()
    } else {
        Vec::new()
    };

    let get_count = |s: &str| stats.counts_by_state.get(s).copied().unwrap_or(0);

    let template = DashboardTemplate {
        prefix: &state.path_prefix,
        csrf_token: &session.csrf_token,
        current_nav: "dashboard",
        queued_count: get_count("queued"),
        running_count: get_count("running"),
        retry_wait_count: get_count("retry_wait"),
        succeeded_count: get_count("succeeded"),
        failed_count: get_count("failed"),
        cancelled_count: get_count("cancelled"),
        dead_letter_count: get_count("dead_letter"),
        pending_count: stats.pending_count,
        workers,
        recent_jobs: recent_jobs.iter().map(to_job_summary_view).collect(),
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Template error: {err}"),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct JobsQuery {
    pub state: Option<String>,
    pub hook_id: Option<String>,
    pub limit: Option<u32>,
    pub offset: Option<u64>,
}

async fn handle_jobs(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<JobsQuery>,
) -> Response {
    let Some(session) = state.authenticate_session(&headers).await else {
        return redirect_to(&format!("{}/login", state.path_prefix));
    };

    let limit = query.limit.unwrap_or(20).clamp(1, 100);
    let offset = query.offset.unwrap_or(0);
    let filter_state_raw = query.state.clone().unwrap_or_default();
    let filter_hook_raw = query.hook_id.clone().unwrap_or_default();

    let state_filter = if filter_state_raw.is_empty() {
        None
    } else {
        filter_state_raw.parse::<JobState>().ok()
    };
    let hook_filter = if filter_hook_raw.is_empty() {
        None
    } else {
        Some(filter_hook_raw.clone())
    };

    let (items, total) = if let Some(coord) = &state.coordinator {
        let filter = JobFilter {
            state: state_filter,
            hook_id: hook_filter,
            limit,
            offset,
        };
        match coord.storage().list_jobs(filter).await {
            Ok(p) => (p.items, p.total),
            Err(_) => (Vec::new(), 0),
        }
    } else {
        (Vec::new(), 0)
    };

    let has_prev = offset > 0;
    let prev_offset = offset.saturating_sub(limit as u64);
    let has_next = offset + (items.len() as u64) < total;
    let next_offset = offset + (limit as u64);

    let range_start = if total == 0 { 0 } else { offset + 1 };
    let range_end = offset + (items.len() as u64);

    let template = JobsTemplate {
        prefix: &state.path_prefix,
        csrf_token: &session.csrf_token,
        current_nav: "jobs",
        filter_state: &filter_state_raw,
        filter_hook_id: &filter_hook_raw,
        total_count: total,
        jobs: items.iter().map(to_job_row_view).collect(),
        limit,
        offset,
        has_prev,
        prev_offset,
        has_next,
        next_offset,
        current_range_start: range_start,
        current_range_end: range_end,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Template error: {err}"),
        )
            .into_response(),
    }
}

async fn handle_job_detail(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some(session) = state.authenticate_session(&headers).await else {
        return redirect_to(&format!("{}/login", state.path_prefix));
    };

    let Ok(job_id) = id.parse() else {
        return (StatusCode::BAD_REQUEST, "Invalid job ID").into_response();
    };

    let Some(coord) = &state.coordinator else {
        return (StatusCode::NOT_FOUND, "Queue coordinator not available").into_response();
    };

    let (job, attempts) = match coord.storage().get_job(job_id).await {
        Ok(Some(j)) => {
            let atts = coord
                .storage()
                .list_attempts(job_id)
                .await
                .unwrap_or_default();
            (j, atts)
        }
        Ok(None) => return (StatusCode::NOT_FOUND, "Job not found").into_response(),
        Err(err) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Storage error: {err}"),
            )
                .into_response();
        }
    };

    let spec_args_str = job.execution_spec.args.join(" ");
    let spec_working_dir_str = job
        .execution_spec
        .working_dir
        .as_deref()
        .unwrap_or("(default)")
        .to_string();
    let env_keys_str = if job.execution_spec.env.is_empty() {
        "(none)".to_string()
    } else {
        let keys: Vec<_> = job.execution_spec.env.keys().cloned().collect();
        keys.join(", ")
    };

    let template = JobDetailTemplate {
        prefix: &state.path_prefix,
        csrf_token: &session.csrf_token,
        current_nav: "jobs",
        job: to_job_detail_view(&job),
        spec: &job.execution_spec,
        spec_args_str,
        spec_working_dir_str,
        env_keys_str,
        attempts: attempts.iter().map(to_attempt_view).collect(),
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Template error: {err}"),
        )
            .into_response(),
    }
}

async fn handle_job_retry(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some(session) = state.authenticate_session(&headers).await else {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    };

    if !verify_csrf(&session, &headers) {
        return (StatusCode::FORBIDDEN, "Invalid CSRF token").into_response();
    }

    let Ok(job_id) = id.parse() else {
        return (StatusCode::BAD_REQUEST, "Invalid job ID").into_response();
    };

    let Some(coord) = &state.coordinator else {
        return (StatusCode::SERVICE_UNAVAILABLE, "Queue not available").into_response();
    };

    match coord.retry_job(job_id, Utc::now()).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(err) => (StatusCode::CONFLICT, format!("Cannot retry job: {err}")).into_response(),
    }
}

async fn handle_job_cancel(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some(session) = state.authenticate_session(&headers).await else {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    };

    if !verify_csrf(&session, &headers) {
        return (StatusCode::FORBIDDEN, "Invalid CSRF token").into_response();
    }

    let Ok(job_id) = id.parse() else {
        return (StatusCode::BAD_REQUEST, "Invalid job ID").into_response();
    };

    let Some(coord) = &state.coordinator else {
        return (StatusCode::SERVICE_UNAVAILABLE, "Queue not available").into_response();
    };

    match coord.cancel_job(job_id, Utc::now()).await {
        Ok(_) => StatusCode::OK.into_response(),
        Err(err) => (StatusCode::CONFLICT, format!("Cannot cancel job: {err}")).into_response(),
    }
}

async fn handle_hooks(State(state): State<WebState>, headers: HeaderMap) -> Response {
    let Some(session) = state.authenticate_session(&headers).await else {
        return redirect_to(&format!("{}/login", state.path_prefix));
    };

    let hooks = state
        .list_hooks()
        .iter()
        .map(|h| HookItemView {
            id: h.definition.id.clone(),
            is_async: h.mode.is_async(),
            methods_str: if h.definition.http_methods.is_empty() {
                "ANY".to_string()
            } else {
                h.definition.http_methods.join(", ")
            },
            command: h.definition.execute_command.clone(),
            working_dir: h
                .definition
                .command_working_directory
                .as_deref()
                .unwrap_or(".")
                .to_string(),
            max_attempts: h
                .definition
                .max_attempts
                .unwrap_or(state.runtime.app.queue.default_max_attempts),
            include_output: h.definition.include_command_output_in_response,
        })
        .collect();

    let template = HooksTemplate {
        prefix: &state.path_prefix,
        csrf_token: &session.csrf_token,
        current_nav: "hooks",
        hooks,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Template error: {err}"),
        )
            .into_response(),
    }
}

async fn handle_storage(State(state): State<WebState>, headers: HeaderMap) -> Response {
    let Some(session) = state.authenticate_session(&headers).await else {
        return redirect_to(&format!("{}/login", state.path_prefix));
    };

    let (backend_name, capabilities, is_healthy, stats) = if let Some(coord) = &state.coordinator {
        let caps = coord.storage().capabilities();
        let ping_ok = coord.storage().ping().await.is_ok();
        let stats = coord
            .storage()
            .stats()
            .await
            .unwrap_or_else(|_| QueueStats {
                counts_by_state: Default::default(),
                pending_count: 0,
            });
        (caps.backend, caps, ping_ok, stats)
    } else {
        (
            "none (synchronous mode)",
            BackendCapabilities {
                backend: "none",
                supports_priority: false,
                supports_multi_host: false,
                supports_idempotency: false,
            },
            true,
            QueueStats {
                counts_by_state: Default::default(),
                pending_count: 0,
            },
        )
    };

    let mut counts: Vec<StateCountRow> = stats
        .counts_by_state
        .into_iter()
        .map(|(s, c)| StateCountRow { state: s, count: c })
        .collect();
    counts.sort_by(|a, b| a.state.cmp(&b.state));

    let template = StorageTemplate {
        prefix: &state.path_prefix,
        csrf_token: &session.csrf_token,
        current_nav: "storage",
        backend_name,
        is_healthy,
        supports_priority: capabilities.supports_priority,
        supports_multi_host: capabilities.supports_multi_host,
        supports_idempotency: capabilities.supports_idempotency,
        pending_count: stats.pending_count,
        counts,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Template error: {err}"),
        )
            .into_response(),
    }
}

async fn handle_events(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if state.authenticate_session(&headers).await.is_none() {
        return (StatusCode::UNAUTHORIZED, "Unauthorized").into_response();
    }

    let coordinator = state.coordinator.clone();
    let stream = futures_util::stream::unfold((coordinator, 0u64), |(coord, count)| async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let data = if let Some(coord) = &coord {
            if let Ok(stats) = coord.storage().stats().await {
                serde_json::to_string(&stats).unwrap_or_default()
            } else {
                r#"{"pending_count":0}"#.to_string()
            }
        } else {
            r#"{"pending_count":0}"#.to_string()
        };
        Some((
            Ok::<_, Infallible>(Event::default().data(data)),
            (coord, count + 1),
        ))
    });

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

// ---------------------------------------------------------------------------
// Router Builder
// ---------------------------------------------------------------------------

pub fn build_web_router(state: WebState) -> Router {
    let routes = Router::new()
        .route("/", get(handle_dashboard))
        .route("/login", get(handle_login_page).post(handle_login_submit))
        .route("/logout", post(handle_logout))
        .route("/jobs", get(handle_jobs))
        .route("/jobs/{id}", get(handle_job_detail))
        .route("/jobs/{id}/retry", post(handle_job_retry))
        .route("/jobs/{id}/cancel", post(handle_job_cancel))
        .route("/hooks", get(handle_hooks))
        .route("/storage", get(handle_storage))
        .route("/events", get(handle_events))
        .route("/static/style.css", get(handle_style))
        .route("/static/htmx.min.js", get(handle_htmx))
        .with_state(state.clone());

    if state.path_prefix.is_empty() || state.path_prefix == "/" {
        routes
    } else {
        Router::new()
            .merge(routes.clone())
            .nest(&state.path_prefix, routes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    use webhook_async_config::AppConfig;
    use webhook_async_storage_sqlite::SqliteStorage;

    async fn test_state(admin_token: Option<&str>) -> (WebState, Option<Arc<SqliteStorage>>) {
        let app = AppConfig::default();
        let runtime = Arc::new(RuntimeConfig {
            app,
            hooks: Vec::new(),
            global_mode: webhook_async_core::mode::GlobalExecutionMode::Sync,
            config_version: "test".to_string(),
        });
        let storage = Arc::new(
            SqliteStorage::connect(&webhook_async_config::SqliteStorageConfig {
                database_url: "sqlite::memory:".to_string(),
                wal_mode: false,
                busy_timeout_ms: 5000,
            })
            .await
            .unwrap(),
        );
        let coordinator = Some(QueueCoordinator::new(
            storage.clone(),
            webhook_async_config::QueueConfig::default(),
        ));
        let state = WebState::new(runtime, coordinator, None)
            .with_admin_token(admin_token.map(|s| s.to_string()));
        (state, Some(storage))
    }

    #[tokio::test]
    async fn redirects_to_login_when_no_session() {
        let (state, storage) = test_state(Some("secret123")).await;
        let router = build_web_router(state);
        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::SEE_OTHER);
        assert_eq!(res.headers().get(header::LOCATION).unwrap(), "/ui/login");
        if let Some(s) = storage {
            s.close().await;
        }
    }

    #[tokio::test]
    async fn login_flow_and_session_cookie() {
        let (state, storage) = test_state(Some("secret123")).await;
        let router = build_web_router(state.clone());

        // Failed login
        let req = Request::builder()
            .method("POST")
            .uri("/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("token=wrong"))
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

        // Successful login
        let router = build_web_router(state.clone());
        let req = Request::builder()
            .method("POST")
            .uri("/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("token=secret123"))
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::SEE_OTHER);
        let set_cookie = res
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(set_cookie.contains("session_id="));
        assert!(set_cookie.contains("HttpOnly"));

        // Use session cookie to access dashboard
        let session_id = parse_cookie_value(set_cookie, "session_id").unwrap();
        let router = build_web_router(state);
        let req = Request::builder()
            .uri("/")
            .header(header::COOKIE, format!("session_id={session_id}"))
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("Operational Overview"));

        if let Some(s) = storage {
            s.close().await;
        }
    }

    #[tokio::test]
    async fn static_assets_served() {
        let (state, storage) = test_state(None).await;
        let router = build_web_router(state);

        let req = Request::builder()
            .uri("/static/style.css")
            .body(Body::empty())
            .unwrap();
        let res = router.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert_eq!(
            res.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/css; charset=utf-8"
        );

        let req = Request::builder()
            .uri("/static/htmx.min.js")
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        if let Some(s) = storage {
            s.close().await;
        }
    }

    #[tokio::test]
    async fn csrf_protection_on_mutations() {
        let (state, storage) = test_state(Some("secret123")).await;
        let session = state.create_session().await;

        // Mutation without CSRF token fails with 403
        let router = build_web_router(state.clone());
        let fake_id = Uuid::new_v4();
        let req = Request::builder()
            .method("POST")
            .uri(format!("/jobs/{fake_id}/retry"))
            .header(header::COOKIE, format!("session_id={}", session.session_id))
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        // Mutation with wrong CSRF token fails with 403
        let router = build_web_router(state.clone());
        let req = Request::builder()
            .method("POST")
            .uri(format!("/jobs/{fake_id}/retry"))
            .header(header::COOKIE, format!("session_id={}", session.session_id))
            .header("X-CSRF-Token", "wrong-csrf-token")
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);

        // Mutation with correct CSRF token reaches handler (job not found -> 409 conflict or error)
        let router = build_web_router(state);
        let req = Request::builder()
            .method("POST")
            .uri(format!("/jobs/{fake_id}/retry"))
            .header(header::COOKIE, format!("session_id={}", session.session_id))
            .header("X-CSRF-Token", &session.csrf_token)
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_ne!(res.status(), StatusCode::FORBIDDEN);
        assert_ne!(res.status(), StatusCode::UNAUTHORIZED);

        if let Some(s) = storage {
            s.close().await;
        }
    }

    #[tokio::test]
    async fn unconfigured_admin_token_returns_503_on_login() {
        let (state, storage) = test_state(None).await;
        let router = build_web_router(state);
        let req = Request::builder()
            .method("POST")
            .uri("/login")
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from("token=anything"))
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        if let Some(s) = storage {
            s.close().await;
        }
    }

    #[tokio::test]
    async fn logout_clears_session_and_cookie() {
        let (state, storage) = test_state(Some("secret123")).await;
        let session = state.create_session().await;
        let router = build_web_router(state.clone());

        let req = Request::builder()
            .method("POST")
            .uri("/logout")
            .header(header::COOKIE, format!("session_id={}", session.session_id))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Body::from(format!("csrf_token={}", session.csrf_token)))
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::SEE_OTHER);
        let set_cookie = res
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(set_cookie.contains("Max-Age=0"));

        // Session was deleted
        assert!(
            state
                .authenticate_session(&HeaderMap::new())
                .await
                .is_none()
        );

        if let Some(s) = storage {
            s.close().await;
        }
    }

    #[tokio::test]
    async fn xss_escaping_in_hooks_view() {
        let app = AppConfig::default();
        let xss_hook = webhook_async_config::ResolvedHook {
            definition: webhook_async_core::hook::HookDefinition {
                id: "<script>alert('xss')</script>".to_string(),
                execute_command: "/bin/echo <unsafe>".to_string(),
                command_working_directory: None,
                response_message: None,
                response_headers: Vec::new(),
                include_command_output_in_response: false,
                include_command_output_in_response_on_error: false,
                pass_environment_to_command: Vec::new(),
                pass_arguments_to_command: Vec::new(),
                pass_file_to_command: Vec::new(),
                parse_parameters_as_json: Vec::new(),
                trigger_rule: None,
                trigger_rule_mismatch_http_response_code: None,
                trigger_signature_soft_failures: false,
                incoming_payload_content_type: None,
                success_http_response_code: None,
                http_methods: Vec::new(),
                max_attempts: None,
                timeout_seconds: None,
                async_mode: None,
            },
            mode: webhook_async_core::mode::EffectiveExecutionMode::Sync,
        };
        let runtime = Arc::new(RuntimeConfig {
            app,
            hooks: vec![xss_hook],
            global_mode: webhook_async_core::mode::GlobalExecutionMode::Sync,
            config_version: "test".to_string(),
        });
        let state = WebState::new(runtime, None, None).with_admin_token(Some("token".to_string()));
        let session = state.create_session().await;
        let router = build_web_router(state);

        let req = Request::builder()
            .uri("/hooks")
            .header(header::COOKIE, format!("session_id={}", session.session_id))
            .body(Body::empty())
            .unwrap();
        let res = router.oneshot(req).await.unwrap();
        let status = res.status();
        let body = res.into_body().collect().await.unwrap().to_bytes();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert_eq!(status, StatusCode::OK, "Body was: {html}");
        // HTML is properly escaped
        // HTML is properly escaped (Askama escapes '<' to '&#60;' and '>' to '&#62;')
        assert!(html.contains("&#60;script&#62;"));
        assert!(!html.contains("<script>alert('xss')</script>"));
    }
}
