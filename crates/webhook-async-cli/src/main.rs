use std::collections::HashMap;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;

use chrono::Utc;
use clap::{Args, Parser, Subcommand};
use serde_json::json;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use webhook_async_api::{ApiState, ListenerConfig, serve_full};
use webhook_async_config::{AppConfig, ResolvedHook, RuntimeConfig};
use webhook_async_core::job::JobId;
use webhook_async_core::state::JobState;
use webhook_async_core::{GlobalExecutionMode, ResponseHeader, validate_cli_mode_flags};
use webhook_async_queue::QueueCoordinator;
use webhook_async_storage::{JobFilter, QueueStorage};
use webhook_async_web::WebState;
use webhook_async_worker::{WorkerPool, WorkerRegistry};

const UPSTREAM_SINGLE_DASH_FLAGS: &[&str] = &[
    "version",
    "ip",
    "port",
    "urlprefix",
    "hooks",
    "header",
    "http-methods",
    "x-request-id",
    "x-request-id-limit",
    "max-multipart-mem",
    "verbose",
    "debug",
    "hotreload",
    "template",
    "secure",
    "cert",
    "key",
    "tls-min-version",
    "cipher-suites",
    "list-cipher-suites",
    "socket",
    "pidfile",
    "logfile",
    "nopanic",
    "setuid",
    "setgid",
];

#[derive(Debug, Parser)]
#[command(
    name = "webhook-async",
    version,
    about = "Standalone Rust webhook receiver and asynchronous job engine"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the webhook HTTP receiver.
    Server(Box<ServerArgs>),
    /// Configuration management commands.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Print version information.
    Version,
    /// Run background queue workers (Phases 3-5).
    Worker(Box<WorkerArgs>),
    /// Run combined receiver + worker (Phases 3-5).
    Run(Box<ServerArgs>),
    /// Manage asynchronous jobs.
    Jobs {
        #[command(subcommand)]
        command: JobsCommand,
    },
    /// Inspect storage backend status.
    Storage {
        #[command(subcommand)]
        command: StorageCommand,
    },
}

#[derive(Debug, Subcommand)]
enum JobsCommand {
    /// List jobs with optional filtering.
    List(JobsListArgs),
    /// Get details and attempts of a job by ID.
    Get(JobIdArgs),
    /// Retry a failed or dead-letter job.
    Retry(JobIdArgs),
    /// Cancel a queued or running job.
    Cancel(JobIdArgs),
}

#[derive(Debug, Args, Clone, Default)]
struct JobsListArgs {
    /// Path to TOML application configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Filter by job state (queued, running, retry_wait, succeeded, failed, cancel_requested, cancelled, dead_letter).
    #[arg(long)]
    state: Option<String>,
    /// Filter by hook ID.
    #[arg(long)]
    hook: Option<String>,
    /// Page limit (1..=500, default 50).
    #[arg(long, default_value = "50")]
    limit: u32,
    /// Page offset (default 0).
    #[arg(long, default_value = "0")]
    offset: u64,
    /// Output raw JSON instead of formatted text.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args, Clone, Default)]
struct JobIdArgs {
    /// Path to TOML application configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Job identifier (UUID).
    id: String,
    /// Output raw JSON instead of formatted text.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum StorageCommand {
    /// Check storage connectivity, capabilities, and queue counts.
    Status(StorageStatusArgs),
}

#[derive(Debug, Args, Clone, Default)]
struct StorageStatusArgs {
    /// Path to TOML application configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Output raw JSON instead of formatted text.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum ConfigCommand {
    /// Validate TOML server configuration and hook definition files.
    Validate(ValidateArgs),
}

#[derive(Debug, Args, Clone, Default)]
struct WorkerArgs {
    /// Path to TOML application configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Unique worker identifier (defaults to hostname-pid-uuid).
    #[arg(long)]
    worker_id: Option<String>,
    /// Enable verbose info logging.
    #[arg(long, short = 'v')]
    verbose: bool,
    /// Enable debug logging.
    #[arg(long)]
    debug: bool,
}

#[derive(Debug, Args, Clone, Default)]
struct ValidateArgs {
    /// Path to TOML application configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Hook definition file (JSON or YAML). Repeatable; defaults to hooks.json when omitted.
    #[arg(long = "hooks")]
    hooks: Vec<PathBuf>,
    /// Parse hooks files as templates.
    #[arg(long)]
    template: bool,
    /// Force global synchronous execution mode.
    #[arg(long)]
    sync: bool,
    /// Force global asynchronous execution mode (requires configured storage).
    #[arg(long = "async")]
    async_mode: bool,
}

#[derive(Debug, Args, Clone, Default)]
struct ServerArgs {
    /// Path to TOML application configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Force global synchronous execution mode.
    #[arg(long)]
    sync: bool,
    /// Force global asynchronous execution mode (requires configured storage).
    #[arg(long = "async")]
    async_mode: bool,
    /// IP address to bind (overrides [server].host).
    #[arg(long)]
    ip: Option<String>,
    /// Port to bind (overrides [server].port).
    #[arg(long)]
    port: Option<u16>,
    /// URL prefix for webhook routes (overrides [server].url_prefix).
    #[arg(long)]
    urlprefix: Option<String>,
    /// Hook definition file (JSON or YAML). Repeatable; defaults to hooks.json when omitted.
    #[arg(long = "hooks")]
    hooks: Vec<PathBuf>,
    /// Global response header in NAME=VALUE format. Repeatable.
    #[arg(long = "header")]
    headers: Vec<String>,
    /// Default allowed HTTP methods (comma-separated).
    #[arg(long = "http-methods")]
    http_methods: Option<String>,
    /// Use incoming X-Request-Id header as request correlation ID.
    #[arg(long = "x-request-id")]
    x_request_id: bool,
    /// Truncate incoming X-Request-Id values to this byte limit (0 = unlimited).
    #[arg(long = "x-request-id-limit")]
    x_request_id_limit: Option<usize>,
    /// Maximum multipart form memory budget in bytes.
    #[arg(long = "max-multipart-mem")]
    max_multipart_mem: Option<usize>,
    /// Enable verbose info logging.
    #[arg(long, short = 'v')]
    verbose: bool,
    /// Enable debug logging.
    #[arg(long)]
    debug: bool,

    /// Watch hook files for changes and reload automatically.
    #[arg(long)]
    hotreload: bool,
    /// Parse hooks files as templates.
    #[arg(long)]
    template: bool,
    /// Serve using TLS.
    #[arg(long)]
    secure: bool,
    /// Path to TLS certificate PEM file.
    #[arg(long)]
    cert: Option<PathBuf>,
    /// Path to TLS private key PEM file.
    #[arg(long)]
    key: Option<PathBuf>,
    /// Minimum TLS version (1.2 or 1.3).
    #[arg(long = "tls-min-version")]
    tls_min_version: Option<String>,
    /// Custom TLS cipher suites (rejected if specified; rustls uses safe defaults).
    #[arg(long = "cipher-suites")]
    cipher_suites: Option<String>,
    /// List available TLS cipher suites and exit.
    #[arg(long = "list-cipher-suites")]
    list_cipher_suites: bool,
    /// Path to Unix domain socket to bind instead of TCP.
    #[arg(long)]
    socket: Option<PathBuf>,
    /// Write PID to specified file and delete upon exit.
    #[arg(long)]
    pidfile: Option<PathBuf>,
    /// Append logs to specified file instead of stderr.
    #[arg(long)]
    logfile: Option<PathBuf>,
    /// Catch panics and print error without crashing process.
    #[arg(long)]
    nopanic: bool,
    /// Drop privileges to specified numeric UID after socket bind.
    #[arg(long)]
    setuid: Option<u32>,
    /// Drop privileges to specified numeric GID after socket bind.
    #[arg(long)]
    setgid: Option<u32>,
}

fn normalize_legacy_args<I, T>(args: I) -> Vec<OsString>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    args.into_iter()
        .enumerate()
        .map(|(idx, raw)| {
            let os = raw.into();
            if idx == 0 {
                return os;
            }
            let Some(s) = os.to_str() else {
                return os;
            };
            if s.starts_with('-') && !s.starts_with("--") {
                let rest = &s[1..];
                let name = rest.split_once('=').map_or(rest, |(k, _)| k);
                if UPSTREAM_SINGLE_DASH_FLAGS.contains(&name) {
                    return OsString::from(format!("-{s}"));
                }
            }
            os
        })
        .collect()
}

fn init_tracing(verbose: bool, debug: bool, logfile: Option<&Path>) {
    let default_level = if debug {
        "debug"
    } else if verbose {
        "info"
    } else {
        "warn"
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));
    if let Some(path) = logfile
        && let Ok(file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
    {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(file)
            .with_ansi(false)
            .with_target(false)
            .try_init();
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn parse_header_flag(raw: &str) -> Result<ResponseHeader, String> {
    let Some((name, value)) = raw.split_once('=') else {
        return Err(format!(
            "invalid --header '{raw}': expected NAME=VALUE format"
        ));
    };
    let name = name.trim();
    if name.is_empty() {
        return Err(format!("invalid --header '{raw}': header name is empty"));
    }
    Ok(ResponseHeader {
        name: name.to_string(),
        value: value.to_string(),
    })
}

fn validate_server_flags(args: &ServerArgs) -> Result<(), String> {
    if let Some(min_ver) = &args.tls_min_version {
        match min_ver.as_str() {
            "1.2" | "1.3" => {}
            other => {
                return Err(format!(
                    "unsupported --tls-min-version '{other}': only 1.2 and 1.3 are supported"
                ));
            }
        }
    }
    if let Some(suites) = &args.cipher_suites {
        return Err(format!(
            "custom --cipher-suites is not supported by rustls: '{suites}'"
        ));
    }
    if args.setuid.is_some() != args.setgid.is_some() {
        return Err("flags --setuid and --setgid must be used together".to_string());
    }
    if args.socket.is_some() && args.setuid.is_some() {
        return Err("flag --setuid/--setgid is incompatible with --socket".to_string());
    }
    Ok(())
}

struct PidFileGuard(Option<PathBuf>);

impl PidFileGuard {
    fn new(path: Option<PathBuf>) -> Result<Self, String> {
        if let Some(ref p) = path {
            let pid = std::process::id();
            std::fs::write(p, format!("{pid}\n"))
                .map_err(|err| format!("failed to write pidfile '{}': {err}", p.display()))?;
        }
        Ok(Self(path))
    }
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        if let Some(ref p) = self.0 {
            let _ = std::fs::remove_file(p);
        }
    }
}

#[cfg(test)]
fn format_bind_addr(host: &str, port: u16) -> Result<SocketAddr, String> {
    let host = host.trim();
    let candidate = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    candidate
        .parse::<SocketAddr>()
        .map_err(|err| format!("invalid bind address '{candidate}': {err}"))
}

fn load_and_resolve_runtime(
    config_path: Option<&PathBuf>,
    cli_hooks: &[PathBuf],
    sync_flag: bool,
    async_flag: bool,
    template: bool,
    apply_server_overrides: Option<&ServerArgs>,
) -> Result<RuntimeConfig, String> {
    let global_mode =
        validate_cli_mode_flags(sync_flag, async_flag).map_err(|err| err.to_string())?;

    let mut app_cfg = match config_path {
        Some(path) => AppConfig::from_toml_file(path).map_err(|err| err.to_string())?,
        None => AppConfig::default(),
    };

    if let Some(s) = apply_server_overrides {
        if let Some(ip) = &s.ip {
            app_cfg.server.host.clone_from(ip);
        }
        if let Some(port) = s.port {
            app_cfg.server.port = port;
        }
        if let Some(prefix) = &s.urlprefix {
            app_cfg.server.url_prefix.clone_from(prefix);
        }
        if !s.headers.is_empty() {
            for raw in &s.headers {
                app_cfg
                    .server
                    .response_headers
                    .push(parse_header_flag(raw)?);
            }
        }
        if let Some(methods) = &s.http_methods {
            app_cfg.server.default_http_methods = methods
                .split(',')
                .map(str::trim)
                .filter(|m| !m.is_empty())
                .map(str::to_owned)
                .collect();
        }
        if s.x_request_id {
            app_cfg.server.use_x_request_id = true;
        }
        if let Some(limit) = s.x_request_id_limit {
            app_cfg.server.x_request_id_limit = limit;
        }
        if let Some(mem) = s.max_multipart_mem {
            app_cfg.server.max_multipart_mem_bytes = mem;
        }
    }

    let fallback_hooks;
    let effective_hooks = if cli_hooks.is_empty() && app_cfg.server.hook_files.is_empty() {
        fallback_hooks = vec![PathBuf::from("hooks.json")];
        &fallback_hooks[..]
    } else {
        cli_hooks
    };

    let hooks = if template {
        app_cfg
            .load_hooks_templated(effective_hooks)
            .map_err(|err| err.to_string())?
    } else {
        app_cfg
            .load_hooks(effective_hooks)
            .map_err(|err| err.to_string())?
    };
    app_cfg
        .build_runtime(hooks, global_mode)
        .map_err(|err| err.to_string())
}

fn spawn_reload_watcher(
    app_cfg: AppConfig,
    hook_files: Vec<PathBuf>,
    template: bool,
    global_mode: GlobalExecutionMode,
    shared_hooks: Arc<std::sync::RwLock<HashMap<String, ResolvedHook>>>,
    hotreload: bool,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        #[cfg(unix)]
        use tokio::signal::unix::{SignalKind, signal};
        #[cfg(unix)]
        let mut sighup = signal(SignalKind::hangup()).ok();
        #[cfg(unix)]
        let mut sigusr1 = signal(SignalKind::user_defined1()).ok();

        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        let mut last_modified_map: HashMap<PathBuf, Option<std::time::SystemTime>> = HashMap::new();
        for path in &hook_files {
            let mtime = std::fs::metadata(path).and_then(|m| m.modified()).ok();
            last_modified_map.insert(path.clone(), mtime);
        }

        loop {
            let mut triggered = false;
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = async {
                    #[cfg(unix)]
                    {
                        tokio::select! {
                            _ = async {
                                if let Some(ref mut s) = sighup {
                                    s.recv().await;
                                } else {
                                    std::future::pending::<()>().await;
                                }
                            } => true,
                            _ = async {
                                if let Some(ref mut s) = sigusr1 {
                                    s.recv().await;
                                } else {
                                    std::future::pending::<()>().await;
                                }
                            } => true,
                        }
                    }
                    #[cfg(not(unix))]
                    std::future::pending::<bool>().await
                } => {
                    info!("received SIGHUP/SIGUSR1 reload signal");
                    triggered = true;
                }
                _ = interval.tick() => {
                    if hotreload {
                        let mut changed = false;
                        for path in &hook_files {
                            let current_mtime = std::fs::metadata(path).and_then(|m| m.modified()).ok();
                            if let Some(prev) = last_modified_map.get(path)
                                && *prev != current_mtime
                            {
                                changed = true;
                                last_modified_map.insert(path.clone(), current_mtime);
                            }
                        }
                        if changed {
                            info!("hotreload detected changed hook files");
                            triggered = true;
                        }
                    }
                }
            }

            if triggered {
                info!("reloading hook definitions");
                let load_res = if template {
                    app_cfg.load_hooks_templated(&hook_files)
                } else {
                    app_cfg.load_hooks(&hook_files)
                };
                match load_res {
                    Ok(parsed_hooks) => {
                        match app_cfg.clone().build_runtime(parsed_hooks, global_mode) {
                            Ok(new_runtime) => {
                                let new_map: HashMap<String, ResolvedHook> = new_runtime
                                    .hooks
                                    .into_iter()
                                    .map(|h| (h.definition.id.clone(), h))
                                    .collect();
                                let count = new_map.len();
                                if let Ok(mut lock) = shared_hooks.write() {
                                    *lock = new_map;
                                    info!(count, "hooks reloaded successfully");
                                }
                            }
                            Err(err) => {
                                warn!(
                                    "reload failed while building runtime: {err}; keeping current hooks"
                                );
                            }
                        }
                    }
                    Err(err) => {
                        warn!(
                            "reload failed while parsing hook files: {err}; keeping current hooks"
                        );
                    }
                }
            }
        }
    })
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).ok();
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = async {
                if let Some(ref mut s) = sigterm {
                    s.recv().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn init_storage(
    storage_cfg: &webhook_async_config::StorageConfig,
) -> Result<Arc<dyn QueueStorage>, String> {
    match storage_cfg.backend {
        webhook_async_config::StorageBackendKind::Sqlite => {
            let sqlite_cfg = storage_cfg.sqlite.as_ref().ok_or_else(|| {
                "storage backend is 'sqlite' but [storage.sqlite] configuration is missing"
                    .to_string()
            })?;
            let storage = webhook_async_storage_sqlite::SqliteStorage::connect(sqlite_cfg)
                .await
                .map_err(|err| format!("failed to initialize SQLite storage: {err}"))?;
            Ok(Arc::new(storage))
        }
        webhook_async_config::StorageBackendKind::Valkey
        | webhook_async_config::StorageBackendKind::Redis => {
            let redis_cfg = match storage_cfg.backend {
                webhook_async_config::StorageBackendKind::Valkey => {
                    storage_cfg.valkey.as_ref().ok_or_else(|| {
                        "storage backend is 'valkey' but [storage.valkey] configuration is missing"
                            .to_string()
                    })?
                }
                webhook_async_config::StorageBackendKind::Redis => {
                    storage_cfg.redis.as_ref().ok_or_else(|| {
                        "storage backend is 'redis' but [storage.redis] configuration is missing"
                            .to_string()
                    })?
                }
                _ => unreachable!(),
            };
            let storage = webhook_async_storage_redis::RedisProtocolStorage::connect(
                storage_cfg.backend,
                redis_cfg,
            )
            .await
            .map_err(|err| format!("failed to initialize Valkey/Redis storage: {err}"))?;
            Ok(Arc::new(storage))
        }
    }
}

async fn run_server(args: ServerArgs) -> Result<(), String> {
    if args.list_cipher_suites {
        println!("Available TLS cipher suites:");
        println!("  TLS_AES_128_GCM_SHA256 (TLS 1.3)");
        println!("  TLS_AES_256_GCM_SHA384 (TLS 1.3)");
        println!("  TLS_CHACHA20_POLY1305_SHA256 (TLS 1.3)");
        println!("  TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 (TLS 1.2)");
        println!("  TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 (TLS 1.2)");
        println!("  TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 (TLS 1.2)");
        println!("  TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 (TLS 1.2)");
        println!("  TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256 (TLS 1.2)");
        println!("  TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256 (TLS 1.2)");
        return Ok(());
    }

    validate_server_flags(&args)?;
    init_tracing(args.verbose, args.debug, args.logfile.as_deref());

    if args.nopanic {
        std::panic::set_hook(Box::new(|info| {
            error!("recovered from panic: {info}");
            eprintln!("recovered from panic: {info}");
        }));
    }

    let _pid_guard = PidFileGuard::new(args.pidfile.clone())?;

    let runtime = load_and_resolve_runtime(
        args.config.as_ref(),
        &args.hooks,
        args.sync,
        args.async_mode,
        args.template,
        Some(&args),
    )?;

    let effective_hook_files: Vec<PathBuf> =
        if args.hooks.is_empty() && runtime.app.server.hook_files.is_empty() {
            vec![PathBuf::from("hooks.json")]
        } else if !args.hooks.is_empty() {
            args.hooks.clone()
        } else {
            runtime.app.server.hook_files.clone()
        };

    let runtime_arc = Arc::new(runtime);
    let worker_registry = WorkerRegistry::new();

    let (coordinator, worker_handle, shutdown_tx) = if runtime_arc.requires_queue_backend() {
        let storage_cfg = runtime_arc.app.storage.as_ref().ok_or_else(|| {
            "asynchronous hook execution is requested, but no [storage] backend is configured in TOML configuration file"
                .to_string()
        })?;
        let storage = init_storage(storage_cfg).await?;
        let coordinator = QueueCoordinator::new(storage.clone(), runtime_arc.app.queue.clone());

        if runtime_arc.app.worker.concurrency > 0 {
            let worker_pool = WorkerPool::new(coordinator.clone(), runtime_arc.app.worker.clone())
                .with_registry(worker_registry.clone());
            let (tx, rx) = tokio::sync::watch::channel(false);
            let handle = tokio::spawn(async move {
                let _ = worker_pool.run(rx).await;
            });
            (Some(coordinator), Some(handle), Some(tx))
        } else {
            (Some(coordinator), None, None)
        }
    } else {
        (None, None, None)
    };

    let state = ApiState::with_queue_and_workers(
        (*runtime_arc).clone(),
        coordinator.clone(),
        Some(worker_registry.clone()),
    )
    .map_err(|err| err.to_string())?;

    let shared_hooks = state.hooks_handle();

    let (reload_tx, reload_rx) = tokio::sync::watch::channel(false);
    let reload_handle = spawn_reload_watcher(
        runtime_arc.app.clone(),
        effective_hook_files,
        args.template,
        runtime_arc.global_mode,
        shared_hooks.clone(),
        args.hotreload || runtime_arc.app.server.hot_reload,
        reload_rx,
    );

    let (ui_handle, ui_shutdown_tx) = if runtime_arc.app.ui.enabled {
        let ui_bind = runtime_arc
            .app
            .ui
            .bind
            .parse::<SocketAddr>()
            .map_err(|err| format!("invalid [ui].bind '{}': {err}", runtime_arc.app.ui.bind))?;
        let web_state = WebState::new_with_hooks(
            runtime_arc.clone(),
            coordinator.clone(),
            Some(worker_registry),
            shared_hooks,
        );
        let web_router = webhook_async_web::build_web_router(web_state);
        let (tx, mut rx) = tokio::sync::watch::channel(false);
        info!(
            addr = %ui_bind,
            prefix = %runtime_arc.app.ui.path_prefix,
            "starting management web UI"
        );
        let handle = tokio::spawn(async move {
            let listener = match tokio::net::TcpListener::bind(ui_bind).await {
                Ok(l) => l,
                Err(err) => {
                    error!("failed to bind web UI listener to {ui_bind}: {err}");
                    return;
                }
            };
            let _ = axum::serve(listener, web_router)
                .with_graceful_shutdown(async move {
                    let _ = rx.changed().await;
                })
                .await;
        });
        (Some(handle), Some(tx))
    } else {
        (None, None)
    };

    info!(
        hooks = runtime_arc.hooks.len(),
        async_queue = coordinator.is_some(),
        ui = runtime_arc.app.ui.enabled,
        secure = args.secure,
        socket = ?args.socket,
        "starting receiver"
    );

    let listener_cfg = ListenerConfig {
        unix_socket: args.socket.clone(),
        secure: args.secure,
        cert_file: args.cert.clone(),
        key_file: args.key.clone(),
        setuid: args.setuid,
        setgid: args.setgid,
    };

    let serve_res = serve_full(state, listener_cfg, shutdown_signal())
        .await
        .map_err(|err| err.to_string());

    let _ = reload_tx.send(true);
    if let Some(tx) = shutdown_tx {
        let _ = tx.send(true);
    }
    if let Some(tx) = ui_shutdown_tx {
        let _ = tx.send(true);
    }
    let _ = reload_handle.await;
    if let Some(handle) = worker_handle {
        let _ = handle.await;
    }
    if let Some(handle) = ui_handle {
        let _ = handle.await;
    }

    serve_res
}

async fn run_worker(args: WorkerArgs) -> Result<(), String> {
    init_tracing(args.verbose, args.debug, None);

    let app_cfg = if let Some(path) = args.config.as_ref() {
        AppConfig::from_toml_file(path)
            .map_err(|err| format!("failed to load config file: {err}"))?
    } else {
        AppConfig::default()
    };

    let storage_cfg = app_cfg.storage.as_ref().ok_or_else(|| {
        "standalone worker requires a configured [storage] backend in TOML configuration file"
            .to_string()
    })?;
    let storage = init_storage(storage_cfg).await?;
    let coordinator = QueueCoordinator::new(storage.clone(), app_cfg.queue.clone());

    let mut pool = WorkerPool::new(coordinator, app_cfg.worker.clone());
    if let Some(wid) = args.worker_id {
        pool = pool.with_worker_id(wid);
    }

    info!(
        worker_id = %pool.worker_id(),
        concurrency = app_cfg.worker.concurrency,
        "starting background queue worker"
    );

    let (tx, rx) = tokio::sync::watch::channel(false);
    let worker_handle = tokio::spawn(async move {
        let _ = pool.run(rx).await;
    });

    shutdown_signal().await;
    info!("worker shutdown signal received; draining in-flight tasks");
    let _ = tx.send(true);
    let _ = worker_handle.await;
    Ok(())
}

fn run_validate(args: ValidateArgs) -> Result<(), String> {
    let runtime = load_and_resolve_runtime(
        args.config.as_ref(),
        &args.hooks,
        args.sync,
        args.async_mode,
        args.template,
        None,
    )?;
    let mode = match runtime.global_mode {
        GlobalExecutionMode::Unspecified => "default(sync)",
        GlobalExecutionMode::Sync => "sync",
        GlobalExecutionMode::Async => "async",
    };
    let storage = runtime
        .app
        .storage
        .as_ref()
        .map_or("none", |s| match s.backend {
            webhook_async_config::StorageBackendKind::Sqlite => "sqlite",
            webhook_async_config::StorageBackendKind::Valkey => "valkey",
            webhook_async_config::StorageBackendKind::Redis => "redis",
        });
    println!(
        "valid: hooks={} global_mode={} storage={} config_version={}",
        runtime.hooks.len(),
        mode,
        storage,
        runtime.config_version
    );
    Ok(())
}

async fn load_storage_from_config(
    config_path: Option<&PathBuf>,
) -> Result<Arc<dyn QueueStorage>, String> {
    let app_cfg = if let Some(path) = config_path {
        AppConfig::from_toml_file(path)
            .map_err(|err| format!("failed to load config file: {err}"))?
    } else {
        AppConfig::default()
    };
    let storage_cfg = app_cfg.storage.as_ref().ok_or_else(|| {
        "command requires a configured [storage] backend in TOML configuration file".to_string()
    })?;
    init_storage(storage_cfg).await
}

async fn run_jobs_list(args: JobsListArgs) -> Result<(), String> {
    let storage = load_storage_from_config(args.config.as_ref()).await?;
    let state = if let Some(s) = args.state {
        Some(JobState::from_str(&s).map_err(|e| format!("invalid state '{s}': {e}"))?)
    } else {
        None
    };
    let filter = JobFilter {
        state,
        hook_id: args.hook,
        limit: args.limit,
        offset: args.offset,
    };
    let page = storage
        .list_jobs(filter)
        .await
        .map_err(|err| format!("failed to list jobs: {err}"))?;
    if args.json {
        let serialized = serde_json::to_string_pretty(&page)
            .map_err(|err| format!("failed to serialize JSON: {err}"))?;
        println!("{serialized}");
    } else {
        println!(
            "{:<36} {:<20} {:<14} {:<10} {:<24}",
            "JOB ID", "HOOK ID", "STATE", "ATTEMPTS", "CREATED AT"
        );
        println!("{}", "-".repeat(108));
        for job in &page.items {
            println!(
                "{:<36} {:<20} {:<14} {}/{}      {:<24}",
                job.id,
                job.hook_id,
                job.state.as_str(),
                job.attempt,
                job.max_attempts,
                job.created_at.to_rfc3339()
            );
        }
        println!(
            "\nTotal: {} (showing offset {}, limit {})",
            page.total, page.offset, page.limit
        );
    }
    Ok(())
}

async fn run_jobs_get(args: JobIdArgs) -> Result<(), String> {
    let storage = load_storage_from_config(args.config.as_ref()).await?;
    let job_id =
        JobId::from_str(&args.id).map_err(|err| format!("invalid job id '{}': {err}", args.id))?;
    let job = storage
        .get_job(job_id)
        .await
        .map_err(|err| format!("failed to get job: {err}"))?
        .ok_or_else(|| format!("job '{}' not found", args.id))?;
    let attempts = storage
        .list_attempts(job_id)
        .await
        .map_err(|err| format!("failed to list job attempts: {err}"))?;
    if args.json {
        let serialized = serde_json::to_string_pretty(&json!({
            "job": job,
            "attempts": attempts,
        }))
        .map_err(|err| format!("failed to serialize JSON: {err}"))?;
        println!("{serialized}");
    } else {
        println!("Job ID:         {}", job.id);
        println!("Hook ID:        {}", job.hook_id);
        println!("State:          {}", job.state.as_str());
        println!("Priority:       {}", job.priority);
        println!("Attempts:       {}/{}", job.attempt, job.max_attempts);
        println!("Created At:     {}", job.created_at.to_rfc3339());
        if let Some(s) = job.started_at {
            println!("Started At:     {}", s.to_rfc3339());
        }
        if let Some(c) = job.completed_at {
            println!("Completed At:   {}", c.to_rfc3339());
        }
        if let Some(w) = &job.worker_id {
            println!("Worker ID:      {}", w);
        }
        if let Some(k) = &job.idempotency_key {
            println!("Idempotency:    {}", k);
        }
        if let Some(code) = job.exit_code {
            println!("Exit Code:      {}", code);
        }
        if let Some(err) = &job.error_summary {
            println!("Error Summary:  {}", err);
        }
        println!("\nAttempts ({}):", attempts.len());
        for att in &attempts {
            let dur_ms = att
                .finished_at
                .map_or(0, |f| (f - att.started_at).num_milliseconds());
            println!(
                "  Attempt #{} (worker: {}, duration: {}ms, exit: {:?}):",
                att.attempt, att.worker_id, dur_ms, att.exit_code
            );
            if let Some(out) = &att.stdout_preview {
                println!("    stdout: {}", out.trim());
            }
            if let Some(err) = &att.stderr_preview {
                println!("    stderr: {}", err.trim());
            }
        }
    }
    Ok(())
}

async fn run_jobs_retry(args: JobIdArgs) -> Result<(), String> {
    let storage = load_storage_from_config(args.config.as_ref()).await?;
    let job_id =
        JobId::from_str(&args.id).map_err(|err| format!("invalid job id '{}': {err}", args.id))?;
    let job = storage
        .retry_job(job_id, Utc::now())
        .await
        .map_err(|err| format!("failed to retry job: {err}"))?;
    if args.json {
        let serialized = serde_json::to_string_pretty(&job)
            .map_err(|err| format!("failed to serialize JSON: {err}"))?;
        println!("{serialized}");
    } else {
        println!(
            "Job {} queued for retry (state: {}, max_attempts: {})",
            job.id,
            job.state.as_str(),
            job.max_attempts
        );
    }
    Ok(())
}

async fn run_jobs_cancel(args: JobIdArgs) -> Result<(), String> {
    let storage = load_storage_from_config(args.config.as_ref()).await?;
    let job_id =
        JobId::from_str(&args.id).map_err(|err| format!("invalid job id '{}': {err}", args.id))?;
    let job = storage
        .request_cancel(job_id, Utc::now())
        .await
        .map_err(|err| format!("failed to cancel job: {err}"))?;
    if args.json {
        let serialized = serde_json::to_string_pretty(&job)
            .map_err(|err| format!("failed to serialize JSON: {err}"))?;
        println!("{serialized}");
    } else {
        println!(
            "Job {} cancel requested (state: {})",
            job.id,
            job.state.as_str()
        );
    }
    Ok(())
}

async fn run_storage_status(args: StorageStatusArgs) -> Result<(), String> {
    let storage = load_storage_from_config(args.config.as_ref()).await?;
    let ping_ok = storage.ping().await.is_ok();
    let caps = storage.capabilities();
    let stats = storage
        .stats()
        .await
        .map_err(|err| format!("failed to query storage stats: {err}"))?;
    if args.json {
        let serialized = serde_json::to_string_pretty(&json!({
            "backend": caps.backend,
            "connected": ping_ok,
            "capabilities": {
                "supports_priority": caps.supports_priority,
                "supports_multi_host": caps.supports_multi_host,
                "supports_idempotency": caps.supports_idempotency,
            },
            "stats": stats,
        }))
        .map_err(|err| format!("failed to serialize JSON: {err}"))?;
        println!("{serialized}");
    } else {
        println!("Backend:       {}", caps.backend);
        println!("Connected:     {}", ping_ok);
        println!("Pending Jobs:  {}", stats.pending_count);
        println!("Capabilities:");
        println!("  priority:    {}", caps.supports_priority);
        println!("  multi-host:  {}", caps.supports_multi_host);
        println!("  idempotency: {}", caps.supports_idempotency);
        println!("Queue Counts:");
        for (st, count) in &stats.counts_by_state {
            println!("  {:<16} {}", st, count);
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let cli = Cli::parse_from(normalize_legacy_args(std::env::args_os()));
    match cli.command {
        Command::Version => {
            println!("webhook-async {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Command::Config {
            command: ConfigCommand::Validate(args),
        } => match run_validate(args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(msg) => {
                eprintln!("error: {msg}");
                ExitCode::from(1)
            }
        },
        Command::Server(args) | Command::Run(args) => {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    eprintln!("error: failed to build Tokio runtime: {err}");
                    return ExitCode::from(1);
                }
            };
            match rt.block_on(run_server(*args)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(msg) => {
                    error!("{msg}");
                    eprintln!("error: {msg}");
                    ExitCode::from(1)
                }
            }
        }
        Command::Worker(args) => {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    eprintln!("error: failed to build Tokio runtime: {err}");
                    return ExitCode::from(1);
                }
            };
            match rt.block_on(run_worker(*args)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(msg) => {
                    error!("{msg}");
                    eprintln!("error: {msg}");
                    ExitCode::from(1)
                }
            }
        }
        Command::Jobs { command } => {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    eprintln!("error: failed to build Tokio runtime: {err}");
                    return ExitCode::from(1);
                }
            };
            let res = match command {
                JobsCommand::List(args) => rt.block_on(run_jobs_list(args)),
                JobsCommand::Get(args) => rt.block_on(run_jobs_get(args)),
                JobsCommand::Retry(args) => rt.block_on(run_jobs_retry(args)),
                JobsCommand::Cancel(args) => rt.block_on(run_jobs_cancel(args)),
            };
            match res {
                Ok(()) => ExitCode::SUCCESS,
                Err(msg) => {
                    eprintln!("error: {msg}");
                    ExitCode::from(1)
                }
            }
        }
        Command::Storage { command } => {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    eprintln!("error: failed to build Tokio runtime: {err}");
                    return ExitCode::from(1);
                }
            };
            let res = match command {
                StorageCommand::Status(args) => rt.block_on(run_storage_status(args)),
            };
            match res {
                Ok(()) => ExitCode::SUCCESS,
                Err(msg) => {
                    eprintln!("error: {msg}");
                    ExitCode::from(1)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn normalizes_upstream_single_dash_flags() {
        let out = normalize_legacy_args([
            "webhook-async",
            "server",
            "-hooks",
            "a.json",
            "-port=9123",
            "-x-request-id",
            "-h",
        ]);
        let strs: Vec<_> = out
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            strs,
            [
                "webhook-async",
                "server",
                "--hooks",
                "a.json",
                "--port=9123",
                "--x-request-id",
                "-h"
            ]
        );
    }

    #[test]
    fn rejects_conflicting_sync_and_async_flags_before_binding() {
        let hooks = NamedTempFile::new().expect("temp");
        let err =
            load_and_resolve_runtime(None, &[hooks.path().to_path_buf()], true, true, false, None)
                .expect_err("must reject --sync + --async");
        assert!(err.contains("cannot be used together"));
    }

    #[test]
    fn config_validate_requires_storage_when_async_is_enabled() {
        let mut hooks = NamedTempFile::with_suffix(".json").expect("temp");
        write!(
            hooks,
            r#"[{{"id":"h1","execute-command":"/bin/echo","async":true}}]"#
        )
        .expect("write");

        let err = load_and_resolve_runtime(
            None,
            &[hooks.path().to_path_buf()],
            false,
            false,
            false,
            None,
        )
        .expect_err("async hook without storage must fail");
        assert!(err.contains("storage"));
    }

    #[test]
    fn config_validate_passes_for_sync_hooks_without_storage() {
        let mut hooks = NamedTempFile::with_suffix(".json").expect("temp");
        write!(hooks, r#"[{{"id":"h1","execute-command":"/bin/echo"}}]"#).expect("write");

        let runtime = load_and_resolve_runtime(
            None,
            &[hooks.path().to_path_buf()],
            false,
            false,
            false,
            None,
        )
        .expect("sync hooks without storage must pass");
        assert_eq!(runtime.hooks.len(), 1);
        assert!(!runtime.requires_queue_backend());
    }

    #[test]
    fn parses_header_and_bind_addresses() {
        let hdr = parse_header_flag("X-Custom=abc=123").expect("valid");
        assert_eq!(hdr.name, "X-Custom");
        assert_eq!(hdr.value, "abc=123");
        assert!(parse_header_flag("MissingEquals").is_err());

        assert_eq!(
            format_bind_addr("127.0.0.1", 9000).expect("v4").to_string(),
            "127.0.0.1:9000"
        );
        assert_eq!(
            format_bind_addr("::1", 9000).expect("v6").to_string(),
            "[::1]:9000"
        );
    }

    #[test]
    fn server_flag_validation_and_pidfile_guard() {
        let args_invalid_tls = ServerArgs {
            tls_min_version: Some("1.1".into()),
            ..Default::default()
        };
        assert!(validate_server_flags(&args_invalid_tls).is_err());

        let args_custom_ciphers = ServerArgs {
            cipher_suites: Some("TLS_ECDHE_RSA".into()),
            ..Default::default()
        };
        assert!(validate_server_flags(&args_custom_ciphers).is_err());

        let args_mismatched_uid = ServerArgs {
            setuid: Some(1000),
            setgid: None,
            ..Default::default()
        };
        assert!(validate_server_flags(&args_mismatched_uid).is_err());

        let args_socket_and_uid = ServerArgs {
            socket: Some(PathBuf::from("/tmp/test.sock")),
            setuid: Some(1000),
            setgid: Some(1000),
            ..Default::default()
        };
        assert!(validate_server_flags(&args_socket_and_uid).is_err());

        let args_valid = ServerArgs {
            tls_min_version: Some("1.2".into()),
            ..Default::default()
        };
        assert!(validate_server_flags(&args_valid).is_ok());

        // Test PidFileGuard
        let pid_path =
            std::env::temp_dir().join(format!("test_webhook_{}.pid", std::process::id()));
        {
            let guard = PidFileGuard::new(Some(pid_path.clone())).expect("pid guard");
            assert!(pid_path.exists());
            let content = std::fs::read_to_string(&pid_path).expect("read pid");
            assert_eq!(content.trim(), std::process::id().to_string());
            drop(guard);
        }
        assert!(!pid_path.exists());
    }

    #[test]
    fn templated_hooks_validate_successfully() {
        let mut hooks = NamedTempFile::with_suffix(".json").expect("temp");
        hooks
            .write_all(br#"[{"id":"{{ getenv('HOOK_ID_NON_EXISTENT', 'default-id') }}","execute-command":"/bin/echo"}]"#)
            .expect("write");

        let runtime = load_and_resolve_runtime(
            None,
            &[hooks.path().to_path_buf()],
            false,
            false,
            true,
            None,
        )
        .expect("templated hook parse");
        assert_eq!(runtime.hooks[0].definition.id, "default-id");
    }

    #[tokio::test]
    async fn init_storage_connects_sqlite_memory() {
        let cfg = webhook_async_config::StorageConfig {
            backend: webhook_async_config::StorageBackendKind::Sqlite,
            sqlite: Some(webhook_async_config::SqliteStorageConfig {
                database_url: "sqlite::memory:".to_string(),
                wal_mode: false,
                busy_timeout_ms: 5000,
            }),
            valkey: None,
            redis: None,
        };
        let storage = init_storage(&cfg).await.expect("storage init");
        assert_eq!(storage.ping().await.unwrap(), ());
    }

    #[tokio::test]
    async fn worker_fails_if_no_storage_in_config() {
        let err = run_worker(WorkerArgs::default()).await.unwrap_err();
        assert!(err.contains("[storage] backend"));
    }

    #[tokio::test]
    async fn cli_storage_status_and_jobs_management() {
        let db_file = NamedTempFile::new().expect("temp db");
        let db_path = db_file.path().display().to_string();
        let mut cfg_file = NamedTempFile::new().expect("temp");
        write!(
            cfg_file,
            r#"[server]
host = "127.0.0.1"
port = 9000

[storage]
backend = "sqlite"

[storage.sqlite]
database_url = "sqlite:{db_path}"
wal_mode = false
"#
        )
        .expect("write");

        let cfg_path = Some(cfg_file.path().to_path_buf());

        // Storage status (table and json)
        run_storage_status(StorageStatusArgs {
            config: cfg_path.clone(),
            json: false,
        })
        .await
        .expect("status table");

        run_storage_status(StorageStatusArgs {
            config: cfg_path.clone(),
            json: true,
        })
        .await
        .expect("status json");

        // Jobs list (initially empty)
        run_jobs_list(JobsListArgs {
            config: cfg_path.clone(),
            state: None,
            hook: None,
            limit: 50,
            offset: 0,
            json: false,
        })
        .await
        .expect("jobs list empty");

        // Enqueue a job directly to test get, retry, cancel CLI
        let storage = load_storage_from_config(cfg_path.as_ref())
            .await
            .expect("storage");
        let spec = webhook_async_core::job::ExecutionSpec {
            hook_id: "test-cli".to_string(),
            executable: "/bin/echo".to_string(),
            args: vec!["hello".to_string()],
            env: std::collections::BTreeMap::new(),
            working_dir: None,
            temp_files: vec![],
            timeout_seconds: 30,
            capture_output: true,
            max_output_bytes: 4096,
            config_version: "v1".to_string(),
        };
        let coordinator = QueueCoordinator::new(
            storage.clone(),
            webhook_async_config::QueueConfig::default(),
        );
        let meta = webhook_async_queue::EnqueueMetadata {
            request_id: "req-123".to_string(),
            request_method: "POST".to_string(),
            remote_addr: None,
            idempotency_key: Some("cli-idemp".to_string()),
            priority: 0,
            max_attempts: Some(3),
        };
        let outcome = coordinator
            .enqueue(spec, meta, Utc::now())
            .await
            .expect("enqueue");
        let job_id = outcome.job().id;

        // Jobs list with item
        run_jobs_list(JobsListArgs {
            config: cfg_path.clone(),
            state: Some("queued".to_string()),
            hook: Some("test-cli".to_string()),
            limit: 50,
            offset: 0,
            json: true,
        })
        .await
        .expect("jobs list json");

        // Jobs get
        run_jobs_get(JobIdArgs {
            config: cfg_path.clone(),
            id: job_id.to_string(),
            json: false,
        })
        .await
        .expect("jobs get text");

        run_jobs_get(JobIdArgs {
            config: cfg_path.clone(),
            id: job_id.to_string(),
            json: true,
        })
        .await
        .expect("jobs get json");

        // Jobs cancel (queued -> cancelled)
        run_jobs_cancel(JobIdArgs {
            config: cfg_path.clone(),
            id: job_id.to_string(),
            json: false,
        })
        .await
        .expect("jobs cancel");

        // Jobs retry (now cancelled -> retry fails or succeed depending on state; wait: retry is only for Failed or DeadLetter)
        let retry_err = run_jobs_retry(JobIdArgs {
            config: cfg_path.clone(),
            id: job_id.to_string(),
            json: false,
        })
        .await;
        assert!(retry_err.is_err(), "cannot retry cancelled job");
    }
}
