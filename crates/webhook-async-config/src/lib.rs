//! Application TOML configuration, validation, secret redaction, and runtime mode resolution.

pub mod error;
pub mod redact;

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use webhook_async_compat::{parse_hooks_files, parse_hooks_files_templated};
use webhook_async_core::{
    EffectiveExecutionMode, GlobalExecutionMode, HookDefinition, ResponseHeader,
    resolve_execution_mode,
};

pub use error::ConfigError;
pub use redact::redact_url_credentials;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub storage: Option<StorageConfig>,
    #[serde(default)]
    pub worker: WorkerConfig,
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub ui: UiConfig,
    #[serde(default)]
    pub auth: AuthConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_url_prefix")]
    pub url_prefix: String,
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
    #[serde(default = "default_max_multipart_mem_bytes")]
    pub max_multipart_mem_bytes: usize,
    #[serde(default)]
    pub hook_files: Vec<PathBuf>,
    #[serde(default)]
    pub hot_reload: bool,
    #[serde(default)]
    pub default_http_methods: Vec<String>,
    #[serde(default)]
    pub response_headers: Vec<ResponseHeader>,
    #[serde(default)]
    pub use_x_request_id: bool,
    #[serde(default)]
    pub x_request_id_limit: usize,
    #[serde(default)]
    pub trust_proxy_headers: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            url_prefix: default_url_prefix(),
            max_body_bytes: default_max_body_bytes(),
            max_multipart_mem_bytes: default_max_multipart_mem_bytes(),
            hook_files: Vec::new(),
            hot_reload: false,
            default_http_methods: Vec::new(),
            response_headers: Vec::new(),
            use_x_request_id: false,
            x_request_id_limit: 0,
            trust_proxy_headers: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackendKind {
    Sqlite,
    Valkey,
    Redis,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub backend: StorageBackendKind,
    #[serde(default)]
    pub sqlite: Option<SqliteStorageConfig>,
    #[serde(default)]
    pub valkey: Option<RedisProtocolStorageConfig>,
    #[serde(default)]
    pub redis: Option<RedisProtocolStorageConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteStorageConfig {
    pub database_url: String,
    #[serde(default = "default_true")]
    pub wal_mode: bool,
    #[serde(default = "default_busy_timeout_ms")]
    pub busy_timeout_ms: u64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisProtocolStorageConfig {
    pub url: String,
    #[serde(default = "default_key_prefix")]
    pub key_prefix: String,
    #[serde(default = "default_stream_name")]
    pub stream_name: String,
    #[serde(default = "default_consumer_group")]
    pub consumer_group: String,
}

impl fmt::Debug for RedisProtocolStorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisProtocolStorageConfig")
            .field("url", &redact_url_credentials(&self.url))
            .field("key_prefix", &self.key_prefix)
            .field("stream_name", &self.stream_name)
            .field("consumer_group", &self.consumer_group)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerConfig {
    #[serde(default = "default_concurrency")]
    pub concurrency: u32,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_lease_duration_seconds")]
    pub lease_duration_seconds: u64,
    #[serde(default = "default_heartbeat_interval_seconds")]
    pub heartbeat_interval_seconds: u64,
    #[serde(default = "default_shutdown_grace_seconds")]
    pub shutdown_grace_seconds: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: default_concurrency(),
            poll_interval_ms: default_poll_interval_ms(),
            lease_duration_seconds: default_lease_duration_seconds(),
            heartbeat_interval_seconds: default_heartbeat_interval_seconds(),
            shutdown_grace_seconds: default_shutdown_grace_seconds(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueConfig {
    #[serde(default = "default_max_pending_jobs")]
    pub max_pending_jobs: u64,
    #[serde(default = "default_max_attempts")]
    pub default_max_attempts: u32,
    #[serde(default = "default_retry_base_delay_seconds")]
    pub retry_base_delay_seconds: u64,
    #[serde(default = "default_retry_max_delay_seconds")]
    pub retry_max_delay_seconds: u64,
    #[serde(default = "default_timeout_seconds")]
    pub default_timeout_seconds: u64,
    #[serde(default = "default_max_output_bytes")]
    pub max_output_bytes: usize,
    #[serde(default = "default_idempotency_retention_seconds")]
    pub idempotency_retention_seconds: u64,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_pending_jobs: default_max_pending_jobs(),
            default_max_attempts: default_max_attempts(),
            retry_base_delay_seconds: default_retry_base_delay_seconds(),
            retry_max_delay_seconds: default_retry_max_delay_seconds(),
            default_timeout_seconds: default_timeout_seconds(),
            max_output_bytes: default_max_output_bytes(),
            idempotency_retention_seconds: default_idempotency_retention_seconds(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_host")]
    pub bind: String,
    #[serde(default = "default_ui_prefix")]
    pub path_prefix: String,
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: default_host(),
            path_prefix: default_ui_prefix(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    #[serde(default)]
    pub admin_token: Option<String>,
    #[serde(default)]
    pub admin_token_env: Option<String>,
    #[serde(default)]
    pub admin_token_file: Option<PathBuf>,
}

impl AuthConfig {
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.admin_token.is_some()
            || self.admin_token_env.is_some()
            || self.admin_token_file.is_some()
    }

    /// Resolves the admin token from literal config, environment variable, or file.
    ///
    /// Precedence:
    /// 1. `admin_token`: literal token value directly in configuration.
    /// 2. `admin_token_env`: environment variable name. If not present in env, falls back to
    ///    reading as a file path if it points to an existing file.
    /// 3. `admin_token_file`: file path whose contents are read and trimmed.
    ///
    /// Returns `Ok(None)` if unconfigured or if the configured source resolves to empty.
    pub fn resolve_admin_token(&self) -> Result<Option<String>, std::io::Error> {
        if let Some(token) = &self.admin_token {
            let trimmed = token.trim().to_string();
            if !trimmed.is_empty() {
                return Ok(Some(trimmed));
            }
        }
        if let Some(env_or_path) = &self.admin_token_env {
            if let Ok(val) = std::env::var(env_or_path) {
                let trimmed = val.trim().to_string();
                if !trimmed.is_empty() {
                    return Ok(Some(trimmed));
                }
            }
            let path = Path::new(env_or_path);
            if path.is_file() {
                let content = std::fs::read_to_string(path)?;
                let trimmed = content.trim().to_string();
                if !trimmed.is_empty() {
                    return Ok(Some(trimmed));
                }
            }
        }
        if let Some(path) = &self.admin_token_file {
            let content = std::fs::read_to_string(path)?;
            let trimmed = content.trim().to_string();
            if !trimmed.is_empty() {
                return Ok(Some(trimmed));
            }
        }
        Ok(None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedHook {
    pub definition: HookDefinition,
    pub mode: EffectiveExecutionMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub app: AppConfig,
    pub hooks: Vec<ResolvedHook>,
    pub global_mode: GlobalExecutionMode,
    pub config_version: String,
}

impl RuntimeConfig {
    #[must_use]
    pub fn requires_queue_backend(&self) -> bool {
        self.global_mode == GlobalExecutionMode::Async
            || self.hooks.iter().any(|h| h.mode.is_async())
    }
}

impl AppConfig {
    pub fn from_toml_str(input: &str) -> Result<Self, ConfigError> {
        let cfg: Self = toml::from_str(input)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn from_toml_file(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_toml_str(&raw)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.server.host.trim().is_empty() {
            return Err(ConfigError::validation(
                "server.host",
                "host must not be empty",
            ));
        }
        if self.server.port == 0 {
            return Err(ConfigError::validation(
                "server.port",
                "port must be between 1 and 65535",
            ));
        }
        if self.server.max_body_bytes == 0 {
            return Err(ConfigError::validation(
                "server.max_body_bytes",
                "must be greater than 0",
            ));
        }
        if self.server.max_multipart_mem_bytes == 0 {
            return Err(ConfigError::validation(
                "server.max_multipart_mem_bytes",
                "must be greater than 0",
            ));
        }

        if let Some(storage) = &self.storage {
            validate_storage(storage)?;
        }

        if self.worker.concurrency == 0 {
            return Err(ConfigError::validation(
                "worker.concurrency",
                "must be at least 1",
            ));
        }
        if self.worker.poll_interval_ms < 10 {
            return Err(ConfigError::validation(
                "worker.poll_interval_ms",
                "must be at least 10 ms",
            ));
        }
        if self.worker.heartbeat_interval_seconds == 0 {
            return Err(ConfigError::validation(
                "worker.heartbeat_interval_seconds",
                "must be at least 1 second",
            ));
        }
        if self.worker.lease_duration_seconds <= self.worker.heartbeat_interval_seconds * 2 {
            return Err(ConfigError::validation(
                "worker.lease_duration_seconds",
                "must be strictly greater than 2x worker.heartbeat_interval_seconds",
            ));
        }
        if self.worker.shutdown_grace_seconds == 0 {
            return Err(ConfigError::validation(
                "worker.shutdown_grace_seconds",
                "must be at least 1 second",
            ));
        }

        if self.queue.max_pending_jobs == 0 {
            return Err(ConfigError::validation(
                "queue.max_pending_jobs",
                "must be at least 1",
            ));
        }
        if self.queue.default_max_attempts == 0 {
            return Err(ConfigError::validation(
                "queue.default_max_attempts",
                "must be at least 1",
            ));
        }
        if self.queue.retry_base_delay_seconds == 0 {
            return Err(ConfigError::validation(
                "queue.retry_base_delay_seconds",
                "must be at least 1 second",
            ));
        }
        if self.queue.retry_max_delay_seconds < self.queue.retry_base_delay_seconds {
            return Err(ConfigError::validation(
                "queue.retry_max_delay_seconds",
                "must be >= queue.retry_base_delay_seconds",
            ));
        }
        if self.queue.default_timeout_seconds == 0 {
            return Err(ConfigError::validation(
                "queue.default_timeout_seconds",
                "must be at least 1 second",
            ));
        }
        if self.queue.max_output_bytes < 1024 {
            return Err(ConfigError::validation(
                "queue.max_output_bytes",
                "must be at least 1024 bytes",
            ));
        }
        if self.queue.idempotency_retention_seconds == 0 {
            return Err(ConfigError::validation(
                "queue.idempotency_retention_seconds",
                "must be at least 1 second",
            ));
        }

        if self.ui.bind.trim().is_empty() {
            return Err(ConfigError::validation("ui.bind", "must not be empty"));
        }
        if !self.ui.path_prefix.starts_with('/') {
            return Err(ConfigError::validation(
                "ui.path_prefix",
                "must start with '/'",
            ));
        }

        if let Some(token) = &self.auth.admin_token
            && token.trim().is_empty()
        {
            return Err(ConfigError::validation(
                "auth.admin_token",
                "must not be empty",
            ));
        }
        if let Some(env_name) = &self.auth.admin_token_env
            && env_name.trim().is_empty()
        {
            return Err(ConfigError::validation(
                "auth.admin_token_env",
                "must not be empty",
            ));
        }
        if let Some(token_file) = &self.auth.admin_token_file
            && token_file.as_os_str().is_empty()
        {
            return Err(ConfigError::validation(
                "auth.admin_token_file",
                "must not be empty",
            ));
        }
        let auth_sources = [
            self.auth.admin_token.is_some(),
            self.auth.admin_token_env.is_some(),
            self.auth.admin_token_file.is_some(),
        ]
        .iter()
        .filter(|&&configured| configured)
        .count();
        if auth_sources > 1 {
            return Err(ConfigError::validation(
                "auth",
                "specify at most one of admin_token, admin_token_env, or admin_token_file",
            ));
        }
        if self.ui.enabled && !self.auth.is_configured() {
            return Err(ConfigError::validation(
                "auth",
                "ui.enabled = true requires auth.admin_token, auth.admin_token_env, or auth.admin_token_file",
            ));
        }

        Ok(())
    }

    pub fn load_hooks(
        &self,
        cli_hook_files: &[PathBuf],
    ) -> Result<Vec<HookDefinition>, ConfigError> {
        let paths = if cli_hook_files.is_empty() {
            &self.server.hook_files
        } else {
            cli_hook_files
        };
        Ok(parse_hooks_files(paths)?)
    }

    pub fn load_hooks_templated(
        &self,
        cli_hook_files: &[PathBuf],
    ) -> Result<Vec<HookDefinition>, ConfigError> {
        let paths = if cli_hook_files.is_empty() {
            &self.server.hook_files
        } else {
            cli_hook_files
        };
        Ok(parse_hooks_files_templated(paths, true)?)
    }

    /// Resolves effective per-hook execution modes and ensures a valid `[storage]` backend
    /// exists whenever global `--async` or any hook `async: true` override is active.
    pub fn build_runtime(
        self,
        hooks: Vec<HookDefinition>,
        global_mode: GlobalExecutionMode,
    ) -> Result<RuntimeConfig, ConfigError> {
        self.validate()?;

        let mut seen = HashSet::with_capacity(hooks.len());
        let mut resolved = Vec::with_capacity(hooks.len());
        let mut any_async = global_mode == GlobalExecutionMode::Async;

        for hook in hooks {
            hook.validate()?;
            if !seen.insert(hook.id.clone()) {
                return Err(ConfigError::DuplicateHookId { id: hook.id });
            }
            let mode = resolve_execution_mode(hook.async_mode, global_mode);
            if mode.is_async() {
                any_async = true;
            }
            resolved.push(ResolvedHook {
                definition: hook,
                mode,
            });
        }

        if any_async && self.storage.is_none() {
            return Err(ConfigError::MissingQueueBackend);
        }

        let config_version = compute_config_version(&self, &resolved);
        Ok(RuntimeConfig {
            app: self,
            hooks: resolved,
            global_mode,
            config_version,
        })
    }
}

fn validate_storage(storage: &StorageConfig) -> Result<(), ConfigError> {
    match storage.backend {
        StorageBackendKind::Sqlite => {
            let Some(sqlite) = &storage.sqlite else {
                return Err(ConfigError::validation(
                    "storage.sqlite",
                    "storage.backend = 'sqlite' requires [storage.sqlite]",
                ));
            };
            if !sqlite.database_url.starts_with("sqlite:") || sqlite.database_url == "sqlite:" {
                return Err(ConfigError::validation(
                    "storage.sqlite.database_url",
                    "must be a non-empty sqlite: URL (e.g. sqlite:///var/lib/webhook-async/webhook-async.db)",
                ));
            }
        }
        StorageBackendKind::Valkey => {
            let Some(valkey) = &storage.valkey else {
                return Err(ConfigError::validation(
                    "storage.valkey",
                    "storage.backend = 'valkey' requires [storage.valkey]",
                ));
            };
            validate_redis_protocol_cfg("storage.valkey", valkey)?;
        }
        StorageBackendKind::Redis => {
            let Some(redis) = &storage.redis else {
                return Err(ConfigError::validation(
                    "storage.redis",
                    "storage.backend = 'redis' requires [storage.redis]",
                ));
            };
            validate_redis_protocol_cfg("storage.redis", redis)?;
        }
    }
    Ok(())
}

fn validate_redis_protocol_cfg(
    field_prefix: &'static str,
    cfg: &RedisProtocolStorageConfig,
) -> Result<(), ConfigError> {
    let valid_scheme = cfg.url.starts_with("redis://")
        || cfg.url.starts_with("rediss://")
        || cfg.url.starts_with("valkey://")
        || cfg.url.starts_with("valkeys://");
    if !valid_scheme {
        return Err(ConfigError::validation(
            field_prefix,
            "url must start with redis://, rediss://, valkey://, or valkeys://",
        ));
    }
    if cfg.key_prefix.trim().is_empty()
        || cfg.stream_name.trim().is_empty()
        || cfg.consumer_group.trim().is_empty()
    {
        return Err(ConfigError::validation(
            field_prefix,
            "key_prefix, stream_name, and consumer_group must not be empty",
        ));
    }
    Ok(())
}

fn compute_config_version(app: &AppConfig, hooks: &[ResolvedHook]) -> String {
    let mut hasher = Sha256::new();
    if let Ok(app_json) = serde_json::to_vec(app) {
        hasher.update((app_json.len() as u64).to_le_bytes());
        hasher.update(&app_json);
    }
    for hook in hooks {
        if let Ok(hook_json) = serde_json::to_vec(&hook.definition) {
            hasher.update((hook_json.len() as u64).to_le_bytes());
            hasher.update(&hook_json);
        }
        hasher.update(if hook.mode.is_async() { b"A" } else { b"S" });
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn default_host() -> String {
    "127.0.0.1".into()
}
fn default_url_prefix() -> String {
    "hooks".into()
}
const fn default_port() -> u16 {
    8080
}
const fn default_max_body_bytes() -> usize {
    10 * 1024 * 1024
}
const fn default_max_multipart_mem_bytes() -> usize {
    1_048_576
}
const fn default_true() -> bool {
    true
}
const fn default_busy_timeout_ms() -> u64 {
    5000
}
fn default_key_prefix() -> String {
    "webhook-async:".into()
}
fn default_stream_name() -> String {
    "webhook-async:jobs".into()
}
fn default_consumer_group() -> String {
    "webhook-async-workers".into()
}
const fn default_concurrency() -> u32 {
    4
}
const fn default_poll_interval_ms() -> u64 {
    500
}
const fn default_lease_duration_seconds() -> u64 {
    120
}
const fn default_heartbeat_interval_seconds() -> u64 {
    20
}
const fn default_shutdown_grace_seconds() -> u64 {
    30
}
const fn default_max_pending_jobs() -> u64 {
    10_000
}
const fn default_max_attempts() -> u32 {
    5
}
const fn default_retry_base_delay_seconds() -> u64 {
    5
}
const fn default_retry_max_delay_seconds() -> u64 {
    900
}
const fn default_timeout_seconds() -> u64 {
    300
}
const fn default_max_output_bytes() -> usize {
    65_536
}
const fn default_idempotency_retention_seconds() -> u64 {
    86_400
}
fn default_ui_prefix() -> String {
    "/ui".into()
}

#[cfg(test)]
mod tests {
    use webhook_async_compat::parse_hooks_json;
    use webhook_async_core::{EffectiveExecutionMode, GlobalExecutionMode};

    use super::{AppConfig, AuthConfig, ConfigError, StorageBackendKind, redact_url_credentials};

    #[test]
    fn parses_defaults_for_synchronous_only_mode_without_storage() {
        let cfg = AppConfig::from_toml_str("").expect("empty config uses safe defaults");
        assert_eq!(cfg.server.host, "127.0.0.1");
        assert_eq!(cfg.server.port, 8080);
        assert!(cfg.storage.is_none());
        assert!(!cfg.ui.enabled);

        let hooks = parse_hooks_json(r#"[{"id":"sync","execute-command":"/bin/true"}]"#)
            .expect("hook json");
        let rt = cfg
            .build_runtime(hooks, GlobalExecutionMode::Unspecified)
            .expect("sync mode needs no queue backend");
        assert!(!rt.requires_queue_backend());
        assert_eq!(rt.hooks[0].mode, EffectiveExecutionMode::Sync);
    }

    #[test]
    fn rejects_async_without_storage_and_resolves_mixed_mode_precedence() {
        let cfg = AppConfig::from_toml_str("").expect("default config");
        let sync_hooks =
            parse_hooks_json(r#"[{"id":"a","execute-command":"/bin/true"}]"#).expect("hook json");
        assert!(matches!(
            cfg.clone()
                .build_runtime(sync_hooks, GlobalExecutionMode::Async),
            Err(ConfigError::MissingQueueBackend)
        ));

        let hook_override =
            parse_hooks_json(r#"[{"id":"a","execute-command":"/bin/true","async":true}]"#)
                .expect("hook json");
        assert!(matches!(
            cfg.build_runtime(hook_override, GlobalExecutionMode::Sync),
            Err(ConfigError::MissingQueueBackend)
        ));

        let sqlite_cfg = AppConfig::from_toml_str(
            r#"
[storage]
backend = "sqlite"
[storage.sqlite]
database_url = "sqlite:///tmp/test.db"
"#,
        )
        .expect("sqlite config");

        let mixed = parse_hooks_json(
            r#"[
                {"id":"inherit","execute-command":"/bin/true"},
                {"id":"force-sync","execute-command":"/bin/true","async":false},
                {"id":"force-async","execute-command":"/bin/true","async":true}
            ]"#,
        )
        .expect("mixed hooks");

        let rt = sqlite_cfg
            .build_runtime(mixed, GlobalExecutionMode::Async)
            .expect("valid mixed runtime");
        assert!(rt.requires_queue_backend());
        assert_eq!(rt.hooks[0].mode, EffectiveExecutionMode::Async);
        assert_eq!(rt.hooks[1].mode, EffectiveExecutionMode::Sync);
        assert_eq!(rt.hooks[2].mode, EffectiveExecutionMode::Async);
        assert!(rt.config_version.starts_with("sha256:"));
        assert_eq!(rt.config_version.len(), 7 + 64);
    }

    #[test]
    fn validates_backend_matching_cross_fields_and_redacts_secrets() {
        let mismatched_backend = r#"
[storage]
backend = "valkey"
[storage.sqlite]
database_url = "sqlite:///tmp/test.db"
"#;
        assert!(AppConfig::from_toml_str(mismatched_backend).is_err());

        let bad_lease = r#"
[worker]
lease_duration_seconds = 30
heartbeat_interval_seconds = 20
"#;
        assert!(AppConfig::from_toml_str(bad_lease).is_err());

        let ui_without_auth = r#"
[ui]
enabled = true
"#;
        assert!(AppConfig::from_toml_str(ui_without_auth).is_err());

        let unknown_key = r#"
[server]
unknown_setting = 123
"#;
        assert!(AppConfig::from_toml_str(unknown_key).is_err());

        let valkey_cfg = AppConfig::from_toml_str(
            r#"
[storage]
backend = "valkey"
[storage.valkey]
url = "rediss://default:supersecret@valkey.internal:6379"
"#,
        )
        .expect("valid valkey config");
        assert_eq!(
            valkey_cfg.storage.as_ref().map(|s| s.backend),
            Some(StorageBackendKind::Valkey)
        );
        let debug_str = format!("{valkey_cfg:?}");
        assert!(!debug_str.contains("supersecret"));
        assert!(debug_str.contains("rediss://***@valkey.internal:6379"));
        assert_eq!(
            redact_url_credentials("redis://:pw@127.0.0.1:6379"),
            "redis://***@127.0.0.1:6379"
        );
    }

    #[test]
    fn resolves_admin_token_from_literal_env_or_file() {
        let auth_none = AuthConfig::default();
        assert_eq!(auth_none.resolve_admin_token().unwrap(), None);

        // Literal token resolution
        let auth_literal = AuthConfig {
            admin_token: Some("  literal-secret-token  ".into()),
            admin_token_env: None,
            admin_token_file: None,
        };
        assert_eq!(
            auth_literal.resolve_admin_token().unwrap(),
            Some("literal-secret-token".into())
        );

        // Env var resolution
        let var_name = "WEBHOOK_ASYNC_TEST_TOKEN_XYZ";
        unsafe { std::env::set_var(var_name, "  super-secret-token  ") };
        let auth_env = AuthConfig {
            admin_token: None,
            admin_token_env: Some(var_name.into()),
            admin_token_file: None,
        };
        assert_eq!(
            auth_env.resolve_admin_token().unwrap(),
            Some("super-secret-token".into())
        );
        unsafe { std::env::remove_var(var_name) };

        // File resolution via admin_token_file
        let tmp = tempfile::NamedTempFile::new().expect("temp file");
        std::fs::write(tmp.path(), "  file-secret-token \n").expect("write token");
        let auth_file = AuthConfig {
            admin_token: None,
            admin_token_env: None,
            admin_token_file: Some(tmp.path().to_path_buf()),
        };
        assert_eq!(
            auth_file.resolve_admin_token().unwrap(),
            Some("file-secret-token".into())
        );

        // Fallback: admin_token_env pointing to a file path directly
        let auth_env_file = AuthConfig {
            admin_token: None,
            admin_token_env: Some(tmp.path().to_string_lossy().into()),
            admin_token_file: None,
        };
        assert_eq!(
            auth_env_file.resolve_admin_token().unwrap(),
            Some("file-secret-token".into())
        );
    }
}
