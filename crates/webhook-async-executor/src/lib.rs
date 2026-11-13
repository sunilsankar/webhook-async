//! Safe direct process executor with timeouts, process-group cleanup,
//! temporary-file lifecycle management, and bounded output capture.

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use tempfile::{Builder, TempPath};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::{Child, Command};
use tokio::sync::watch;
use webhook_async_core::ExecutionSpec;

/// Errors returned while preparing or running an allowlisted hook command.
#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("invalid execution spec: {0}")]
    InvalidSpec(String),

    #[error("invalid working directory '{path}': {reason}")]
    InvalidWorkingDir { path: String, reason: String },

    #[error("failed to create temporary argument file for '{env_name}': {source}")]
    TempFile {
        env_name: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to spawn executable '{executable}': {source}")]
    Spawn {
        executable: String,
        #[source]
        source: std::io::Error,
    },

    #[error("process wait/io failed: {0}")]
    Io(#[from] std::io::Error),
}

/// Bounded process output and exit status from running an `ExecutionSpec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOutcome {
    pub exit_code: Option<i32>,
    pub combined_output: Vec<u8>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub timed_out: bool,
    pub cancelled: bool,
}

impl ExecutionOutcome {
    /// Returns `true` when the command exited cleanly with status `0`, did not time out,
    /// and was not cancelled.
    #[must_use]
    pub fn is_success(&self) -> bool {
        !self.timed_out && !self.cancelled && self.exit_code == Some(0)
    }

    /// Formats bounded UTF-8-lossy preview of `stdout` when non-empty.
    #[must_use]
    pub fn stdout_preview(&self) -> Option<String> {
        (!self.stdout.is_empty()).then(|| String::from_utf8_lossy(&self.stdout).into_owned())
    }

    /// Formats bounded UTF-8-lossy preview of `stderr` when non-empty.
    #[must_use]
    pub fn stderr_preview(&self) -> Option<String> {
        (!self.stderr.is_empty()).then(|| String::from_utf8_lossy(&self.stderr).into_owned())
    }
}

/// Executes `spec` directly without shell interpolation, enforces `spec.timeout_seconds`,
/// bounds captured stdout/stderr to `spec.max_output_bytes`, and removes any `temp_files`
/// after the child exits.
pub async fn execute(spec: &ExecutionSpec) -> Result<ExecutionOutcome, ExecutorError> {
    execute_cancellable(spec, None).await
}

/// Executes `spec` with cooperative cancellation support via a `watch::Receiver<bool>`
/// (`true` triggers process-group termination).
pub async fn execute_cancellable(
    spec: &ExecutionSpec,
    mut cancel: Option<watch::Receiver<bool>>,
) -> Result<ExecutionOutcome, ExecutorError> {
    if spec.executable.trim().is_empty() {
        return Err(ExecutorError::InvalidSpec(
            "executable path must not be empty".to_string(),
        ));
    }

    let working_dir = spec
        .working_dir
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());

    if let Some(dir) = working_dir {
        let meta =
            tokio::fs::metadata(dir)
                .await
                .map_err(|err| ExecutorError::InvalidWorkingDir {
                    path: dir.to_string(),
                    reason: err.to_string(),
                })?;
        if !meta.is_dir() {
            return Err(ExecutorError::InvalidWorkingDir {
                path: dir.to_string(),
                reason: "path is not a directory".to_string(),
            });
        }
    }

    if cancel.as_ref().is_some_and(|rx| *rx.borrow()) {
        return Ok(ExecutionOutcome {
            exit_code: None,
            combined_output: Vec::new(),
            stdout: Vec::new(),
            stderr: Vec::new(),
            stdout_truncated: false,
            stderr_truncated: false,
            timed_out: false,
            cancelled: true,
        });
    }

    // Create temp files off the executor hot path via spawn_blocking so file I/O
    // does not block Tokio worker threads, and hold `TempPath` guards until exit.
    let temp_specs = spec.temp_files.clone();
    let working_dir_owned = working_dir.map(str::to_owned);
    let temp_files = tokio::task::spawn_blocking(move || {
        create_temp_files(&temp_specs, working_dir_owned.as_deref())
    })
    .await
    .map_err(|err| ExecutorError::Io(std::io::Error::other(err.to_string())))??;

    let mut cmd = Command::new(&spec.executable);
    cmd.args(&spec.args)
        .envs(&spec.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    if let Some(dir) = working_dir {
        cmd.current_dir(dir);
    }
    for (env_name, path) in &temp_files {
        cmd.env(env_name, path.as_os_str());
    }

    #[cfg(unix)]
    {
        // Put child in its own process group so timeout/cancel termination kills descendants
        // that would otherwise keep stdout/stderr pipes open.
        cmd.process_group(0);
    }

    let mut child = cmd.spawn().map_err(|source| ExecutorError::Spawn {
        executable: spec.executable.clone(),
        source,
    })?;

    let stdout_pipe = child.stdout.take();
    let stderr_pipe = child.stderr.take();
    let max_bytes = spec.max_output_bytes;

    let io_task =
        tokio::spawn(async move { read_bounded_pipes(stdout_pipe, stderr_pipe, max_bytes).await });

    let timeout_sleep = if spec.timeout_seconds > 0 {
        Some(tokio::time::sleep(Duration::from_secs(
            spec.timeout_seconds,
        )))
    } else {
        None
    };
    tokio::pin!(timeout_sleep);

    let wait_outcome = tokio::select! {
        biased;
        _ = wait_for_cancel(&mut cancel) => {
            terminate_child_tree(&mut child).await;
            let _ = child.wait().await;
            WaitResult::Cancelled
        }
        res = child.wait() => WaitResult::Exited(res?),
        _ = async {
            if let Some(sleep) = timeout_sleep.as_mut().as_pin_mut() {
                sleep.await;
            } else {
                std::future::pending::<()>().await;
            }
        } => {
            terminate_child_tree(&mut child).await;
            let _ = child.wait().await;
            WaitResult::TimedOut
        }
    };

    // Wait for pipe drain after termination; drop temp files explicitly afterwards.
    let captured = match io_task.await {
        Ok(Ok(c)) => c,
        Ok(Err(err)) => {
            drop(temp_files);
            return Err(ExecutorError::Io(err));
        }
        Err(err) => {
            drop(temp_files);
            return Err(ExecutorError::Io(std::io::Error::other(err.to_string())));
        }
    };
    drop(temp_files);

    Ok(match wait_outcome {
        WaitResult::Exited(status) => ExecutionOutcome {
            exit_code: status.code(),
            combined_output: captured.combined,
            stdout: captured.stdout,
            stderr: captured.stderr,
            stdout_truncated: captured.stdout_truncated,
            stderr_truncated: captured.stderr_truncated,
            timed_out: false,
            cancelled: false,
        },
        WaitResult::TimedOut => ExecutionOutcome {
            exit_code: None,
            combined_output: captured.combined,
            stdout: captured.stdout,
            stderr: captured.stderr,
            stdout_truncated: captured.stdout_truncated,
            stderr_truncated: captured.stderr_truncated,
            timed_out: true,
            cancelled: false,
        },
        WaitResult::Cancelled => ExecutionOutcome {
            exit_code: None,
            combined_output: captured.combined,
            stdout: captured.stdout,
            stderr: captured.stderr,
            stdout_truncated: captured.stdout_truncated,
            stderr_truncated: captured.stderr_truncated,
            timed_out: false,
            cancelled: true,
        },
    })
}

async fn wait_for_cancel(cancel: &mut Option<watch::Receiver<bool>>) {
    match cancel {
        Some(rx) => {
            if rx.wait_for(|&c| c).await.is_err() {
                std::future::pending::<()>().await;
            }
        }
        None => std::future::pending::<()>().await,
    }
}

enum WaitResult {
    Exited(std::process::ExitStatus),
    TimedOut,
    Cancelled,
}

fn create_temp_files(
    specs: &[webhook_async_core::TempFileSpec],
    working_dir: Option<&str>,
) -> Result<Vec<(String, TempPath)>, ExecutorError> {
    use std::io::Write;

    let mut out = Vec::with_capacity(specs.len());
    for spec in specs {
        let mut file = match working_dir {
            Some(dir) => Builder::new()
                .prefix("webhook-")
                .tempfile_in(Path::new(dir)),
            None => Builder::new().prefix("webhook-").tempfile(),
        }
        .map_err(|source| ExecutorError::TempFile {
            env_name: spec.env_name.clone(),
            source,
        })?;

        file.write_all(&spec.content)
            .map_err(|source| ExecutorError::TempFile {
                env_name: spec.env_name.clone(),
                source,
            })?;

        // Convert to `TempPath` so the file descriptor closes before spawning the child
        // while keeping automatic unlink-on-drop semantics.
        let path = file.into_temp_path();
        out.push((spec.env_name.clone(), path));
    }
    Ok(out)
}

async fn terminate_child_tree(child: &mut Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id()
        && let Ok(pgid) = libc::pid_t::try_from(pid)
        && pgid > 0
    {
        // SAFETY: `pgid` came from a live `tokio::process::Child` placed into its own
        // process group via `Command::process_group(0)` and `child.wait()` has not reaped it yet.
        // Sending SIGKILL to `-pgid` kills the child and any spawned descendants.
        unsafe {
            libc::kill(-pgid, libc::SIGKILL);
        }
        return;
    }

    let _ = child.kill().await;
}

struct CapturedOutput {
    combined: Vec<u8>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdout_truncated: bool,
    stderr_truncated: bool,
}

async fn read_bounded_pipes<R1, R2>(
    mut stdout_pipe: Option<R1>,
    mut stderr_pipe: Option<R2>,
    max_bytes: usize,
) -> std::io::Result<CapturedOutput>
where
    R1: AsyncRead + Unpin,
    R2: AsyncRead + Unpin,
{
    let mut combined = Vec::new();
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut stdout_truncated = false;
    let mut stderr_truncated = false;

    let mut out_buf = [0u8; 4096];
    let mut err_buf = [0u8; 4096];

    loop {
        match (&mut stdout_pipe, &mut stderr_pipe) {
            (None, None) => break,
            (Some(out_r), Some(err_r)) => {
                tokio::select! {
                    n = out_r.read(&mut out_buf) => {
                        let n = n?;
                        if n == 0 {
                            stdout_pipe = None;
                        } else {
                            append_bounded(&mut stdout, &mut stdout_truncated, &out_buf[..n], max_bytes);
                            append_bounded_silent(&mut combined, &out_buf[..n], max_bytes);
                        }
                    }
                    n = err_r.read(&mut err_buf) => {
                        let n = n?;
                        if n == 0 {
                            stderr_pipe = None;
                        } else {
                            append_bounded(&mut stderr, &mut stderr_truncated, &err_buf[..n], max_bytes);
                            append_bounded_silent(&mut combined, &err_buf[..n], max_bytes);
                        }
                    }
                }
            }
            (Some(out_r), None) => {
                let n = out_r.read(&mut out_buf).await?;
                if n == 0 {
                    stdout_pipe = None;
                } else {
                    append_bounded(&mut stdout, &mut stdout_truncated, &out_buf[..n], max_bytes);
                    append_bounded_silent(&mut combined, &out_buf[..n], max_bytes);
                }
            }
            (None, Some(err_r)) => {
                let n = err_r.read(&mut err_buf).await?;
                if n == 0 {
                    stderr_pipe = None;
                } else {
                    append_bounded(&mut stderr, &mut stderr_truncated, &err_buf[..n], max_bytes);
                    append_bounded_silent(&mut combined, &err_buf[..n], max_bytes);
                }
            }
        }
    }

    Ok(CapturedOutput {
        combined,
        stdout,
        stderr,
        stdout_truncated,
        stderr_truncated,
    })
}

fn append_bounded(dest: &mut Vec<u8>, truncated: &mut bool, chunk: &[u8], max_bytes: usize) {
    let remaining = max_bytes.saturating_sub(dest.len());
    let take = chunk.len().min(remaining);
    if take > 0 {
        dest.extend_from_slice(&chunk[..take]);
    }
    if take < chunk.len() {
        *truncated = true;
    }
}

fn append_bounded_silent(dest: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) {
    let remaining = max_bytes.saturating_sub(dest.len());
    let take = chunk.len().min(remaining);
    if take > 0 {
        dest.extend_from_slice(&chunk[..take]);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use webhook_async_core::TempFileSpec;

    use super::*;

    fn base_spec(executable: &str, args: &[&str]) -> ExecutionSpec {
        ExecutionSpec {
            hook_id: "test".to_string(),
            executable: executable.to_string(),
            args: args.iter().map(|s| (*s).to_string()).collect(),
            env: BTreeMap::new(),
            working_dir: None,
            temp_files: Vec::new(),
            timeout_seconds: 10,
            capture_output: true,
            max_output_bytes: 1024,
            config_version: "v1".to_string(),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn executes_direct_command_with_args_env_and_temp_file_cleanup() {
        let work_dir = tempfile::tempdir().expect("tempdir");
        let mut spec = base_spec(
            "/bin/sh",
            &[
                "-c",
                "printf '%s|%s|' \"$1\" \"$HOOK_CUSTOM\"; cat \"$HOOK_PAYLOAD_FILE\"",
                "sh",
                "arg-one",
            ],
        );
        spec.working_dir = Some(work_dir.path().to_string_lossy().into_owned());
        spec.env
            .insert("HOOK_CUSTOM".to_string(), "env-val".to_string());
        spec.temp_files.push(TempFileSpec {
            env_name: "HOOK_PAYLOAD_FILE".to_string(),
            content: b"temp-body".to_vec(),
        });

        let outcome = execute(&spec).await.expect("execute");
        assert!(outcome.is_success());
        assert_eq!(outcome.exit_code, Some(0));
        assert_eq!(outcome.stdout, b"arg-one|env-val|temp-body");

        // Temp file created inside working_dir must be removed after exit.
        let entries: Vec<_> = std::fs::read_dir(work_dir.path())
            .expect("readdir")
            .collect();
        assert!(entries.is_empty(), "temp files must be cleaned up");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn does_not_interpret_shell_metacharacters_in_args() {
        let spec = base_spec("/bin/echo", &["hello; echo INJECTED"]);
        let outcome = execute(&spec).await.expect("execute");
        assert!(outcome.is_success());
        assert_eq!(outcome.stdout, b"hello; echo INJECTED\n");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn enforces_output_bound_without_broken_pipe_failure() {
        let mut spec = base_spec("/bin/sh", &["-c", "head -c 20000 /dev/zero | tr '\\0' 'a'"]);
        spec.max_output_bytes = 64;
        let outcome = execute(&spec).await.expect("execute");
        assert!(outcome.is_success());
        assert_eq!(outcome.stdout.len(), 64);
        assert!(outcome.stdout_truncated);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_kills_process_group_including_descendants() {
        let mut spec = base_spec("/bin/sh", &["-c", "sleep 30 & wait"]);
        spec.timeout_seconds = 1;
        let start = std::time::Instant::now();
        let outcome = execute(&spec).await.expect("execute");
        assert!(outcome.timed_out);
        assert!(!outcome.is_success());
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "process group kill should prevent pipe hang"
        );
    }

    #[tokio::test]
    async fn rejects_missing_working_directory() {
        let mut spec = base_spec("/bin/echo", &["hi"]);
        spec.working_dir = Some("/nonexistent/webhook-async-missing-dir".to_string());
        let err = execute(&spec).await.expect_err("must reject invalid dir");
        assert!(matches!(err, ExecutorError::InvalidWorkingDir { .. }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_kills_process_group_including_descendants() {
        let mut spec = base_spec("/bin/sh", &["-c", "sleep 30 & wait"]);
        spec.timeout_seconds = 30;
        let (tx, rx) = watch::channel(false);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(80)).await;
            let _ = tx.send(true);
        });
        let start = std::time::Instant::now();
        let outcome = execute_cancellable(&spec, Some(rx))
            .await
            .expect("execute_cancellable");
        assert!(outcome.cancelled);
        assert!(!outcome.timed_out);
        assert!(!outcome.is_success());
        assert!(start.elapsed() < Duration::from_secs(5));
    }
}
