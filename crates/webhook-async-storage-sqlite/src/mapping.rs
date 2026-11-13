use std::str::FromStr;

use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use webhook_async_core::{
    ErrorClass, ExecutionSpec, JobAttempt, JobId, JobRecord, JobState, LeaseToken,
};
use webhook_async_storage::StorageError;

/// Formats UTC timestamps at fixed-width nanosecond precision (`YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`)
/// so SQLite TEXT comparisons (`<=`, `<`, `ORDER BY`) match chronological order exactly.
pub fn fmt_ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

pub fn parse_ts(raw: &str, field: &str) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| StorageError::Internal(format!("invalid timestamp in '{field}': {err}")))
}

pub fn parse_opt_ts(
    raw: Option<String>,
    field: &str,
) -> Result<Option<DateTime<Utc>>, StorageError> {
    raw.map(|s| parse_ts(&s, field)).transpose()
}

pub fn parse_state(raw: &str) -> Result<JobState, StorageError> {
    match raw {
        "queued" => Ok(JobState::Queued),
        "running" => Ok(JobState::Running),
        "retry_wait" => Ok(JobState::RetryWait),
        "succeeded" => Ok(JobState::Succeeded),
        "failed" => Ok(JobState::Failed),
        "cancel_requested" => Ok(JobState::CancelRequested),
        "cancelled" => Ok(JobState::Cancelled),
        "dead_letter" => Ok(JobState::DeadLetter),
        other => Err(StorageError::Internal(format!(
            "unknown job state '{other}'"
        ))),
    }
}

pub const fn error_class_str(class: ErrorClass) -> &'static str {
    match class {
        ErrorClass::Transient => "transient",
        ErrorClass::Permanent => "permanent",
        ErrorClass::Configuration => "configuration",
        ErrorClass::Timeout => "timeout",
        ErrorClass::Cancelled => "cancelled",
    }
}

pub fn parse_error_class(raw: &str) -> Result<ErrorClass, StorageError> {
    match raw {
        "transient" => Ok(ErrorClass::Transient),
        "permanent" => Ok(ErrorClass::Permanent),
        "configuration" => Ok(ErrorClass::Configuration),
        "timeout" => Ok(ErrorClass::Timeout),
        "cancelled" => Ok(ErrorClass::Cancelled),
        other => Err(StorageError::Internal(format!(
            "unknown error_class '{other}'"
        ))),
    }
}

pub fn map_sqlx_err(err: sqlx::Error) -> StorageError {
    match err {
        sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::Io(_) => {
            StorageError::BackendUnavailable(err.to_string())
        }
        sqlx::Error::Database(db_err) => {
            let msg = db_err.to_string();
            if msg.contains("database is locked") || msg.contains("busy") {
                StorageError::BackendUnavailable(msg)
            } else {
                StorageError::Internal(msg)
            }
        }
        other => StorageError::Internal(other.to_string()),
    }
}

pub fn row_to_job(row: &SqliteRow) -> Result<JobRecord, StorageError> {
    let id_str: String = row.try_get("id").map_err(map_sqlx_err)?;
    let id = JobId::from_str(&id_str)
        .map_err(|err| StorageError::Internal(format!("invalid job id '{id_str}': {err}")))?;

    let state_str: String = row.try_get("state").map_err(map_sqlx_err)?;
    let state = parse_state(&state_str)?;

    let lease_token_str: Option<String> = row.try_get("lease_token").map_err(map_sqlx_err)?;
    let lease_token = lease_token_str
        .map(|s| {
            uuid::Uuid::parse_str(&s)
                .map(LeaseToken)
                .map_err(|err| StorageError::Internal(format!("invalid lease_token '{s}': {err}")))
        })
        .transpose()?;

    let spec_json: String = row.try_get("execution_spec_json").map_err(map_sqlx_err)?;
    let execution_spec: ExecutionSpec = serde_json::from_str(&spec_json).map_err(|err| {
        StorageError::Internal(format!("invalid execution_spec_json for job {id}: {err}"))
    })?;

    let err_class_str: Option<String> = row.try_get("error_class").map_err(map_sqlx_err)?;
    let error_class = err_class_str
        .as_deref()
        .map(parse_error_class)
        .transpose()?;

    let priority: i64 = row.try_get("priority").map_err(map_sqlx_err)?;
    let attempt: i64 = row.try_get("attempt").map_err(map_sqlx_err)?;
    let max_attempts: i64 = row.try_get("max_attempts").map_err(map_sqlx_err)?;
    let exit_code: Option<i64> = row.try_get("exit_code").map_err(map_sqlx_err)?;

    Ok(JobRecord {
        id,
        hook_id: row.try_get("hook_id").map_err(map_sqlx_err)?,
        state,
        priority: i32::try_from(priority).map_err(|_| {
            StorageError::Internal(format!("priority out of i32 range: {priority}"))
        })?,
        created_at: parse_ts(
            &row.try_get::<String, _>("created_at")
                .map_err(map_sqlx_err)?,
            "created_at",
        )?,
        available_at: parse_ts(
            &row.try_get::<String, _>("available_at")
                .map_err(map_sqlx_err)?,
            "available_at",
        )?,
        started_at: parse_opt_ts(
            row.try_get("started_at").map_err(map_sqlx_err)?,
            "started_at",
        )?,
        completed_at: parse_opt_ts(
            row.try_get("completed_at").map_err(map_sqlx_err)?,
            "completed_at",
        )?,
        updated_at: parse_ts(
            &row.try_get::<String, _>("updated_at")
                .map_err(map_sqlx_err)?,
            "updated_at",
        )?,
        attempt: u32::try_from(attempt)
            .map_err(|_| StorageError::Internal(format!("attempt out of u32 range: {attempt}")))?,
        max_attempts: u32::try_from(max_attempts).map_err(|_| {
            StorageError::Internal(format!("max_attempts out of u32 range: {max_attempts}"))
        })?,
        next_retry_at: parse_opt_ts(
            row.try_get("next_retry_at").map_err(map_sqlx_err)?,
            "next_retry_at",
        )?,
        idempotency_key: row.try_get("idempotency_key").map_err(map_sqlx_err)?,
        worker_id: row.try_get("worker_id").map_err(map_sqlx_err)?,
        lease_token,
        lease_expires_at: parse_opt_ts(
            row.try_get("lease_expires_at").map_err(map_sqlx_err)?,
            "lease_expires_at",
        )?,
        heartbeat_at: parse_opt_ts(
            row.try_get("heartbeat_at").map_err(map_sqlx_err)?,
            "heartbeat_at",
        )?,
        execution_spec,
        request_id: row.try_get("request_id").map_err(map_sqlx_err)?,
        request_method: row.try_get("request_method").map_err(map_sqlx_err)?,
        remote_addr: row.try_get("remote_addr").map_err(map_sqlx_err)?,
        exit_code: exit_code
            .map(|c| {
                i32::try_from(c)
                    .map_err(|_| StorageError::Internal(format!("exit_code out of range: {c}")))
            })
            .transpose()?,
        stdout_preview: row.try_get("stdout_preview").map_err(map_sqlx_err)?,
        stderr_preview: row.try_get("stderr_preview").map_err(map_sqlx_err)?,
        error_class,
        error_summary: row.try_get("error_summary").map_err(map_sqlx_err)?,
    })
}

pub fn row_to_attempt(row: &SqliteRow) -> Result<JobAttempt, StorageError> {
    let job_id_str: String = row.try_get("job_id").map_err(map_sqlx_err)?;
    let job_id = JobId::from_str(&job_id_str)
        .map_err(|err| StorageError::Internal(format!("invalid job_id '{job_id_str}': {err}")))?;
    let attempt: i64 = row.try_get("attempt").map_err(map_sqlx_err)?;
    let exit_code: Option<i64> = row.try_get("exit_code").map_err(map_sqlx_err)?;
    let err_class_str: Option<String> = row.try_get("error_class").map_err(map_sqlx_err)?;
    Ok(JobAttempt {
        job_id,
        attempt: u32::try_from(attempt)
            .map_err(|_| StorageError::Internal(format!("attempt out of range: {attempt}")))?,
        worker_id: row.try_get("worker_id").map_err(map_sqlx_err)?,
        started_at: parse_ts(
            &row.try_get::<String, _>("started_at")
                .map_err(map_sqlx_err)?,
            "attempt.started_at",
        )?,
        finished_at: parse_opt_ts(
            row.try_get("finished_at").map_err(map_sqlx_err)?,
            "attempt.finished_at",
        )?,
        exit_code: exit_code
            .map(|c| {
                i32::try_from(c)
                    .map_err(|_| StorageError::Internal(format!("exit_code out of range: {c}")))
            })
            .transpose()?,
        error_class: err_class_str
            .as_deref()
            .map(parse_error_class)
            .transpose()?,
        error_summary: row.try_get("error_summary").map_err(map_sqlx_err)?,
        stdout_preview: row.try_get("stdout_preview").map_err(map_sqlx_err)?,
        stderr_preview: row.try_get("stderr_preview").map_err(map_sqlx_err)?,
    })
}
