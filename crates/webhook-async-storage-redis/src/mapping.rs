use std::collections::HashMap;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use webhook_async_config::redact_url_credentials;
use webhook_async_core::{ErrorClass, ExecutionSpec, JobId, JobRecord, JobState, LeaseToken};
use webhook_async_storage::StorageError;

#[derive(Debug, Serialize, Deserialize)]
struct JobStaticFields {
    id: JobId,
    hook_id: String,
    priority: i32,
    created_at: DateTime<Utc>,
    idempotency_key: Option<String>,
    execution_spec: ExecutionSpec,
    request_id: String,
    request_method: String,
    remote_addr: Option<String>,
}

pub(crate) fn fmt_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

pub(crate) fn parse_ts(raw: &str) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| StorageError::Internal(format!("invalid stored timestamp '{raw}': {err}")))
}

pub(crate) fn ts_millis(ts: DateTime<Utc>) -> i64 {
    ts.timestamp_millis()
}

pub(crate) fn error_class_str(class: ErrorClass) -> &'static str {
    match class {
        ErrorClass::Transient => "transient",
        ErrorClass::Permanent => "permanent",
        ErrorClass::Configuration => "configuration",
        ErrorClass::Timeout => "timeout",
        ErrorClass::Cancelled => "cancelled",
    }
}

pub(crate) fn parse_error_class(raw: &str) -> Result<ErrorClass, StorageError> {
    match raw {
        "transient" => Ok(ErrorClass::Transient),
        "permanent" => Ok(ErrorClass::Permanent),
        "configuration" => Ok(ErrorClass::Configuration),
        "timeout" => Ok(ErrorClass::Timeout),
        "cancelled" => Ok(ErrorClass::Cancelled),
        other => Err(StorageError::Internal(format!(
            "unknown stored error_class '{other}'"
        ))),
    }
}

pub(crate) fn parse_state(raw: &str) -> Result<JobState, StorageError> {
    JobState::ALL
        .iter()
        .copied()
        .find(|s| s.as_str() == raw)
        .ok_or_else(|| StorageError::Internal(format!("unknown stored job state '{raw}'")))
}

pub(crate) fn job_to_hash_fields(job: &JobRecord) -> Result<Vec<(String, String)>, StorageError> {
    let static_fields = JobStaticFields {
        id: job.id,
        hook_id: job.hook_id.clone(),
        priority: job.priority,
        created_at: job.created_at,
        idempotency_key: job.idempotency_key.clone(),
        execution_spec: job.execution_spec.clone(),
        request_id: job.request_id.clone(),
        request_method: job.request_method.clone(),
        remote_addr: job.remote_addr.clone(),
    };
    let static_json = serde_json::to_string(&static_fields).map_err(|err| {
        StorageError::Internal(format!("failed to serialize job snapshot: {err}"))
    })?;

    let mut fields = vec![
        ("static_json".into(), static_json),
        ("id".into(), job.id.to_string()),
        ("hook_id".into(), job.hook_id.clone()),
        ("priority".into(), job.priority.to_string()),
        ("created_at".into(), fmt_ts(job.created_at)),
        (
            "created_at_ms".into(),
            ts_millis(job.created_at).to_string(),
        ),
        ("state".into(), job.state.as_str().to_string()),
        ("available_at".into(), fmt_ts(job.available_at)),
        ("updated_at".into(), fmt_ts(job.updated_at)),
        ("attempt".into(), job.attempt.to_string()),
        ("max_attempts".into(), job.max_attempts.to_string()),
    ];

    if let Some(ts) = job.started_at {
        fields.push(("started_at".into(), fmt_ts(ts)));
    }
    if let Some(ts) = job.completed_at {
        fields.push(("completed_at".into(), fmt_ts(ts)));
    }
    if let Some(ts) = job.next_retry_at {
        fields.push(("next_retry_at".into(), fmt_ts(ts)));
    }
    if let Some(w) = &job.worker_id {
        fields.push(("worker_id".into(), w.clone()));
    }
    if let Some(tok) = job.lease_token {
        fields.push(("lease_token".into(), tok.0.to_string()));
    }
    if let Some(ts) = job.lease_expires_at {
        fields.push(("lease_expires_at".into(), fmt_ts(ts)));
    }
    if let Some(ts) = job.heartbeat_at {
        fields.push(("heartbeat_at".into(), fmt_ts(ts)));
    }
    if let Some(code) = job.exit_code {
        fields.push(("exit_code".into(), code.to_string()));
    }
    if let Some(out) = &job.stdout_preview {
        fields.push(("stdout_preview".into(), out.clone()));
    }
    if let Some(err) = &job.stderr_preview {
        fields.push(("stderr_preview".into(), err.clone()));
    }
    if let Some(cls) = job.error_class {
        fields.push(("error_class".into(), error_class_str(cls).to_string()));
    }
    if let Some(sum) = &job.error_summary {
        fields.push(("error_summary".into(), sum.clone()));
    }
    Ok(fields)
}

pub(crate) fn hash_pairs_to_job(pairs: Vec<String>) -> Result<JobRecord, StorageError> {
    if !pairs.len().is_multiple_of(2) {
        return Err(StorageError::Internal(
            "corrupted HGETALL pair count returned from Redis/Valkey".into(),
        ));
    }
    let mut map = HashMap::with_capacity(pairs.len() / 2);
    let mut iter = pairs.into_iter();
    while let (Some(k), Some(v)) = (iter.next(), iter.next()) {
        map.insert(k, v);
    }
    hash_map_to_job(&map)
}

pub(crate) fn hash_map_to_job(map: &HashMap<String, String>) -> Result<JobRecord, StorageError> {
    let static_json = map
        .get("static_json")
        .ok_or_else(|| StorageError::Internal("missing static_json in job hash".into()))?;
    let static_fields: JobStaticFields = serde_json::from_str(static_json)
        .map_err(|err| StorageError::Internal(format!("corrupted static_json: {err}")))?;

    let state = parse_state(req_field(map, "state")?)?;
    let available_at = parse_ts(req_field(map, "available_at")?)?;
    let updated_at = parse_ts(req_field(map, "updated_at")?)?;
    let attempt: u32 = req_field(map, "attempt")?
        .parse()
        .map_err(|err| StorageError::Internal(format!("invalid attempt count: {err}")))?;
    let max_attempts: u32 = req_field(map, "max_attempts")?
        .parse()
        .map_err(|err| StorageError::Internal(format!("invalid max_attempts: {err}")))?;

    let started_at = map.get("started_at").map(|s| parse_ts(s)).transpose()?;
    let completed_at = map.get("completed_at").map(|s| parse_ts(s)).transpose()?;
    let next_retry_at = map.get("next_retry_at").map(|s| parse_ts(s)).transpose()?;
    let lease_expires_at = map
        .get("lease_expires_at")
        .map(|s| parse_ts(s))
        .transpose()?;
    let heartbeat_at = map.get("heartbeat_at").map(|s| parse_ts(s)).transpose()?;

    let lease_token = map
        .get("lease_token")
        .map(|s| uuid::Uuid::parse_str(s).map(LeaseToken))
        .transpose()
        .map_err(|err| StorageError::Internal(format!("invalid stored lease_token: {err}")))?;

    let exit_code = map
        .get("exit_code")
        .map(|s| s.parse::<i32>())
        .transpose()
        .map_err(|err| StorageError::Internal(format!("invalid stored exit_code: {err}")))?;

    let error_class = map
        .get("error_class")
        .map(|s| parse_error_class(s))
        .transpose()?;

    Ok(JobRecord {
        id: static_fields.id,
        hook_id: static_fields.hook_id,
        state,
        priority: static_fields.priority,
        created_at: static_fields.created_at,
        available_at,
        started_at,
        completed_at,
        updated_at,
        attempt,
        max_attempts,
        next_retry_at,
        idempotency_key: static_fields.idempotency_key,
        worker_id: map.get("worker_id").cloned(),
        lease_token,
        lease_expires_at,
        heartbeat_at,
        execution_spec: static_fields.execution_spec,
        request_id: static_fields.request_id,
        request_method: static_fields.request_method,
        remote_addr: static_fields.remote_addr,
        exit_code,
        stdout_preview: map.get("stdout_preview").cloned(),
        stderr_preview: map.get("stderr_preview").cloned(),
        error_class,
        error_summary: map.get("error_summary").cloned(),
    })
}

fn req_field<'a>(map: &'a HashMap<String, String>, key: &str) -> Result<&'a str, StorageError> {
    map.get(key)
        .map(String::as_str)
        .ok_or_else(|| StorageError::Internal(format!("missing '{key}' in job hash")))
}

pub(crate) fn redis_err_to_storage(err: redis::RedisError, url: &str) -> StorageError {
    let safe_url = redact_url_credentials(url);
    if err.is_io_error()
        || err.is_connection_dropped()
        || err.is_connection_refusal()
        || err.is_timeout()
    {
        StorageError::BackendUnavailable(format!("RESP backend '{safe_url}' unavailable: {err}"))
    } else {
        StorageError::Internal(format!("RESP backend '{safe_url}' error: {err}"))
    }
}
