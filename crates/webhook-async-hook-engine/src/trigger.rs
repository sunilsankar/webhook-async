use std::net::{IpAddr, SocketAddr};

use chrono::{DateTime, FixedOffset, NaiveDateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use ipnet::IpNet;
use regex::Regex;
use sha1::Sha1;
use sha2::{Sha256, Sha512};
use subtle::ConstantTimeEq;
use webhook_async_core::{HookDefinition, MatchKind, MatchRule, TriggerRule};

use crate::HookEngineError;
use crate::context::RequestContext;
use crate::extract::extract_as_string;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerOutcome {
    Matched,
    Mismatch {
        status_code: u16,
        body: &'static str,
    },
}

/// Evaluates `hook.trigger_rule` against `ctx` at the current UTC time.
/// Hooks without a `trigger_rule` always match.
pub fn eval_trigger(
    hook: &HookDefinition,
    ctx: &RequestContext,
) -> Result<TriggerOutcome, HookEngineError> {
    eval_trigger_at(hook, ctx, Utc::now())
}

/// Deterministic variant of [`eval_trigger`] using an explicit `now` timestamp (for
/// `scalr-signature` replay window checks).
pub fn eval_trigger_at(
    hook: &HookDefinition,
    ctx: &RequestContext,
    now: DateTime<Utc>,
) -> Result<TriggerOutcome, HookEngineError> {
    let Some(rule) = hook.trigger_rule.as_ref() else {
        return Ok(TriggerOutcome::Matched);
    };

    match eval_rule_tree(rule, hook.trigger_signature_soft_failures, &|m| {
        eval_match_rule_at(m, ctx, now)
    }) {
        Ok(true) => Ok(TriggerOutcome::Matched),
        Ok(false) => Ok(TriggerOutcome::Mismatch {
            status_code: mismatch_http_status(hook),
            body: "Hook rules were not satisfied.",
        }),
        Err(err) if err.is_parameter_missing() => Ok(TriggerOutcome::Mismatch {
            status_code: mismatch_http_status(hook),
            body: "Hook rules were not satisfied.",
        }),
        Err(err) => Err(err),
    }
}

/// Evaluates a single `MatchRule` leaf node against `ctx`.
pub fn eval_match_rule_at(
    rule: &MatchRule,
    ctx: &RequestContext,
    now: DateTime<Utc>,
) -> Result<bool, HookEngineError> {
    match &rule.kind {
        MatchKind::Value(expected) => {
            let actual = extract_as_string(ctx, &rule.parameter)?;
            Ok(bool::from(actual.as_bytes().ct_eq(expected.as_bytes())))
        }
        MatchKind::Regex(pattern) => {
            let re = Regex::new(pattern).map_err(|err| {
                HookEngineError::Evaluation(format!("invalid regex '{pattern}': {err}"))
            })?;
            let actual = extract_as_string(ctx, &rule.parameter)?;
            Ok(re.is_match(&actual))
        }
        MatchKind::PayloadHmacSha1 { secret } => verify_payload_hmac(
            check_nonempty_secret(secret).and_then(|s| {
                Hmac::<Sha1>::new_from_slice(s)
                    .map_err(|e| HookEngineError::Evaluation(e.to_string()))
            })?,
            ctx,
            &rule.parameter,
            "sha1=",
        ),
        MatchKind::PayloadHmacSha256 { secret } => verify_payload_hmac(
            check_nonempty_secret(secret).and_then(|s| {
                Hmac::<Sha256>::new_from_slice(s)
                    .map_err(|e| HookEngineError::Evaluation(e.to_string()))
            })?,
            ctx,
            &rule.parameter,
            "sha256=",
        ),
        MatchKind::PayloadHmacSha512 { secret } => verify_payload_hmac(
            check_nonempty_secret(secret).and_then(|s| {
                Hmac::<Sha512>::new_from_slice(s)
                    .map_err(|e| HookEngineError::Evaluation(e.to_string()))
            })?,
            ctx,
            &rule.parameter,
            "sha512=",
        ),
        MatchKind::IpWhitelist { ip_range } => eval_ip_whitelist(ctx, ip_range),
        MatchKind::ScalrSignature { secret } => eval_scalr_signature(ctx, secret, now),
    }
}

fn check_nonempty_secret(secret: &str) -> Result<&[u8], HookEngineError> {
    if secret.is_empty() {
        return Err(HookEngineError::SignatureMismatch(
            "signature validation failed, no secret defined".into(),
        ));
    }
    Ok(secret.as_bytes())
}

fn verify_payload_hmac(
    mut mac: impl Mac,
    ctx: &RequestContext,
    parameter: &webhook_async_core::ParameterSpec,
    prefix: &str,
) -> Result<bool, HookEngineError> {
    let header_val = extract_as_string(ctx, parameter)?;
    mac.update(&ctx.raw_body);
    let expected_hex = hex::encode(mac.finalize().into_bytes());

    let matched = if header_val.contains(',') {
        header_val
            .split(',')
            .map(str::trim)
            .filter_map(|part| part.strip_prefix(prefix))
            .fold(false, |acc, cand| {
                acc | bool::from(cand.as_bytes().ct_eq(expected_hex.as_bytes()))
            })
    } else {
        let cand = header_val.strip_prefix(prefix).unwrap_or(&header_val);
        bool::from(cand.as_bytes().ct_eq(expected_hex.as_bytes()))
    };

    if matched {
        Ok(true)
    } else {
        Err(HookEngineError::SignatureMismatch(
            "invalid payload signature".into(),
        ))
    }
}

fn eval_ip_whitelist(ctx: &RequestContext, ip_range: &str) -> Result<bool, HookEngineError> {
    let remote_raw = ctx.remote_addr.as_deref().ok_or_else(|| {
        HookEngineError::Evaluation("missing remote address for ip-whitelist rule".into())
    })?;
    let remote_ip = parse_remote_ip(remote_raw).ok_or_else(|| {
        HookEngineError::Evaluation(format!("invalid remote address '{remote_raw}'"))
    })?;

    let mut nets = Vec::new();
    for token in ip_range.split_whitespace() {
        let net = if token.contains('/') {
            token.parse::<IpNet>().map_err(|err| {
                HookEngineError::Evaluation(format!("invalid CIDR '{token}': {err}"))
            })?
        } else {
            let ip = token.parse::<IpAddr>().map_err(|err| {
                HookEngineError::Evaluation(format!("invalid IP '{token}': {err}"))
            })?;
            IpNet::from(ip)
        };
        nets.push(net);
    }

    if nets.is_empty() {
        return Err(HookEngineError::Evaluation(
            "ip-whitelist range cannot be empty".into(),
        ));
    }

    Ok(nets.iter().any(|net| net.contains(&remote_ip)))
}

fn parse_remote_ip(raw: &str) -> Option<IpAddr> {
    let trimmed = raw.trim();
    if let Ok(sock) = trimmed.parse::<SocketAddr>() {
        return Some(sock.ip());
    }
    let unbracketed = trimmed.trim_start_matches('[').trim_end_matches(']');
    unbracketed.parse::<IpAddr>().ok()
}

fn eval_scalr_signature(
    ctx: &RequestContext,
    secret: &str,
    now: DateTime<Utc>,
) -> Result<bool, HookEngineError> {
    if secret.is_empty() {
        return Err(HookEngineError::SignatureMismatch(
            "signature validation failed, no secret defined".into(),
        ));
    }
    let sig = match ctx.headers.get("X-Signature").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(false),
    };
    let date_str = match ctx.headers.get("Date").and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(false),
    };

    let mut mac = <Hmac<Sha1> as KeyInit>::new_from_slice(secret.as_bytes())
        .map_err(|err| HookEngineError::Evaluation(format!("invalid HMAC key: {err}")))?;
    mac.update(&ctx.raw_body);
    mac.update(date_str.as_bytes());
    let expected_hex = hex::encode(mac.finalize().into_bytes());

    if !bool::from(sig.as_bytes().ct_eq(expected_hex.as_bytes())) {
        return Err(HookEngineError::SignatureMismatch(
            "invalid scalr signature".into(),
        ));
    }

    let parsed = parse_scalr_date(date_str).ok_or_else(|| {
        HookEngineError::SignatureMismatch(format!("invalid scalr Date header '{date_str}'"))
    })?;
    if (now.timestamp() - parsed.timestamp()).abs() > 300 {
        return Err(HookEngineError::SignatureMismatch(
            "scalr signature timestamp outside 300s window".into(),
        ));
    }
    Ok(true)
}

/// Parses Scalr `Date` format (`Mon 02 Jan 2006 15:04:05 MST`, e.g. `UTC`, `GMT`, `+0000`, `PST`).
fn parse_scalr_date(s: &str) -> Option<DateTime<Utc>> {
    let trimmed = s.trim();
    let (datetime_part, tz_part) = trimmed.rsplit_once(' ')?;
    let naive = NaiveDateTime::parse_from_str(datetime_part, "%a %d %b %Y %H:%M:%S").ok()?;
    let offset_secs = match tz_part {
        "UTC" | "GMT" | "Z" => 0,
        "EST" => -5 * 3600,
        "EDT" => -4 * 3600,
        "CST" => -6 * 3600,
        "CDT" => -5 * 3600,
        "MST" => -7 * 3600,
        "MDT" => -6 * 3600,
        "PST" => -8 * 3600,
        "PDT" => -7 * 3600,
        other if (other.starts_with('+') || other.starts_with('-')) && other.len() == 5 => {
            let sign = if other.starts_with('-') { -1 } else { 1 };
            let hrs: i32 = other[1..3].parse().ok()?;
            let mins: i32 = other[3..5].parse().ok()?;
            sign * (hrs * 3600 + mins * 60)
        }
        _ => return None,
    };
    let offset = FixedOffset::east_opt(offset_secs)?;
    naive
        .and_local_timezone(offset)
        .single()
        .map(|dt| dt.with_timezone(&Utc))
}

/// Evaluates `TriggerRule` boolean combinators (`and`, `or`, `not`, `empty`) using
/// upstream `adnanh/webhook` v2.8.3 error-swallowing rules:
/// - `Or` always swallows `ParameterMissing` errors on a branch and continues.
/// - `Or` swallows `SignatureMismatch` errors only when `soft_signature_failures` is true.
/// - `Empty` rule evaluates to `false`.
pub fn eval_rule_tree<F>(
    rule: &TriggerRule,
    soft_signature_failures: bool,
    eval_match: &F,
) -> Result<bool, HookEngineError>
where
    F: Fn(&MatchRule) -> Result<bool, HookEngineError>,
{
    match rule {
        TriggerRule::Empty => Ok(false),
        TriggerRule::Match(m) => eval_match(m),
        TriggerRule::Not(inner) => Ok(!eval_rule_tree(inner, soft_signature_failures, eval_match)?),
        TriggerRule::And(items) => {
            for item in items {
                if !eval_rule_tree(item, soft_signature_failures, eval_match)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        TriggerRule::Or(items) => {
            for item in items {
                match eval_rule_tree(item, soft_signature_failures, eval_match) {
                    Ok(true) => return Ok(true),
                    Ok(false) => {}
                    Err(err) if err.is_parameter_missing() => {}
                    Err(err) if soft_signature_failures && err.is_signature_mismatch() => {}
                    Err(err) => return Err(err),
                }
            }
            Ok(false)
        }
    }
}

/// Resolves the HTTP mismatch status code matching upstream `webhook.go`:
/// invalid / unrecognized HTTP status codes fall back to 200.
#[must_use]
pub fn mismatch_http_status(hook: &HookDefinition) -> u16 {
    match hook.trigger_rule_mismatch_http_response_code {
        Some(code) if (100..=599).contains(&code) && standard_status_known(code) => code,
        _ => 200,
    }
}

const fn standard_status_known(code: u16) -> bool {
    matches!(
        code,
        100..=103
            | 200..=208
            | 226
            | 300..=308
            | 400..=418
            | 421..=426
            | 428
            | 429
            | 431
            | 451
            | 500..=511
    )
}
