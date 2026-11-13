use std::collections::HashSet;
use std::path::{Path, PathBuf};

use webhook_async_core::{
    HookDefinition, MatchKind, MatchRule, ParameterSpec, ResponseHeader, TriggerRule, ValueSource,
};

use crate::CompatError;
use crate::raw::{RawArgument, RawHook, RawMatchRule, RawRules};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookFileFormat {
    Auto,
    Json,
    Yaml,
}

pub fn parse_hooks_json(input: &str) -> Result<Vec<HookDefinition>, CompatError> {
    let raw: Vec<RawHook> = serde_json::from_str(input)?;
    translate_hooks(raw)
}

pub fn parse_hooks_yaml(input: &str) -> Result<Vec<HookDefinition>, CompatError> {
    let raw: Vec<RawHook> = yaml_serde::from_str(input)?;
    translate_hooks(raw)
}

pub fn parse_hooks_str(
    input: &str,
    format: HookFileFormat,
) -> Result<Vec<HookDefinition>, CompatError> {
    match format {
        HookFileFormat::Json => parse_hooks_json(input),
        HookFileFormat::Yaml => parse_hooks_yaml(input),
        HookFileFormat::Auto => {
            if input.trim_start().starts_with('[') {
                parse_hooks_json(input)
            } else {
                parse_hooks_yaml(input)
            }
        }
    }
}

pub fn parse_hooks_file_templated(
    path: &Path,
    is_template: bool,
) -> Result<Vec<HookDefinition>, CompatError> {
    let mut content = std::fs::read_to_string(path).map_err(|source| CompatError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if is_template {
        content = crate::template::render_template(&content)?;
    }
    let format = match path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("json") => HookFileFormat::Json,
        Some("yaml" | "yml") => HookFileFormat::Yaml,
        _ => HookFileFormat::Auto,
    };
    parse_hooks_str(&content, format)
}

pub fn parse_hooks_files_templated(
    paths: &[PathBuf],
    is_template: bool,
) -> Result<Vec<HookDefinition>, CompatError> {
    let mut seen_ids = HashSet::new();
    let mut all = Vec::new();
    for path in paths {
        for hook in parse_hooks_file_templated(path, is_template)? {
            if !seen_ids.insert(hook.id.clone()) {
                return Err(CompatError::DuplicateHookId { id: hook.id });
            }
            all.push(hook);
        }
    }
    Ok(all)
}

pub fn parse_hooks_file(path: &Path) -> Result<Vec<HookDefinition>, CompatError> {
    parse_hooks_file_templated(path, false)
}

pub fn parse_hooks_files(paths: &[PathBuf]) -> Result<Vec<HookDefinition>, CompatError> {
    parse_hooks_files_templated(paths, false)
}

fn translate_hooks(raw_hooks: Vec<RawHook>) -> Result<Vec<HookDefinition>, CompatError> {
    let mut seen_ids = HashSet::new();
    let mut out = Vec::with_capacity(raw_hooks.len());
    for raw in raw_hooks {
        let hook = translate_hook(raw)?;
        if !seen_ids.insert(hook.id.clone()) {
            return Err(CompatError::DuplicateHookId { id: hook.id });
        }
        out.push(hook);
    }
    Ok(out)
}

fn translate_hook(raw: RawHook) -> Result<HookDefinition, CompatError> {
    let hook_id = raw.id.clone();

    let parse_parameters_as_json = raw
        .parse_parameters_as_json
        .into_iter()
        .map(|arg| {
            let spec = translate_argument(&hook_id, arg)?;
            if !matches!(
                spec.source,
                ValueSource::Header | ValueSource::Payload | ValueSource::Query
            ) {
                return Err(CompatError::InvalidHook {
                    id: hook_id.clone(),
                    message: format!(
                        "parse-parameters-as-json does not support source '{}'",
                        spec.source.as_str()
                    ),
                });
            }
            Ok(spec)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut http_methods = Vec::with_capacity(raw.http_methods.len());
    for m in raw.http_methods {
        let trimmed = m.trim().to_ascii_uppercase();
        if trimmed.is_empty() {
            return Err(CompatError::InvalidHook {
                id: hook_id,
                message: "http-methods entries must not be empty".into(),
            });
        }
        http_methods.push(trimmed);
    }

    let hook = HookDefinition {
        id: raw.id,
        execute_command: raw.execute_command,
        command_working_directory: raw.command_working_directory,
        response_message: raw.response_message,
        response_headers: raw
            .response_headers
            .into_iter()
            .map(|h| ResponseHeader {
                name: h.name,
                value: h.value,
            })
            .collect(),
        include_command_output_in_response: raw.include_command_output_in_response,
        include_command_output_in_response_on_error: raw
            .include_command_output_in_response_on_error,
        pass_environment_to_command: raw
            .pass_environment_to_command
            .into_iter()
            .map(|a| translate_argument(&hook_id, a))
            .collect::<Result<Vec<_>, _>>()?,
        pass_arguments_to_command: raw
            .pass_arguments_to_command
            .into_iter()
            .map(|a| translate_argument(&hook_id, a))
            .collect::<Result<Vec<_>, _>>()?,
        pass_file_to_command: raw
            .pass_file_to_command
            .into_iter()
            .map(|a| translate_argument(&hook_id, a))
            .collect::<Result<Vec<_>, _>>()?,
        parse_parameters_as_json,
        trigger_rule: raw
            .trigger_rule
            .map(|rule| translate_rule(&hook_id, rule))
            .transpose()?,
        trigger_rule_mismatch_http_response_code: raw.trigger_rule_mismatch_http_response_code,
        trigger_signature_soft_failures: raw.trigger_signature_soft_failures,
        incoming_payload_content_type: raw.incoming_payload_content_type,
        success_http_response_code: raw.success_http_response_code,
        http_methods,
        async_mode: raw.async_mode,
        timeout_seconds: raw.timeout_seconds,
        max_attempts: raw.max_attempts,
    };

    hook.validate()?;
    Ok(hook)
}

fn translate_argument(hook_id: &str, arg: RawArgument) -> Result<ParameterSpec, CompatError> {
    let source = match arg.source.as_str() {
        "header" => ValueSource::Header,
        "url" | "query" => ValueSource::Query,
        "payload" => ValueSource::Payload,
        "raw-request-body" => ValueSource::RawRequestBody,
        "request" => ValueSource::Request,
        "string" => ValueSource::StringLiteral,
        "entire-payload" => ValueSource::EntirePayload,
        "entire-headers" => ValueSource::EntireHeaders,
        "entire-query" => ValueSource::EntireQuery,
        other => {
            return Err(CompatError::InvalidHook {
                id: hook_id.to_string(),
                message: format!("unsupported argument source '{other}'"),
            });
        }
    };

    let requires_name = matches!(
        source,
        ValueSource::Header | ValueSource::Query | ValueSource::Payload | ValueSource::Request
    );
    let name = arg.name.unwrap_or_default();
    if requires_name && name.is_empty() {
        return Err(CompatError::InvalidHook {
            id: hook_id.to_string(),
            message: format!(
                "argument source '{}' requires a non-empty 'name'",
                arg.source
            ),
        });
    }

    if source == ValueSource::Request
        && !matches!(name.to_ascii_lowercase().as_str(), "remote-addr" | "method")
    {
        return Err(CompatError::InvalidHook {
            id: hook_id.to_string(),
            message: format!(
                "unsupported 'request' argument name '{name}' (expected 'remote-addr' or 'method')"
            ),
        });
    }

    Ok(ParameterSpec {
        source,
        name,
        env_name: arg.envname,
        base64_decode: arg.base64decode.unwrap_or(false),
    })
}

fn translate_rule(hook_id: &str, raw: RawRules) -> Result<TriggerRule, CompatError> {
    let operator_count = usize::from(raw.and.is_some())
        + usize::from(raw.or.is_some())
        + usize::from(raw.not.is_some())
        + usize::from(raw.match_rule.is_some());

    if operator_count > 1 {
        return Err(CompatError::InvalidHook {
            id: hook_id.to_string(),
            message: "trigger-rule node must specify at most one of 'and', 'or', 'not', or 'match'"
                .into(),
        });
    }

    if let Some(items) = raw.and {
        return Ok(TriggerRule::And(
            items
                .into_iter()
                .map(|r| translate_rule(hook_id, r))
                .collect::<Result<Vec<_>, _>>()?,
        ));
    }
    if let Some(items) = raw.or {
        return Ok(TriggerRule::Or(
            items
                .into_iter()
                .map(|r| translate_rule(hook_id, r))
                .collect::<Result<Vec<_>, _>>()?,
        ));
    }
    if let Some(inner) = raw.not {
        return Ok(TriggerRule::Not(Box::new(translate_rule(hook_id, *inner)?)));
    }
    if let Some(m) = raw.match_rule {
        return Ok(TriggerRule::Match(translate_match_rule(hook_id, m)?));
    }

    Ok(TriggerRule::Empty)
}

fn translate_match_rule(hook_id: &str, m: RawMatchRule) -> Result<MatchRule, CompatError> {
    let require_param = |param: Option<RawArgument>| -> Result<ParameterSpec, CompatError> {
        let raw_param = param.ok_or_else(|| CompatError::InvalidHook {
            id: hook_id.to_string(),
            message: format!("match rule '{}' requires 'parameter'", m.rule_type),
        })?;
        translate_argument(hook_id, raw_param)
    };

    let require_secret = |secret: Option<String>| -> Result<String, CompatError> {
        match secret {
            Some(s) if !s.is_empty() => Ok(s),
            Some(_) => Err(CompatError::InvalidHook {
                id: hook_id.to_string(),
                message: format!("match rule '{}' requires a non-empty 'secret'", m.rule_type),
            }),
            None => Err(CompatError::InvalidHook {
                id: hook_id.to_string(),
                message: format!("match rule '{}' requires 'secret'", m.rule_type),
            }),
        }
    };

    let (parameter, kind) = match m.rule_type.as_str() {
        "value" => {
            let value = m.value.ok_or_else(|| CompatError::InvalidHook {
                id: hook_id.to_string(),
                message: "match rule 'value' requires 'value'".into(),
            })?;
            (require_param(m.parameter)?, MatchKind::Value(value))
        }
        "regex" => {
            let regex = m.regex.ok_or_else(|| CompatError::InvalidHook {
                id: hook_id.to_string(),
                message: "match rule 'regex' requires 'regex'".into(),
            })?;
            (require_param(m.parameter)?, MatchKind::Regex(regex))
        }
        "payload-hmac-sha1" | "payload-hash-sha1" => (
            require_param(m.parameter)?,
            MatchKind::PayloadHmacSha1 {
                secret: require_secret(m.secret)?,
            },
        ),
        "payload-hmac-sha256" | "payload-hash-sha256" => (
            require_param(m.parameter)?,
            MatchKind::PayloadHmacSha256 {
                secret: require_secret(m.secret)?,
            },
        ),
        "payload-hmac-sha512" | "payload-hash-sha512" => (
            require_param(m.parameter)?,
            MatchKind::PayloadHmacSha512 {
                secret: require_secret(m.secret)?,
            },
        ),
        "ip-whitelist" => {
            let ip_range = m.ip_range.filter(|s| !s.trim().is_empty()).ok_or_else(|| {
                CompatError::InvalidHook {
                    id: hook_id.to_string(),
                    message: "match rule 'ip-whitelist' requires a non-empty 'ip-range'".into(),
                }
            })?;
            let param = match m.parameter {
                Some(p) => translate_argument(hook_id, p)?,
                None => ParameterSpec {
                    source: ValueSource::Request,
                    name: "remote-addr".into(),
                    env_name: None,
                    base64_decode: false,
                },
            };
            (param, MatchKind::IpWhitelist { ip_range })
        }
        "scalr-signature" => {
            let param = match m.parameter {
                Some(p) => translate_argument(hook_id, p)?,
                None => ParameterSpec {
                    source: ValueSource::Header,
                    name: "X-Signature".into(),
                    env_name: None,
                    base64_decode: false,
                },
            };
            (
                param,
                MatchKind::ScalrSignature {
                    secret: require_secret(m.secret)?,
                },
            )
        }
        other => {
            return Err(CompatError::InvalidHook {
                id: hook_id.to_string(),
                message: format!("unsupported match rule type '{other}'"),
            });
        }
    };

    Ok(MatchRule { parameter, kind })
}

#[cfg(test)]
mod tests {
    use webhook_async_core::{MatchKind, TriggerRule, ValueSource};

    use super::{
        HookFileFormat, parse_hooks_file_templated, parse_hooks_json, parse_hooks_str,
        parse_hooks_yaml,
    };

    #[test]
    fn parses_upstream_json_hook_features_and_async_extension() {
        let json = r#"[
          {
            "id": "github",
            "execute-command": "/usr/local/bin/deploy.sh",
            "command-working-directory": "/srv/app",
            "http-methods": ["Post "],
            "include-command-output-in-response": true,
            "include-command-output-in-response-on-error": false,
            "response-message": "ok",
            "response-headers": [{"name": "X-Custom", "value": "1"}],
            "success-http-response-code": 201,
            "trigger-rule-mismatch-http-response-code": 400,
            "trigger-signature-soft-failures": true,
            "incoming-payload-content-type": "application/json",
            "pass-environment-to-command": [
              {"source": "payload", "name": "head_commit.timestamp"},
              {"source": "query", "name": "branch", "envname": "TARGET_BRANCH"}
            ],
            "pass-arguments-to-command": [
              {"source": "raw-request-body"},
              {"source": "string", "name": "literal-arg"}
            ],
            "pass-file-to-command": [
              {"source": "payload", "name": "cert", "base64decode": true}
            ],
            "parse-parameters-as-json": [
              {"source": "payload", "name": "payload"}
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
                  "or": [
                    {"match": {"type": "ip-whitelist", "ip-range": "127.0.0.1/32"}},
                    {"not": {"match": {"type": "scalr-signature", "secret": "scalr"}}}
                  ]
                }
              ]
            }
          },
          {
            "id": "async-job",
            "execute-command": "/bin/true",
            "async": true
          }
        ]"#;

        let hooks = parse_hooks_json(json).expect("valid upstream hooks");
        assert_eq!(hooks.len(), 2);
        assert_eq!(hooks[0].http_methods, vec!["POST"]);
        assert_eq!(hooks[0].async_mode, None);
        assert_eq!(hooks[1].async_mode, Some(true));
        assert_eq!(
            hooks[0].pass_environment_to_command[1].source,
            ValueSource::Query
        );
        assert!(hooks[0].pass_file_to_command[0].base64_decode);
        assert!(matches!(hooks[0].trigger_rule, Some(TriggerRule::And(_))));
    }

    #[test]
    fn parses_yaml_hooks_and_auto_detects_format() {
        let yaml = r#"
- id: redeploy
  execute-command: /srv/redeploy.sh
  async: false
  trigger-rule:
    match:
      type: value
      value: refs/heads/main
      parameter:
        source: payload
        name: ref
"#;
        let hooks = parse_hooks_yaml(yaml).expect("yaml hooks");
        assert_eq!(hooks[0].id, "redeploy");
        assert_eq!(hooks[0].async_mode, Some(false));
        assert!(matches!(
            hooks[0].trigger_rule,
            Some(TriggerRule::Match(ref m)) if matches!(m.kind, MatchKind::Value(ref v) if v == "refs/heads/main")
        ));

        let auto = parse_hooks_str(yaml, HookFileFormat::Auto).expect("auto yaml");
        assert_eq!(auto, hooks);
    }

    #[test]
    fn rejects_unknown_fields_and_invalid_rules() {
        let unknown_field = r#"[{"id":"a","execute-command":"/bin/true","unknown-flag":1}]"#;
        assert!(parse_hooks_json(unknown_field).is_err());

        let multi_op = r#"[{
            "id": "a",
            "execute-command": "/bin/true",
            "trigger-rule": {"and": [], "or": []}
        }]"#;
        assert!(parse_hooks_json(multi_op).is_err());

        let bad_json_source = r#"[{
            "id": "a",
            "execute-command": "/bin/true",
            "parse-parameters-as-json": [{"source": "raw-request-body"}]
        }]"#;
        assert!(parse_hooks_json(bad_json_source).is_err());

        let empty_secret = r#"[{
            "id": "a",
            "execute-command": "/bin/true",
            "trigger-rule": {
                "match": {
                    "type": "payload-hmac-sha256",
                    "secret": "",
                    "parameter": {"source": "header", "name": "X-Sig"}
                }
            }
        }]"#;
        assert!(parse_hooks_json(empty_secret).is_err());

        let dup_ids = r#"[
            {"id":"dup","execute-command":"/bin/true"},
            {"id":"dup","execute-command":"/bin/false"}
        ]"#;
        assert!(parse_hooks_json(dup_ids).is_err());
    }

    #[test]
    fn parses_templated_hook_file() {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // SAFETY: test-only environment variable
        unsafe { std::env::set_var("APP_DEPLOY_SECRET", "super-deploy-secret-777") };

        let mut file = NamedTempFile::new().unwrap();
        let content = r#"[
            {
                "id": "templated-hook",
                "execute-command": "/bin/echo",
                "trigger-rule": {
                    "match": {
                        "type": "value",
                        "value": {{ getenv("APP_DEPLOY_SECRET") | js }},
                        "parameter": {
                            "source": "header",
                            "name": "X-Secret"
                        }
                    }
                }
            }
        ]"#;
        file.write_all(content.as_bytes()).unwrap();

        let hooks = parse_hooks_file_templated(file.path(), true).expect("parse templated");
        assert_eq!(hooks.len(), 1);
        assert_eq!(hooks[0].id, "templated-hook");
        assert!(matches!(
            hooks[0].trigger_rule,
            Some(TriggerRule::Match(ref m)) if matches!(m.kind, MatchKind::Value(ref v) if v == "super-deploy-secret-777")
        ));
    }
}
