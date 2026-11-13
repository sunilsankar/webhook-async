//! Request context normalization, dot-path value extraction, and trigger-rule AST evaluation.

pub mod context;
pub mod error;
pub mod extract;
pub mod trigger;

use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
pub use context::{RequestContext, canonical_header_name};
pub use error::HookEngineError;
pub use extract::{extract_as_string, extract_value, lookup_dot_path};
pub use trigger::{
    TriggerOutcome, eval_match_rule_at, eval_rule_tree, eval_trigger, eval_trigger_at,
    mismatch_http_status,
};
use webhook_async_core::{ExecutionSpec, HookDefinition, TempFileSpec};

/// Builds an immutable [`ExecutionSpec`] snapshot from a matched [`HookDefinition`]
/// and normalized [`RequestContext`].
///
/// Upstream parameter-missing behavior:
/// - `pass-arguments-to-command`: missing parameter yields `""`.
/// - `pass-environment-to-command`: missing parameter is omitted.
/// - `pass-file-to-command`: missing parameter is omitted; `base64decode` decodes standard base64.
pub fn prepare_execution_spec(
    hook: &HookDefinition,
    ctx: &RequestContext,
    default_timeout_seconds: u64,
    max_output_bytes: usize,
    config_version: &str,
) -> Result<ExecutionSpec, HookEngineError> {
    let mut args = Vec::with_capacity(hook.pass_arguments_to_command.len());
    for spec in &hook.pass_arguments_to_command {
        match extract_as_string(ctx, spec) {
            Ok(val) => args.push(val),
            Err(err) if err.is_parameter_missing() => args.push(String::new()),
            Err(err) => return Err(err),
        }
    }

    let mut env = BTreeMap::new();
    for spec in &hook.pass_environment_to_command {
        match extract_as_string(ctx, spec) {
            Ok(val) => {
                env.insert(spec.resolved_env_name(false), val);
            }
            Err(err) if err.is_parameter_missing() => {}
            Err(err) => return Err(err),
        }
    }

    let mut temp_files = Vec::with_capacity(hook.pass_file_to_command.len());
    for spec in &hook.pass_file_to_command {
        match extract_as_string(ctx, spec) {
            Ok(raw) => {
                let content = if spec.base64_decode {
                    STANDARD.decode(raw.as_bytes()).map_err(|err| {
                        HookEngineError::Extraction(format!(
                            "failed to base64-decode file parameter '{}': {err}",
                            spec.name
                        ))
                    })?
                } else {
                    raw.into_bytes()
                };
                temp_files.push(TempFileSpec {
                    env_name: spec.resolved_env_name(true),
                    content,
                });
            }
            Err(err) if err.is_parameter_missing() => {}
            Err(err) => return Err(err),
        }
    }

    Ok(ExecutionSpec {
        hook_id: hook.id.clone(),
        executable: hook.execute_command.clone(),
        args,
        env,
        working_dir: hook.command_working_directory.clone(),
        temp_files,
        timeout_seconds: hook.timeout_seconds.unwrap_or(default_timeout_seconds),
        capture_output: hook.include_command_output_in_response,
        max_output_bytes,
        config_version: config_version.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use webhook_async_core::{
        HookDefinition, MatchKind, MatchRule, ParameterSpec, TriggerRule, ValueSource,
    };

    use super::{
        HookEngineError, RequestContext, canonical_header_name, eval_rule_tree, extract_as_string,
        mismatch_http_status,
    };

    fn param(source: ValueSource, name: &str) -> ParameterSpec {
        ParameterSpec {
            source,
            name: name.to_string(),
            env_name: None,
            base64_decode: false,
        }
    }

    #[test]
    fn canonical_header_and_literal_dot_key_precedence() {
        assert_eq!(canonical_header_name("x-hub-signature"), "X-Hub-Signature");
        let body = br#"{"a.b":"literal","a":{"b":"nested","c":42},"commits":[{"id":"abc"}]}"#;
        let mut ctx = RequestContext::new(
            "req-1",
            "POST",
            Some("127.0.0.1:9000".into()),
            body.to_vec(),
        );
        ctx.insert_header("x-hub-signature", "sha1=deadbeef");
        ctx.insert_query("branch", "main");
        ctx.parse_json_payload().expect("json payload");

        assert_eq!(
            extract_as_string(&ctx, &param(ValueSource::Header, "X-HUB-SIGNATURE")).unwrap(),
            "sha1=deadbeef"
        );
        assert_eq!(
            extract_as_string(&ctx, &param(ValueSource::Query, "branch")).unwrap(),
            "main"
        );
        assert_eq!(
            extract_as_string(&ctx, &param(ValueSource::Payload, "a.b")).unwrap(),
            "literal"
        );
        assert_eq!(
            extract_as_string(&ctx, &param(ValueSource::Payload, "a.c")).unwrap(),
            "42"
        );
        assert_eq!(
            extract_as_string(&ctx, &param(ValueSource::Payload, "commits.0.id")).unwrap(),
            "abc"
        );
        assert_eq!(
            extract_as_string(&ctx, &param(ValueSource::Request, "method")).unwrap(),
            "POST"
        );
        assert_eq!(
            extract_as_string(&ctx, &param(ValueSource::StringLiteral, "fixed")).unwrap(),
            "fixed"
        );
    }

    #[test]
    fn wraps_json_array_as_root_and_parses_embedded_json_parameters() {
        let mut arr_ctx = RequestContext::new(
            "req-2",
            "POST",
            None,
            br#"[{"event":"processed"}]"#.to_vec(),
        );
        arr_ctx.parse_json_payload().expect("array payload");
        assert_eq!(
            extract_as_string(&arr_ctx, &param(ValueSource::Payload, "root.0.event")).unwrap(),
            "processed"
        );

        let mut nested_ctx = RequestContext::new(
            "req-3",
            "POST",
            None,
            br#"{"payload":"{\"canon_url\":\"https://bitbucket.org\"}"}"#.to_vec(),
        );
        nested_ctx.parse_json_payload().expect("object payload");
        nested_ctx
            .parse_parameters_as_json(&[param(ValueSource::Payload, "payload")])
            .expect("parse nested json");
        assert_eq!(
            extract_as_string(
                &nested_ctx,
                &param(ValueSource::Payload, "payload.canon_url")
            )
            .unwrap(),
            "https://bitbucket.org"
        );

        let mut hdr_ctx = RequestContext::new("req-3b", "POST", None, Vec::new());
        hdr_ctx.insert_header("x-meta", r#"{"camelKey":"ok"}"#);
        hdr_ctx
            .parse_parameters_as_json(&[param(ValueSource::Header, "X-Meta")])
            .expect("parse json header");
        assert_eq!(
            extract_as_string(&hdr_ctx, &param(ValueSource::Header, "x-meta.camelKey")).unwrap(),
            "ok"
        );
    }

    #[test]
    fn empty_body_is_allowed_and_missing_node_returns_parameter_missing() {
        let mut ctx = RequestContext::new("req-4", "GET", None, Vec::new());
        ctx.parse_json_payload().expect("empty body ok");
        let err = extract_as_string(&ctx, &param(ValueSource::Payload, "missing")).unwrap_err();
        assert!(err.is_parameter_missing());
    }

    #[test]
    fn trigger_tree_error_swallowing_and_mismatch_status_fallback() {
        assert!(!eval_rule_tree(&TriggerRule::Empty, false, &|_| Ok(true)).unwrap());

        let sig_rule = TriggerRule::Or(vec![
            TriggerRule::Match(MatchRule {
                parameter: param(ValueSource::Header, "X-Sig"),
                kind: MatchKind::PayloadHmacSha1 {
                    secret: "bad".into(),
                },
            }),
            TriggerRule::Match(MatchRule {
                parameter: param(ValueSource::Payload, "ok"),
                kind: MatchKind::Value("yes".into()),
            }),
        ]);

        let eval = |m: &MatchRule| match &m.kind {
            MatchKind::PayloadHmacSha1 { .. } => {
                Err(HookEngineError::SignatureMismatch("bad hmac".into()))
            }
            MatchKind::Value(_) => Ok(true),
            _ => unreachable!(),
        };

        // Without soft failures, SignatureMismatch aborts Or.
        assert!(
            eval_rule_tree(&sig_rule, false, &eval)
                .unwrap_err()
                .is_signature_mismatch()
        );
        // With soft failures, Or proceeds to second branch.
        assert!(eval_rule_tree(&sig_rule, true, &eval).unwrap());

        // ParameterMissing is always swallowed by Or.
        let missing_or = TriggerRule::Or(vec![
            TriggerRule::Match(MatchRule {
                parameter: param(ValueSource::Payload, "missing"),
                kind: MatchKind::Value("x".into()),
            }),
            TriggerRule::Match(MatchRule {
                parameter: param(ValueSource::Payload, "present"),
                kind: MatchKind::Value("y".into()),
            }),
        ]);
        let eval_missing = |m: &MatchRule| {
            if m.parameter.name == "missing" {
                Err(HookEngineError::ParameterMissing {
                    value_source: ValueSource::Payload,
                    name: "missing".into(),
                })
            } else {
                Ok(true)
            }
        };
        assert!(eval_rule_tree(&missing_or, false, &eval_missing).unwrap());

        let mut hook = HookDefinition {
            id: "test".into(),
            execute_command: "/bin/true".into(),
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
            trigger_rule_mismatch_http_response_code: Some(999),
            trigger_signature_soft_failures: false,
            incoming_payload_content_type: None,
            success_http_response_code: None,
            http_methods: Vec::new(),
            async_mode: None,
            timeout_seconds: None,
            max_attempts: None,
        };
        assert_eq!(mismatch_http_status(&hook), 200);
        hook.trigger_rule_mismatch_http_response_code = Some(400);
        assert_eq!(mismatch_http_status(&hook), 400);
    }

    fn base_hook() -> HookDefinition {
        HookDefinition {
            id: "test".into(),
            execute_command: "/bin/echo".into(),
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
            async_mode: None,
            timeout_seconds: None,
            max_attempts: None,
        }
    }

    #[test]
    fn leaf_matchers_value_regex_hmac_and_ip_whitelist() {
        use chrono::{TimeZone, Utc};
        use hmac::{Hmac, KeyInit, Mac};
        use sha1::Sha1;
        use sha2::{Sha256, Sha512};

        let body = br#"{"ref":"refs/heads/master","num":7}"#;
        let mut ctx =
            RequestContext::new("req-hmac", "POST", Some("[::1]:9000".into()), body.to_vec());
        ctx.parse_json_payload().unwrap();

        let mut m1 = Hmac::<Sha1>::new_from_slice(b"mysecret").unwrap();
        m1.update(body);
        let sig1 = hex::encode(m1.finalize().into_bytes());

        let mut m256 = Hmac::<Sha256>::new_from_slice(b"mysecret").unwrap();
        m256.update(body);
        let sig256 = hex::encode(m256.finalize().into_bytes());

        let mut m512 = Hmac::<Sha512>::new_from_slice(b"mysecret").unwrap();
        m512.update(body);
        let sig512 = hex::encode(m512.finalize().into_bytes());

        ctx.insert_header("X-Hub-Signature", format!("sha1={sig1}"));
        ctx.insert_header(
            "X-Hub-Signature-256",
            format!("sha1=ignored, sha256={sig256}"),
        );
        ctx.insert_header("X-Hub-Signature-512", &sig512);

        let mut hook = base_hook();
        hook.trigger_rule = Some(TriggerRule::And(vec![
            TriggerRule::Match(MatchRule {
                parameter: param(ValueSource::Payload, "ref"),
                kind: MatchKind::Value("refs/heads/master".into()),
            }),
            TriggerRule::Match(MatchRule {
                parameter: param(ValueSource::Payload, "num"),
                kind: MatchKind::Regex(r"^\d+$".into()),
            }),
            TriggerRule::Match(MatchRule {
                parameter: param(ValueSource::Header, "X-Hub-Signature"),
                kind: MatchKind::PayloadHmacSha1 {
                    secret: "mysecret".into(),
                },
            }),
            TriggerRule::Match(MatchRule {
                parameter: param(ValueSource::Header, "X-Hub-Signature-256"),
                kind: MatchKind::PayloadHmacSha256 {
                    secret: "mysecret".into(),
                },
            }),
            TriggerRule::Match(MatchRule {
                parameter: param(ValueSource::Header, "X-Hub-Signature-512"),
                kind: MatchKind::PayloadHmacSha512 {
                    secret: "mysecret".into(),
                },
            }),
            TriggerRule::Match(MatchRule {
                parameter: param(ValueSource::Request, "remote-addr"),
                kind: MatchKind::IpWhitelist {
                    ip_range: "127.0.0.1/8 ::1".into(),
                },
            }),
        ]));

        assert_eq!(
            super::eval_trigger(&hook, &ctx).unwrap(),
            super::TriggerOutcome::Matched
        );

        // Wrong signature must error without leaking expected hex.
        ctx.insert_header("X-Hub-Signature", "sha1=0000");
        let err = super::eval_trigger(&hook, &ctx).unwrap_err();
        assert!(err.is_signature_mismatch());
        assert!(!err.to_string().contains(&sig1));

        // Scalr signature deterministic timestamp checks.
        let now = Utc.with_ymd_and_hms(2026, 11, 11, 12, 0, 0).unwrap();
        let date_ok = "Wed 11 Nov 2026 11:56:00 UTC";
        let mut smac = Hmac::<Sha1>::new_from_slice(b"scalr-key").unwrap();
        smac.update(body);
        smac.update(date_ok.as_bytes());
        let s_sig = hex::encode(smac.finalize().into_bytes());
        ctx.insert_header("X-Signature", &s_sig);
        ctx.insert_header("Date", date_ok);
        hook.trigger_rule = Some(TriggerRule::Match(MatchRule {
            parameter: param(ValueSource::Header, "X-Signature"),
            kind: MatchKind::ScalrSignature {
                secret: "scalr-key".into(),
            },
        }));
        assert_eq!(
            super::eval_trigger_at(&hook, &ctx, now).unwrap(),
            super::TriggerOutcome::Matched
        );

        // Expired timestamp (>300s) fails.
        let later = Utc.with_ymd_and_hms(2026, 11, 11, 12, 2, 0).unwrap();
        assert!(
            super::eval_trigger_at(&hook, &ctx, later)
                .unwrap_err()
                .is_signature_mismatch()
        );
    }

    #[tokio::test]
    async fn parses_form_xml_and_multipart_payloads_and_prepares_spec() {
        let mut form_ctx =
            RequestContext::new("req-f", "POST", None, b"a=1&a=2&b=hello%20world".to_vec());
        form_ctx.parse_form_payload().unwrap();
        assert_eq!(
            extract_as_string(&form_ctx, &param(ValueSource::Payload, "a")).unwrap(),
            "1"
        );
        assert_eq!(
            extract_as_string(&form_ctx, &param(ValueSource::Payload, "b")).unwrap(),
            "hello world"
        );

        let xml_body = br#"<app><users><user name="Jeff"/><user name="Bob"/></users><messages><message id="1">Hello!!</message></messages></app>"#;
        let mut xml_ctx = RequestContext::new("req-x", "POST", None, xml_body.to_vec());
        xml_ctx.parse_xml_payload().unwrap();
        assert_eq!(
            extract_as_string(
                &xml_ctx,
                &param(ValueSource::Payload, "app.users.user.0.-name")
            )
            .unwrap(),
            "Jeff"
        );
        assert_eq!(
            extract_as_string(
                &xml_ctx,
                &param(ValueSource::Payload, "app.messages.message.#text")
            )
            .unwrap(),
            "Hello!!"
        );

        let mp_body = concat!(
            "--BOUNDARY\r\n",
            "Content-Disposition: form-data; name=\"payload\"\r\n\r\n",
            "{\"event\":\"media.play\"}\r\n",
            "--BOUNDARY\r\n",
            "Content-Disposition: form-data; name=\"thumb\"; filename=\"t.json\"\r\n",
            "Content-Type: application/json\r\n\r\n",
            "{\"w\":120}\r\n",
            "--BOUNDARY--\r\n"
        );
        let mut mp_ctx = RequestContext::new("req-m", "POST", None, mp_body.as_bytes().to_vec());
        mp_ctx
            .parse_multipart_payload(
                "multipart/form-data; boundary=BOUNDARY",
                &[param(ValueSource::Payload, "payload")],
                1024 * 1024,
            )
            .await
            .unwrap();
        mp_ctx
            .parse_parameters_as_json(&[param(ValueSource::Payload, "payload")])
            .unwrap();
        assert_eq!(
            extract_as_string(&mp_ctx, &param(ValueSource::Payload, "payload.event")).unwrap(),
            "media.play"
        );
        assert_eq!(
            extract_as_string(&mp_ctx, &param(ValueSource::Payload, "thumb.w")).unwrap(),
            "120"
        );

        // Spec construction: missing arg -> "", missing env/file skipped, base64decode decoded.
        let mut hook = base_hook();
        hook.pass_arguments_to_command = vec![
            param(ValueSource::Payload, "payload.event"),
            param(ValueSource::Payload, "missing"),
        ];
        hook.pass_environment_to_command = vec![
            param(ValueSource::Payload, "payload.event"),
            param(ValueSource::Payload, "missing"),
        ];
        hook.pass_file_to_command = vec![ParameterSpec {
            source: ValueSource::StringLiteral,
            name: "aGVsbG8=".into(),
            env_name: Some("FILE_PATH".into()),
            base64_decode: true,
        }];
        let spec = super::prepare_execution_spec(&hook, &mp_ctx, 30, 65536, "sha256:abc").unwrap();
        assert_eq!(spec.args, vec!["media.play", ""]);
        assert_eq!(
            spec.env.get("HOOK_payload.event").map(String::as_str),
            Some("media.play")
        );
        assert!(!spec.env.contains_key("HOOK_missing"));
        assert_eq!(spec.temp_files.len(), 1);
        assert_eq!(spec.temp_files[0].env_name, "FILE_PATH");
        assert_eq!(spec.temp_files[0].content, b"hello");
    }
}
