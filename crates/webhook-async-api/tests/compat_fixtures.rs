//! End-to-end compatibility regression suite adapted from `adnanh/webhook` v2.8.3
//! (`test/hooks.json.tmpl`, `test/hooks.yaml.tmpl`, and `webhook_test.go`).

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use hmac::{Hmac, KeyInit, Mac};
use http_body_util::BodyExt;
use sha1::Sha1;
use sha2::Sha256;
use tempfile::TempDir;
use tower::ServiceExt;
use webhook_async_api::{ApiState, build_router};
use webhook_async_compat::parse_hooks_json;
use webhook_async_config::AppConfig;
use webhook_async_core::GlobalExecutionMode;

fn create_dump_script(dir: &Path, name: &str, body: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, format!("#!/bin/sh\n{body}\n")).expect("write test script");
    let mut perms = fs::metadata(&path).expect("stat").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).expect("chmod");
    path
}

fn hmac_sha1_hex(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha1>::new_from_slice(secret.as_bytes()).expect("hmac key");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

fn hmac_sha256_hex(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac key");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

async fn send(
    app: &axum::Router,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> (StatusCode, BTreeMap<String, String>, String) {
    let mut builder = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let req = builder
        .body(Body::from(body.to_vec()))
        .expect("build request");
    let resp = app.clone().oneshot(req).await.expect("oneshot");
    let status = resp.status();
    let resp_headers = resp
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_owned(), v.to_str().unwrap_or("").to_owned()))
        .collect();
    let bytes = resp.into_body().collect().await.expect("body").to_bytes();
    (
        status,
        resp_headers,
        String::from_utf8_lossy(&bytes).into_owned(),
    )
}

#[tokio::test]
async fn upstream_v283_github_bitbucket_xml_sendgrid_plex_and_output_fixtures() {
    let dir = TempDir::new().expect("tempdir");
    let dump = create_dump_script(
        dir.path(),
        "echo.sh",
        r#"
if [ "$1" = "exit=1" ]; then
  echo "failing-stdout"
  echo "failing-stderr" >&2
  exit 1
fi
printf "args:"
for a in "$@"; do printf "|%s" "$a"; done
printf "\n"
if [ -n "${HOOK_head_commit_ts:-}" ]; then
  printf "env_ts=%s\n" "$HOOK_head_commit_ts"
fi
if [ -n "${PAYLOAD_FILE:-}" ]; then
  printf "file=%s\n" "$(cat "$PAYLOAD_FILE")"
  printf "filepath=%s\n" "$PAYLOAD_FILE"
fi
"#,
    );
    let dump_str = dump.to_string_lossy();
    let work_dir = dir.path().to_string_lossy();

    let hooks_json = serde_json::json!([
        {
            "id": "github",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "http-methods": ["Post "],
            "include-command-output-in-response": true,
            "trigger-rule-mismatch-http-response-code": 400,
            "pass-environment-to-command": [
                { "source": "payload", "name": "head_commit.timestamp", "envname": "HOOK_head_commit_ts" }
            ],
            "pass-arguments-to-command": [
                { "source": "payload", "name": "head_commit.id" },
                { "source": "payload", "name": "head_commit.author.email" }
            ],
            "trigger-rule": {
                "and": [
                    {
                        "match": {
                            "type": "payload-hmac-sha1",
                            "secret": "mysecret",
                            "parameter": { "source": "header", "name": "X-Hub-Signature" }
                        }
                    },
                    {
                        "match": {
                            "type": "value",
                            "value": "refs/heads/master",
                            "parameter": { "source": "payload", "name": "ref" }
                        }
                    }
                ]
            }
        },
        {
            "id": "github-multi-sig",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "include-command-output-in-response": true,
            "trigger-signature-soft-failures": true,
            "trigger-rule": {
                "or": [
                    {
                        "match": {
                            "type": "payload-hmac-sha256",
                            "secret": "wrong",
                            "parameter": { "source": "header", "name": "X-Hub-Signature-256" }
                        }
                    },
                    {
                        "match": {
                            "type": "payload-hmac-sha256",
                            "secret": "mysecret",
                            "parameter": { "source": "header", "name": "X-Hub-Signature-256" }
                        }
                    }
                ]
            }
        },
        {
            "id": "github-multi-sig-fail",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "include-command-output-in-response": true,
            "trigger-rule": {
                "or": [
                    {
                        "match": {
                            "type": "payload-hmac-sha256",
                            "secret": "wrong",
                            "parameter": { "source": "header", "name": "X-Hub-Signature-256" }
                        }
                    },
                    {
                        "match": {
                            "type": "payload-hmac-sha256",
                            "secret": "mysecret",
                            "parameter": { "source": "header", "name": "X-Hub-Signature-256" }
                        }
                    }
                ]
            }
        },
        {
            "id": "bitbucket",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "include-command-output-in-response": false,
            "response-message": "success",
            "trigger-rule-mismatch-http-response-code": 999,
            "parse-parameters-as-json": [
                { "source": "payload", "name": "payload" }
            ],
            "trigger-rule": {
                "and": [
                    {
                        "match": {
                            "type": "value",
                            "parameter": { "source": "payload", "name": "payload.canon_url" },
                            "value": "https://bitbucket.org"
                        }
                    },
                    {
                        "match": {
                            "type": "value",
                            "parameter": { "source": "payload", "name": "payload.commits.0.branch" },
                            "value": "master"
                        }
                    }
                ]
            }
        },
        {
            "id": "xml",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "response-message": "xml-ok",
            "trigger-rule": {
                "and": [
                    {
                        "match": {
                            "type": "value",
                            "parameter": { "source": "payload", "name": "app.users.user.0.-name" },
                            "value": "Jeff"
                        }
                    },
                    {
                        "match": {
                            "type": "value",
                            "parameter": { "source": "payload", "name": "app.messages.message.#text" },
                            "value": "Hello!!"
                        }
                    }
                ]
            }
        },
        {
            "id": "sendgrid/dir",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "response-message": "success",
            "trigger-rule": {
                "match": {
                    "type": "value",
                    "parameter": { "source": "payload", "name": "root.0.event" },
                    "value": "it worked!"
                }
            }
        },
        {
            "id": "plex",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "response-message": "plex-ok",
            "parse-parameters-as-json": [
                { "source": "payload", "name": "payload" }
            ],
            "trigger-rule": {
                "match": {
                    "type": "value",
                    "parameter": { "source": "payload", "name": "payload.event" },
                    "value": "media.play"
                }
            }
        },
        {
            "id": "txt-raw",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "incoming-payload-content-type": "text/plain",
            "include-command-output-in-response": true,
            "pass-arguments-to-command": [
                { "source": "raw-request-body" }
            ]
        },
        {
            "id": "err-hidden",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "include-command-output-in-response": true,
            "pass-arguments-to-command": [{ "source": "string", "name": "exit=1" }]
        },
        {
            "id": "err-shown",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "include-command-output-in-response": true,
            "include-command-output-in-response-on-error": true,
            "pass-arguments-to-command": [{ "source": "string", "name": "exit=1" }]
        },
        {
            "id": "file-and-request",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "include-command-output-in-response": true,
            "pass-arguments-to-command": [
                { "source": "request", "name": "method" }
            ],
            "pass-file-to-command": [
                {
                    "source": "payload",
                    "name": "encoded",
                    "envname": "PAYLOAD_FILE",
                    "base64decode": true
                }
            ]
        },
        {
            "id": "issue-471",
            "execute-command": dump_str,
            "command-working-directory": work_dir,
            "response-message": "success",
            "trigger-rule": {
                "or": [
                    {
                        "match": {
                            "type": "value",
                            "parameter": { "source": "payload", "name": "missing_field" },
                            "value": "never"
                        }
                    },
                    {
                        "match": {
                            "type": "value",
                            "parameter": { "source": "payload", "name": "foo" },
                            "value": "bar"
                        }
                    }
                ]
            }
        }
    ]);

    let hooks = parse_hooks_json(&hooks_json.to_string()).expect("parse fixture hooks");
    let runtime = AppConfig::default()
        .build_runtime(hooks, GlobalExecutionMode::Sync)
        .expect("build runtime");
    let state = ApiState::new(runtime).expect("sync state");
    let app = build_router(state);

    // 1. github: valid HMAC-SHA1 + method check + env + args
    let gh_body = br#"{"ref":"refs/heads/master","head_commit":{"id":"abc123","timestamp":"2026-01-01T00:00:00Z","author":{"email":"dev@example.com"}}}"#;
    let gh_sig = format!("sha1={}", hmac_sha1_hex("mysecret", gh_body));
    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/github",
        &[
            ("Content-Type", "application/json"),
            ("X-Hub-Signature", &gh_sig),
        ],
        gh_body,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("args:|abc123|dev@example.com"));
    assert!(body.contains("env_ts=2026-01-01T00:00:00Z"));

    // github wrong method -> 405
    let (status, _, _) = send(&app, Method::GET, "/hooks/github", &[], &[]).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);

    // github bad signature without soft-failure returns 500
    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/github",
        &[
            ("Content-Type", "application/json"),
            ("X-Hub-Signature", "sha1=0000"),
        ],
        gh_body,
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body, "Error occurred while evaluating hook rules.");

    // github valid signature + mismatched ref -> 400
    let wrong_ref_body = br#"{"ref":"refs/heads/dev"}"#;
    let wrong_ref_sig = format!("sha1={}", hmac_sha1_hex("mysecret", wrong_ref_body));
    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/github",
        &[
            ("Content-Type", "application/json"),
            ("X-Hub-Signature", &wrong_ref_sig),
        ],
        wrong_ref_body,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body, "Hook rules were not satisfied.");

    // 2. github-multi-sig vs github-multi-sig-fail
    let sig256 = format!("sha256={}", hmac_sha256_hex("mysecret", b"{}"));
    let (status, _, _) = send(
        &app,
        Method::POST,
        "/hooks/github-multi-sig",
        &[("X-Hub-Signature-256", &sig256)],
        b"{}",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/github-multi-sig-fail",
        &[("X-Hub-Signature-256", &sig256)],
        b"{}",
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body, "Error occurred while evaluating hook rules.");

    // 3. bitbucket form-urlencoded + parse-parameters-as-json + invalid mismatch code 999 -> 200
    let bb_form = "payload=%7B%22canon_url%22%3A%22https%3A%2F%2Fbitbucket.org%22%2C%22commits%22%3A%5B%7B%22branch%22%3A%22master%22%7D%5D%7D";
    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/bitbucket",
        &[("Content-Type", "application/x-www-form-urlencoded")],
        bb_form.as_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "success");

    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/bitbucket",
        &[("Content-Type", "application/x-www-form-urlencoded")],
        b"payload=%7B%22canon_url%22%3A%22other%22%7D",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "Hook rules were not satisfied.");

    // 4. xml mxj parsing
    let xml_body = br#"<app><users><user name="Jeff"/><user name="Jane"/></users><messages><message type="hi">Hello!!</message></messages></app>"#;
    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/xml",
        &[("Content-Type", "application/xml")],
        xml_body,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "xml-ok");

    // 5. sendgrid/dir JSON array wrapping + slash in hook ID
    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/sendgrid/dir",
        &[("Content-Type", "application/json")],
        br#"[{"event":"it worked!"}]"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "success");

    // 6. plex multipart/form-data + parse-parameters-as-json
    let mp_body = concat!(
        "--BOUND\r\n",
        "Content-Disposition: form-data; name=\"payload\"\r\n\r\n",
        "{\"event\":\"media.play\"}\r\n",
        "--BOUND--\r\n"
    );
    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/plex",
        &[("Content-Type", "multipart/form-data; boundary=BOUND")],
        mp_body.as_bytes(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "plex-ok");

    // 7. txt-raw with incoming-payload-content-type override even if caller sent application/json
    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/txt-raw",
        &[("Content-Type", "application/json")],
        b"raw-hello",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.trim(), "args:|raw-hello");

    // 8. error output suppression vs inclusion
    let (status, _, body) = send(&app, Method::POST, "/hooks/err-hidden", &[], b"").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!body.contains("failing-stdout"));

    let (status, _, body) = send(&app, Method::POST, "/hooks/err-shown", &[], b"").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(body.contains("failing-stdout"));
    assert!(body.contains("failing-stderr"));

    // 9. pass-file-to-command base64decode + request.method + cleanup check
    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/file-and-request",
        &[("Content-Type", "application/json")],
        br#"{"encoded":"ZmlsZS1ieXRlcw=="}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("args:|POST"));
    assert!(body.contains("file=file-bytes"));
    let fp = body
        .lines()
        .find_map(|l| l.strip_prefix("filepath="))
        .expect("filepath output");
    assert!(
        !Path::new(fp).exists(),
        "temp file {fp} must be deleted after execution"
    );

    // 10. issue-471 missing field inside `or` succeeds when second branch matches
    let (status, _, body) = send(
        &app,
        Method::POST,
        "/hooks/issue-471",
        &[("Content-Type", "application/json")],
        br#"{"foo":"bar"}"#,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "success");
}
