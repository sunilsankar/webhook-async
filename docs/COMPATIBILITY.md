# Upstream Compatibility Inventory (`adnanh/webhook`)

Pinned upstream baseline:
- Repository: `https://github.com/adnanh/webhook`
- Tag / Release: `2.8.3`
- Commit SHA: `f78eeeca7d2d5f4af2e792d1df5c6e2b0c922dad`
- Upstream license: MIT

Status legend (per `AGENTS.md` §6):
- `TESTED_COMPATIBLE`: behavior implemented and verified by passing regression tests in this workspace.
- `PARTIAL`: data model / parsing implemented, runtime execution or HTTP integration pending in later phase.
- `PENDING`: not yet implemented; planned in `TASKS.md`.
- `DEVIATION_SECURITY` / `EXTENSION`: documented intentional difference or additive extension.

---

## 1. Hook Definition Fields (`internal/hook/hook.go`, `docs/Hook-Definition.md`)

| Upstream Field | Type / Default | Semantics in v2.8.3 | Status | Verification Evidence |
|---|---|---|---|---|
| `id` | `string` (required) | Unique URL route segment (`/{urlprefix}/{id}`); may contain `/` (e.g. `sendgrid/dir`). | `TESTED_COMPATIBLE` | `webhook-async-core` + `webhook-async-compat` parser & duplicate-ID tests |
| `execute-command` | `string` (required) | Direct executable path (no shell interpolation in `webhook-async`). | `TESTED_COMPATIBLE` | `webhook-async-compat::parser::tests`, `webhook-async-executor::tests`, `compat_fixtures` |
| `command-working-directory` | `string` (optional) | Working directory for process and default parent for `pass-file-to-command` temp files. | `TESTED_COMPATIBLE` | `webhook-async-executor::tests::missing_working_dir_returns_error`, `compat_fixtures` |
| `response-message` | `string` (optional) | Response body when `include-command-output-in-response` is `false`. | `TESTED_COMPATIBLE` | `webhook-async-api::tests`, `compat_fixtures` |
| `response-headers` | `[{"name","value"}]` | Static response headers set before writing status/body (overrides matching global `-header` keys). | `TESTED_COMPATIBLE` | `webhook-async-api::tests::custom_headers_and_success_code` |
| `include-command-output-in-response` | `bool` (`false`) | When `true`, waits synchronously and returns combined stdout+stderr. | `TESTED_COMPATIBLE` | `webhook-async-api::tests`, `compat_fixtures` |
| `include-command-output-in-response-on-error` | `bool` (`false`) | When `true` (and output enabled), non-zero exit returns HTTP 500 with command output; otherwise `"Error occurred while executing the hook's command."`. | `TESTED_COMPATIBLE` | `compat_fixtures::compat_error_output_suppression_and_opt_in` |
| `pass-environment-to-command` | `[]Argument` | Passes env vars; defaults to `HOOK_<name>` (preserving `<name>` case) if `envname` empty; missing parameters omitted. | `TESTED_COMPATIBLE` | `webhook-async-core::hook::tests`, `webhook-async-executor::tests`, `compat_fixtures` |
| `pass-arguments-to-command` | `[]Argument` | Ordered CLI arguments passed directly via argv; missing parameters resolve to `""`. | `TESTED_COMPATIBLE` | `webhook-async-executor::tests`, `compat_fixtures` |
| `pass-file-to-command` | `[]Argument` | Writes temp file (optional `base64decode`), passes path via `envname` or `HOOK_<UPPER(name)>`, deletes after exit; missing parameters omitted. | `TESTED_COMPATIBLE` | `webhook-async-executor::tests`, `compat_fixtures::compat_pass_file_base64_and_request_source` |
| `parse-parameters-as-json` | `[]Argument` | Decodes JSON string parameter in-place into map in `header`, `url`/`query`, or `payload`. | `TESTED_COMPATIBLE` | `webhook-async-hook-engine::tests`, `compat_fixtures::compat_bitbucket_form_json_param_and_invalid_status` |
| `trigger-rule` | `*Rules` (optional) | Boolean rule tree (`and`, `or`, `not`, `match`). | `TESTED_COMPATIBLE` | `webhook-async-hook-engine::tests`, `compat_fixtures` |
| `trigger-rule-mismatch-http-response-code` | `int` (`200`) | HTTP status on rule mismatch; invalid codes (e.g. `999`) fall back to `200`. | `TESTED_COMPATIBLE` | `webhook-async-hook-engine::tests`, `compat_fixtures::compat_bitbucket_form_json_param_and_invalid_status` |
| `trigger-signature-soft-failures` | `bool` (`false`) | Allows `or` rule to swallow `SignatureError` and continue evaluating later branches. | `TESTED_COMPATIBLE` | `compat_fixtures::compat_github_multi_sig_soft_failures_toggle` |
| `incoming-payload-content-type` | `string` (optional) | Overrides `Content-Type` header when selecting body parser. | `TESTED_COMPATIBLE` | `compat_fixtures::compat_txt_raw_content_type_override` |
| `success-http-response-code` | `int` (`200`) | HTTP status on success; invalid codes fall back to `200`. | `TESTED_COMPATIBLE` | `webhook-async-api::tests::custom_headers_and_success_code` |
| `http-methods` | `[]string` | Allowed HTTP methods; entries trimmed and uppercased (`"Post "` -> `"POST"`). Overrides global `-http-methods`. | `TESTED_COMPATIBLE` | `compat_fixtures::compat_github_hmac_sha1_and_method_filter` |
| `async` (`EXTENSION`) | `bool` (optional) | `webhook-async` opt-in override (`true` = async, `false` = sync, omitted = inherit CLI mode). | `TESTED_COMPATIBLE` | `webhook-async-core::mode::tests`, `webhook-async-config::tests`, `webhook-async-api::tests::async_hook_returns_202_after_durable_enqueue_and_deduplicates_idempotency`, `tests/async_pipeline.rs` |
| `timeout-seconds` / `max-attempts` (`EXTENSION`) | `u64` / `u32` | Per-hook runtime timeout and retry overrides (> 0). | `TESTED_COMPATIBLE` | `webhook-async-core::hook::tests`, `webhook-async-worker::tests`, `tests/async_pipeline.rs` |

---

## 2. Hook File Formats & Parsing (`internal/hook/hook.go`, `test/hooks.json.tmpl`, `test/hooks.yaml.tmpl`)

| Behavior | Upstream Baseline Details | Status | Notes |
|---|---|---|---|
| JSON array of hook objects | Standard JSON array (`[{...}]`), rejected on unknown fields in `webhook-async` (`AGENTS.md` §6). | `TESTED_COMPATIBLE` | `webhook-async-compat::parser::parse_hooks_json` (`#[serde(deny_unknown_fields)]`). |
| YAML sequence of hook objects | YAML array (`- id: ...`) via `yaml_serde`. | `TESTED_COMPATIBLE` | `webhook-async-compat::parser::parse_hooks_yaml`. |
| Duplicate hook IDs across files | Upstream logs warning / keeps map entry or errors; `webhook-async` rejects duplicate IDs explicitly. | `TESTED_COMPATIBLE` | `parse_hooks_files` returns `CompatError::DuplicateHookId`. |
| Upstream `test/hooks.json.tmpl` quirks | Upstream test fixture uses `ghodss/yaml` quirks in 3 places: trailing commas (`xml`), `"or": [...]` bare pair inside `"and"` array (`github-multi-sig`), and unquoted `"value": 1` (`issue-471`). `docs/Hook-Rules.md` documents strict JSON and quoted string `"value"`. | `DEVIATION_SECURITY` | Strict JSON/YAML parsing is enforced; documented rather than enabling permissive non-standard JSON. |
| Go `text/template` (`-template`) | Evaluates `getenv`, `cat` (trims trailing `\n`, empty on missing), `credential` (`CREDENTIALS_DIRECTORY` + fallback `getenv`), `js` escaping before parsing hook JSON/YAML. | `TESTED_COMPATIBLE` | `webhook-async-compat::template::tests`, `webhook-async-cli::tests::templated_hooks_validate_successfully`. |
| Hot reload (`-hotreload`, `SIGHUP`, `SIGUSR1`) | Watches hook files and reloads on interval / signals without dropping server. In-flight jobs keep immutable `ExecutionSpec` snapshots. | `TESTED_COMPATIBLE` | `webhook-async-api::tests::reload_hooks_swaps_hook_table_cleanly`, `webhook-async-cli::spawn_reload_watcher`. |

---

## 3. Request Value Sources & Extraction (`internal/hook/request.go`, `internal/hook/hook.go`)

| Source / Behavior | Upstream Semantics | Status | Notes |
|---|---|---|---|
| `header` | Canonical MIME header key (`textproto.CanonicalMIMEHeaderKey`, e.g. `x-hub-signature` -> `X-Hub-Signature`). Sub-keys after `.` (from `parse-parameters-as-json`) preserve original casing. | `TESTED_COMPATIBLE` | `webhook-async-hook-engine` unit tests + `compat_fixtures`. |
| `url` / `query` | Query parameters (`query` is an alias for `url`). | `TESTED_COMPATIBLE` | `ValueSource::Query` parses both `"url"` and `"query"`. |
| `payload` | Parsed JSON/form/XML/multipart payload tree. | `TESTED_COMPATIBLE` | Verified across JSON, XML, form-urlencoded, and multipart in `compat_fixtures`. |
| `raw-request-body` | Untouched raw request body bytes as UTF-8/lossy string. | `TESTED_COMPATIBLE` | Preserved in `RequestContext.raw_body`; verified in `compat_fixtures::compat_txt_raw_content_type_override`. |
| `request` (`remote-addr`, `method`) | Case-insensitive `name`: `remote-addr` and `method`. | `TESTED_COMPATIBLE` | Validated in `compat`; verified in `compat_fixtures::compat_pass_file_base64_and_request_source`. |
| `string` | Literal string value taken from `name`. | `TESTED_COMPATIBLE` | `ValueSource::StringLiteral`; verified in `compat_fixtures`. |
| `entire-payload`, `entire-headers`, `entire-query` | Serializes full object map as JSON string when passed to args/env/files. | `TESTED_COMPATIBLE` | `webhook-async-hook-engine::extract::tests`. |
| Literal dotted keys vs nested dot paths | Checks raw map key `"a.b"` before splitting at first `.`; supports non-negative array indices (`0`, `1`); complex objects/arrays serialize as compact JSON. | `TESTED_COMPATIBLE` | `lookup_dot_path` + `extract::tests`. |
| Top-level JSON array payloads | Wrapped under `Payload["root"] = [...]` (`ParseJSONPayload`). | `TESTED_COMPATIBLE` | `RequestContext::parse_json_payload` + `compat_fixtures::compat_sendgrid_json_array_and_slash_hook_id`. |
| XML (`application/xml`, `text/xml`) | `mxj` conventions: attributes `-attr`, text `#text`, repeated nodes as JSON arrays. | `TESTED_COMPATIBLE` | `RequestContext::parse_xml_payload` + `compat_fixtures::compat_xml_mxj_attributes_and_text`. |
| Form & Multipart (`x-www-form-urlencoded`, `multipart/form-data`) | First value per form key; JSON-typed file parts or `parse-parameters-as-json` payload entries decoded as JSON; respects `-max-multipart-mem` (`1048576`). | `TESTED_COMPATIBLE` | `parse_form_payload` + `parse_multipart_payload` + `compat_fixtures`. |

---

## 4. Trigger Rules & Signature Verification (`internal/hook/hook.go`, `webhook.go`)

| Feature | Upstream Semantics | Status | Notes |
|---|---|---|---|
| Boolean combinators `and`, `or`, `not`, empty rule | Empty rule returns `false, nil`; single operator per rule node. | `TESTED_COMPATIBLE` | `eval_rule_tree` + `compat` validation. |
| `Or` error swallowing | Always swallows `ParameterNodeError`; swallows `SignatureError` only if `trigger-signature-soft-failures: true`. | `TESTED_COMPATIBLE` | `webhook-async-hook-engine::tests`, `compat_fixtures`. |
| HTTP mismatch vs evaluation error | Missing node (`ParameterNodeError`) -> `trigger-rule-mismatch-http-response-code` (body `"Hook rules were not satisfied."`); unswallowed errors -> HTTP 500 `"Error occurred while evaluating hook rules."`. | `TESTED_COMPATIBLE` | `webhook-async-api::tests`, `compat_fixtures::compat_github_hmac_sha1_and_method_filter`, `compat_fixtures::compat_github_multi_sig_soft_failures_toggle`. |
| `value` | Constant-time string comparison (`subtle::ConstantTimeEq`). | `TESTED_COMPATIBLE` | `webhook-async-hook-engine::trigger::tests`, `compat_fixtures`. |
| `regex` | Regular expression match (`regex` crate; rejects invalid regex as evaluation error). | `TESTED_COMPATIBLE` | `webhook-async-hook-engine::trigger::tests`. |
| `payload-hmac-sha1` / `sha256` / `sha512` (+ deprecated `payload-hash-*`) | Empty secret errors; empty payload allowed; comma-separated header values filter by `sha1=`/`sha256=`/`sha512=`; single value trims prefix if present; constant-time compare; never leaks expected MAC. | `TESTED_COMPATIBLE` | `webhook-async-hook-engine::trigger::tests`, `compat_fixtures`. |
| `ip-whitelist` | Whitespace-separated IPs/CIDRs; strips `[` `]` and `:port` from `remote-addr`. Upstream quirk: bare IPv6 without `/` had `/32` appended in Go `hook.go:199`; `webhook-async` uses `/128` for IPv6 host addresses and `/32` for IPv4. | `TESTED_COMPATIBLE` (`DEVIATION_SECURITY` on bare IPv6 mask) | `webhook-async-hook-engine::trigger::tests`. |
| `scalr-signature` | HMAC-SHA1 over `rawBody + Date` vs `X-Signature`; validates `Date` (`Mon 02 Jan 2006 15:04:05 MST`) within ±300s window. | `TESTED_COMPATIBLE` | `webhook-async-hook-engine::trigger::tests`. |

---

## 5. HTTP Server, Execution Modes, Security & CLI (`webhook.go`, `tls.go`, `signals.go`, `platform_unix.go`)

| Feature | Upstream Semantics | Status | Notes |
|---|---|---|---|
| Endpoint routing (`/{urlprefix}/{id}`) | Default `urlprefix` = `"hooks"`; empty prefix routes `/{id}`; hook IDs with `/` allowed; `/` returns `200 OK`. | `TESTED_COMPATIBLE` | `webhook-async-api::tests`, `compat_fixtures::compat_sendgrid_json_array_and_slash_hook_id`. |
| Request ID (`-x-request-id`, `-x-request-id-limit`) | Uses `X-Request-Id` header when enabled (truncated to limit if `> 0`), else generates 6-char ID. | `TESTED_COMPATIBLE` | `webhook-async-api::tests::request_id_generation_and_truncation`, `webhook-async-cli::tests`. |
| Global headers (`-header`), HTTP methods (`-http-methods`) | Static response headers on all responses; global method allowlist when hook `http-methods` is empty; `405` on mismatch. | `TESTED_COMPATIBLE` | `webhook-async-api::tests`, `compat_fixtures`, `webhook-async-cli::tests`. |
| Sync vs Async opt-in mode (`AGENTS.md` §7A) | Default sync; `--sync` and `--async` mutually exclusive; per-hook `async` overrides CLI flag; sync path requires zero queue storage; `--async` persists execution spec durably before returning HTTP 202. | `TESTED_COMPATIBLE` | `webhook-async-core::mode::tests`, `webhook-async-config::tests`, `webhook-async-api::tests`, `webhook-async-cli::tests`, `tests/async_pipeline.rs`. |
| Synchronous execution & response (`include-command-output-in-response`) | Runs allowlisted binary with args/env/temp-files; returns combined stdout/stderr when enabled or `response-message` immediately (bounded background spawn) when disabled. | `TESTED_COMPATIBLE` | `webhook-async-api::tests`, `compat_fixtures`. |
| TLS (`-secure`, `-cert`, `-key`, `-tls-min-version`, `-cipher-suites`, `-list-cipher-suites`) | HTTPS serving via Rustls (TLS 1.2 and 1.3), self-signed fallback via `rcgen` when cert/key omitted, `--list-cipher-suites`, explicit rejection of custom cipher suites. | `TESTED_COMPATIBLE` | `webhook-async-api::tls`, `webhook-async-cli::tests::server_flag_validation_and_pidfile_guard`. |
| Unix socket (`-socket`), PID file (`-pidfile`), privilege drop (`-setuid`/`-setgid`), signals (`SIGHUP`/`SIGUSR1`/`SIGTERM`/`SIGINT`), `-logfile`, `-nopanic` | Unix listener with unlinking on start/stop, PID file lifecycle guard, paired privilege drop, signal reload/shutdown, logfile redirection, nopanic hook. | `TESTED_COMPATIBLE` | `webhook-async-api::tests::serve_full_unix_domain_socket_and_cleans_up`, `webhook-async-cli::tests::server_flag_validation_and_pidfile_guard`. |
