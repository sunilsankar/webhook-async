use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawResponseHeader {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawArgument {
    pub source: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub envname: Option<String>,
    #[serde(default)]
    pub base64decode: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawMatchRule {
    #[serde(rename = "type")]
    pub rule_type: String,
    #[serde(default)]
    pub parameter: Option<RawArgument>,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub regex: Option<String>,
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(rename = "ip-range", default)]
    pub ip_range: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawRules {
    #[serde(default)]
    pub and: Option<Vec<RawRules>>,
    #[serde(default)]
    pub or: Option<Vec<RawRules>>,
    #[serde(default)]
    pub not: Option<Box<RawRules>>,
    #[serde(rename = "match", default)]
    pub match_rule: Option<RawMatchRule>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawHook {
    pub id: String,
    #[serde(rename = "execute-command")]
    pub execute_command: String,
    #[serde(rename = "command-working-directory", default)]
    pub command_working_directory: Option<String>,
    #[serde(rename = "response-message", default)]
    pub response_message: Option<String>,
    #[serde(rename = "response-headers", default)]
    pub response_headers: Vec<RawResponseHeader>,
    #[serde(rename = "include-command-output-in-response", default)]
    pub include_command_output_in_response: bool,
    #[serde(rename = "include-command-output-in-response-on-error", default)]
    pub include_command_output_in_response_on_error: bool,
    #[serde(rename = "pass-environment-to-command", default)]
    pub pass_environment_to_command: Vec<RawArgument>,
    #[serde(rename = "pass-arguments-to-command", default)]
    pub pass_arguments_to_command: Vec<RawArgument>,
    #[serde(rename = "pass-file-to-command", default)]
    pub pass_file_to_command: Vec<RawArgument>,
    #[serde(rename = "parse-parameters-as-json", default)]
    pub parse_parameters_as_json: Vec<RawArgument>,
    #[serde(rename = "trigger-rule", default)]
    pub trigger_rule: Option<RawRules>,
    #[serde(rename = "trigger-rule-mismatch-http-response-code", default)]
    pub trigger_rule_mismatch_http_response_code: Option<u16>,
    #[serde(rename = "trigger-signature-soft-failures", default)]
    pub trigger_signature_soft_failures: bool,
    #[serde(rename = "incoming-payload-content-type", default)]
    pub incoming_payload_content_type: Option<String>,
    #[serde(rename = "success-http-response-code", default)]
    pub success_http_response_code: Option<u16>,
    #[serde(rename = "http-methods", default)]
    pub http_methods: Vec<String>,
    /// Opt-in async execution extension (absent = inherit global mode).
    #[serde(rename = "async", default)]
    pub async_mode: Option<bool>,
    /// Optional per-hook execution timeout extension.
    #[serde(rename = "timeout-seconds", default)]
    pub timeout_seconds: Option<u64>,
    /// Optional per-hook retry limit extension.
    #[serde(rename = "max-attempts", default)]
    pub max_attempts: Option<u32>,
}
