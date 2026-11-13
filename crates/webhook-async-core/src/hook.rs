use serde::{Deserialize, Serialize};

use crate::CoreError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseHeader {
    pub name: String,
    pub value: String,
}

/// Validated upstream-compatible request value sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ValueSource {
    Header,
    /// Covers both upstream `url` and `query` source names.
    Query,
    Payload,
    RawRequestBody,
    Request,
    StringLiteral,
    EntirePayload,
    EntireHeaders,
    EntireQuery,
}

impl ValueSource {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Header => "header",
            Self::Query => "url",
            Self::Payload => "payload",
            Self::RawRequestBody => "raw-request-body",
            Self::Request => "request",
            Self::StringLiteral => "string",
            Self::EntirePayload => "entire-payload",
            Self::EntireHeaders => "entire-headers",
            Self::EntireQuery => "entire-query",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParameterSpec {
    pub source: ValueSource,
    pub name: String,
    pub env_name: Option<String>,
    pub base64_decode: bool,
}

impl ParameterSpec {
    /// Resolves environment variable name matching upstream `adnanh/webhook` v2.8.3:
    /// - `envname` if non-empty
    /// - otherwise `HOOK_<name>` (`pass-environment-to-command` keeps case;
    ///   `pass-file-to-command` uppercases `<name>`).
    #[must_use]
    pub fn resolved_env_name(&self, uppercase_fallback: bool) -> String {
        if let Some(env_name) = &self.env_name
            && !env_name.is_empty()
        {
            return env_name.clone();
        }
        if uppercase_fallback {
            format!("HOOK_{}", self.name.to_uppercase())
        } else {
            format!("HOOK_{}", self.name)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MatchKind {
    Value(String),
    Regex(String),
    PayloadHmacSha1 { secret: String },
    PayloadHmacSha256 { secret: String },
    PayloadHmacSha512 { secret: String },
    IpWhitelist { ip_range: String },
    ScalrSignature { secret: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchRule {
    pub parameter: ParameterSpec,
    pub kind: MatchKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TriggerRule {
    And(Vec<TriggerRule>),
    Or(Vec<TriggerRule>),
    Not(Box<TriggerRule>),
    Match(MatchRule),
    Empty,
}

impl TriggerRule {
    /// Guards against degenerate deeply nested rule trees before evaluation.
    pub fn validate_depth(&self, max_depth: usize) -> Result<(), CoreError> {
        fn check(rule: &TriggerRule, depth: usize, max_depth: usize) -> Result<(), CoreError> {
            if depth > max_depth {
                return Err(CoreError::Validation(format!(
                    "trigger-rule nesting exceeds maximum depth of {max_depth}"
                )));
            }
            match rule {
                TriggerRule::And(items) | TriggerRule::Or(items) => {
                    for item in items {
                        check(item, depth + 1, max_depth)?;
                    }
                }
                TriggerRule::Not(inner) => check(inner, depth + 1, max_depth)?,
                TriggerRule::Match(_) | TriggerRule::Empty => {}
            }
            Ok(())
        }
        check(self, 1, max_depth)
    }
}

/// Validated, backend-neutral domain representation of a configured webhook.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookDefinition {
    pub id: String,
    pub execute_command: String,
    pub command_working_directory: Option<String>,
    pub response_message: Option<String>,
    pub response_headers: Vec<ResponseHeader>,
    pub include_command_output_in_response: bool,
    pub include_command_output_in_response_on_error: bool,
    pub pass_environment_to_command: Vec<ParameterSpec>,
    pub pass_arguments_to_command: Vec<ParameterSpec>,
    pub pass_file_to_command: Vec<ParameterSpec>,
    pub parse_parameters_as_json: Vec<ParameterSpec>,
    pub trigger_rule: Option<TriggerRule>,
    pub trigger_rule_mismatch_http_response_code: Option<u16>,
    pub trigger_signature_soft_failures: bool,
    pub incoming_payload_content_type: Option<String>,
    pub success_http_response_code: Option<u16>,
    pub http_methods: Vec<String>,
    /// Explicit per-hook execution mode override (`async` JSON/YAML property).
    pub async_mode: Option<bool>,
    /// Optional per-hook command timeout override in seconds.
    pub timeout_seconds: Option<u64>,
    /// Optional per-hook async retry attempt override.
    pub max_attempts: Option<u32>,
}

impl HookDefinition {
    pub const MAX_TRIGGER_RULE_DEPTH: usize = 32;

    pub fn validate(&self) -> Result<(), CoreError> {
        if self.id.trim().is_empty() {
            return Err(CoreError::Validation("hook id must not be empty".into()));
        }
        if self.execute_command.trim().is_empty() {
            return Err(CoreError::Validation(format!(
                "hook '{}' execute-command must not be empty",
                self.id
            )));
        }
        if let Some(timeout) = self.timeout_seconds
            && timeout == 0
        {
            return Err(CoreError::Validation(format!(
                "hook '{}' timeout-seconds must be greater than 0",
                self.id
            )));
        }
        if let Some(attempts) = self.max_attempts
            && attempts == 0
        {
            return Err(CoreError::Validation(format!(
                "hook '{}' max-attempts must be greater than 0",
                self.id
            )));
        }
        if let Some(rule) = &self.trigger_rule {
            rule.validate_depth(Self::MAX_TRIGGER_RULE_DEPTH)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{HookDefinition, ParameterSpec, TriggerRule, ValueSource};

    fn minimal_hook() -> HookDefinition {
        HookDefinition {
            id: "deploy".into(),
            execute_command: "/usr/local/bin/deploy.sh".into(),
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
    fn env_name_fallback_matches_upstream() {
        let param = ParameterSpec {
            source: ValueSource::Payload,
            name: "head_commit.timestamp".into(),
            env_name: None,
            base64_decode: false,
        };
        assert_eq!(param.resolved_env_name(false), "HOOK_head_commit.timestamp");
        assert_eq!(param.resolved_env_name(true), "HOOK_HEAD_COMMIT.TIMESTAMP");

        let explicit = ParameterSpec {
            env_name: Some("CUSTOM_VAR".into()),
            ..param
        };
        assert_eq!(explicit.resolved_env_name(false), "CUSTOM_VAR");
        assert_eq!(explicit.resolved_env_name(true), "CUSTOM_VAR");
    }

    #[test]
    fn validates_hook_and_trigger_depth() {
        let mut hook = minimal_hook();
        assert!(hook.validate().is_ok());

        hook.id = "  ".into();
        assert!(hook.validate().is_err());

        hook.id = "sendgrid/dir".into();
        hook.execute_command = "".into();
        assert!(hook.validate().is_err());

        hook.execute_command = "/bin/echo".into();
        let mut rule = TriggerRule::Empty;
        for _ in 0..=HookDefinition::MAX_TRIGGER_RULE_DEPTH {
            rule = TriggerRule::Not(Box::new(rule));
        }
        hook.trigger_rule = Some(rule);
        assert!(hook.validate().is_err());
    }
}
