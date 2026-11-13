use serde::{Deserialize, Serialize};

use crate::CoreError;

/// Global server execution mode selected via CLI flags (`--sync` or `--async`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GlobalExecutionMode {
    #[default]
    Unspecified,
    Sync,
    Async,
}

/// Effective execution mode resolved per hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectiveExecutionMode {
    Sync,
    Async,
}

impl EffectiveExecutionMode {
    #[must_use]
    pub const fn is_async(self) -> bool {
        matches!(self, Self::Async)
    }
}

/// Validates mutually exclusive `--sync` and `--async` flags before listener binding.
pub fn validate_cli_mode_flags(
    sync_flag: bool,
    async_flag: bool,
) -> Result<GlobalExecutionMode, CoreError> {
    match (sync_flag, async_flag) {
        (true, true) => Err(CoreError::ConflictingExecutionModeFlags),
        (true, false) => Ok(GlobalExecutionMode::Sync),
        (false, true) => Ok(GlobalExecutionMode::Async),
        (false, false) => Ok(GlobalExecutionMode::Unspecified),
    }
}

/// Resolves execution mode with strict precedence:
/// 1. Explicit per-hook `async` boolean (`Some(true)` or `Some(false)`).
/// 2. Global CLI mode (`--async` or `--sync`).
/// 3. Synchronous default when neither is specified.
#[must_use]
pub const fn resolve_execution_mode(
    hook_async_override: Option<bool>,
    global_mode: GlobalExecutionMode,
) -> EffectiveExecutionMode {
    match hook_async_override {
        Some(true) => EffectiveExecutionMode::Async,
        Some(false) => EffectiveExecutionMode::Sync,
        None => match global_mode {
            GlobalExecutionMode::Async => EffectiveExecutionMode::Async,
            GlobalExecutionMode::Sync | GlobalExecutionMode::Unspecified => {
                EffectiveExecutionMode::Sync
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EffectiveExecutionMode, GlobalExecutionMode, resolve_execution_mode,
        validate_cli_mode_flags,
    };
    use crate::CoreError;

    #[test]
    fn cli_mode_flags_reject_conflict_and_preserve_defaults() {
        assert_eq!(
            validate_cli_mode_flags(false, false),
            Ok(GlobalExecutionMode::Unspecified)
        );
        assert_eq!(
            validate_cli_mode_flags(true, false),
            Ok(GlobalExecutionMode::Sync)
        );
        assert_eq!(
            validate_cli_mode_flags(false, true),
            Ok(GlobalExecutionMode::Async)
        );
        assert_eq!(
            validate_cli_mode_flags(true, true),
            Err(CoreError::ConflictingExecutionModeFlags)
        );
    }

    #[test]
    fn execution_mode_precedence_matrix() {
        use EffectiveExecutionMode::{Async, Sync};
        use GlobalExecutionMode as G;

        // No global flag defaults to Sync unless hook explicitly sets async: true.
        assert_eq!(resolve_execution_mode(None, G::Unspecified), Sync);
        assert_eq!(resolve_execution_mode(Some(false), G::Unspecified), Sync);
        assert_eq!(resolve_execution_mode(Some(true), G::Unspecified), Async);

        // Global --sync allows per-hook async: true override.
        assert_eq!(resolve_execution_mode(None, G::Sync), Sync);
        assert_eq!(resolve_execution_mode(Some(false), G::Sync), Sync);
        assert_eq!(resolve_execution_mode(Some(true), G::Sync), Async);

        // Global --async is overridden by explicit per-hook async: false.
        assert_eq!(resolve_execution_mode(None, G::Async), Async);
        assert_eq!(resolve_execution_mode(Some(false), G::Async), Sync);
        assert_eq!(resolve_execution_mode(Some(true), G::Async), Async);
    }
}
