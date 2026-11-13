//! Core domain types, state transitions, execution mode resolution, and errors.

pub mod error;
pub mod hook;
pub mod job;
pub mod mode;
pub mod state;

pub use error::CoreError;
pub use hook::{
    HookDefinition, MatchKind, MatchRule, ParameterSpec, ResponseHeader, TriggerRule, ValueSource,
};
pub use job::{
    ErrorClass, ExecutionSpec, JobAttempt, JobId, JobRecord, LeaseToken, TempFileSpec, WorkerInfo,
};
pub use mode::{
    EffectiveExecutionMode, GlobalExecutionMode, resolve_execution_mode, validate_cli_mode_flags,
};
pub use state::JobState;
