use std::fmt;

use serde::{Deserialize, Serialize};

use crate::CoreError;

/// Canonical asynchronous job states shared across queue and storage adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    Queued,
    Running,
    RetryWait,
    Succeeded,
    Failed,
    CancelRequested,
    Cancelled,
    DeadLetter,
}

impl JobState {
    pub const ALL: [Self; 8] = [
        Self::Queued,
        Self::Running,
        Self::RetryWait,
        Self::Succeeded,
        Self::Failed,
        Self::CancelRequested,
        Self::Cancelled,
        Self::DeadLetter,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::RetryWait => "retry_wait",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::CancelRequested => "cancel_requested",
            Self::Cancelled => "cancelled",
            Self::DeadLetter => "dead_letter",
        }
    }

    /// Returns true if no further state transitions are permitted automatically or manually.
    #[must_use]
    pub const fn is_final(self) -> bool {
        matches!(self, Self::Succeeded | Self::Cancelled)
    }

    /// Returns true if this job has stopped running/waiting (terminal unless manually retried).
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::DeadLetter
        )
    }

    /// Returns true if the job is currently owned or actively waiting for execution.
    #[must_use]
    pub const fn is_pending_or_active(self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Running | Self::RetryWait | Self::CancelRequested
        )
    }

    /// Central, backend-neutral state transition policy.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Queued, Self::Running | Self::Cancelled)
                | (
                    Self::Running,
                    Self::Succeeded
                        | Self::RetryWait
                        | Self::Failed
                        | Self::DeadLetter
                        | Self::CancelRequested
                        | Self::Cancelled
                        | Self::Queued
                )
                | (
                    Self::RetryWait,
                    Self::Queued | Self::Cancelled | Self::DeadLetter
                )
                | (
                    Self::CancelRequested,
                    Self::Cancelled | Self::Succeeded | Self::Failed | Self::DeadLetter
                )
                | (Self::Failed, Self::Queued | Self::DeadLetter)
                | (Self::DeadLetter, Self::Queued)
        )
    }

    pub fn transition_to(self, next: Self) -> Result<Self, CoreError> {
        if self.can_transition_to(next) {
            Ok(next)
        } else {
            Err(CoreError::InvalidStateTransition {
                from: self,
                to: next,
            })
        }
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for JobState {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "retry_wait" => Ok(Self::RetryWait),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancel_requested" => Ok(Self::CancelRequested),
            "cancelled" => Ok(Self::Cancelled),
            "dead_letter" => Ok(Self::DeadLetter),
            other => Err(CoreError::Validation(format!(
                "unknown job state '{other}'"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JobState;

    #[test]
    fn exhaustive_state_transition_matrix() {
        use JobState::*;
        let expected_valid = [
            (Queued, Running),
            (Queued, Cancelled),
            (Running, Succeeded),
            (Running, RetryWait),
            (Running, Failed),
            (Running, DeadLetter),
            (Running, CancelRequested),
            (Running, Cancelled),
            (Running, Queued),
            (RetryWait, Queued),
            (RetryWait, Cancelled),
            (RetryWait, DeadLetter),
            (CancelRequested, Cancelled),
            (CancelRequested, Succeeded),
            (CancelRequested, Failed),
            (CancelRequested, DeadLetter),
            (Failed, Queued),
            (Failed, DeadLetter),
            (DeadLetter, Queued),
        ];

        for from in JobState::ALL {
            for to in JobState::ALL {
                let is_valid = expected_valid.contains(&(from, to));
                assert_eq!(
                    from.can_transition_to(to),
                    is_valid,
                    "unexpected transition allowance for {from} -> {to}"
                );
                let res = from.transition_to(to);
                assert_eq!(res.is_ok(), is_valid);
            }
        }
    }

    #[test]
    fn final_states_reject_all_transitions() {
        for next in JobState::ALL {
            assert!(!JobState::Succeeded.can_transition_to(next));
            assert!(!JobState::Cancelled.can_transition_to(next));
        }
        assert!(JobState::Succeeded.is_final());
        assert!(JobState::Cancelled.is_final());
        assert!(!JobState::Failed.is_final());
        assert!(JobState::Failed.is_terminal());
    }
}
