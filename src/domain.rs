#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildStatus {
    Queued,
    Preparing,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Cancelled,
    Interrupted,
}

impl BuildStatus {
    pub fn can_transition_to(self, next: Self) -> bool {
        use BuildStatus::{
            Cancelled, Cancelling, Failed, Interrupted, Preparing, Queued, Running, Succeeded,
        };

        matches!(
            (self, next),
            (Queued, Preparing)
                | (Queued, Cancelled)
                | (Preparing, Running)
                | (Preparing, Failed)
                | (Preparing, Cancelling)
                | (Preparing, Interrupted)
                | (Running, Succeeded)
                | (Running, Failed)
                | (Running, Cancelling)
                | (Running, Interrupted)
                | (Cancelling, Cancelled)
                | (Cancelling, Interrupted)
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Job {
    pub id: String,
    pub name: String,
    pub repository_url: String,
    pub default_ref: Option<String>,
    pub disabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Build {
    pub id: String,
    pub job_id: String,
    pub number: u64,
    pub requested_ref: String,
    pub status: BuildStatus,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub cancel_requested_at: Option<i64>,
    pub cancel_reason: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildCancellationReason {
    User,
    ServerShutdown,
}

impl BuildCancellationReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::ServerShutdown => "server_shutdown",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct BuildStep {
    pub step_index: u32,
    pub name: String,
    pub command: String,
    pub status: String,
    pub exit_code: Option<i32>,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_status_transitions() {
        assert!(BuildStatus::Queued.can_transition_to(BuildStatus::Preparing));
        assert!(BuildStatus::Preparing.can_transition_to(BuildStatus::Running));
        assert!(BuildStatus::Running.can_transition_to(BuildStatus::Succeeded));
        assert!(BuildStatus::Running.can_transition_to(BuildStatus::Cancelling));
        assert!(BuildStatus::Cancelling.can_transition_to(BuildStatus::Cancelled));
    }

    #[test]
    fn invalid_status_transitions_are_rejected() {
        assert!(!BuildStatus::Succeeded.can_transition_to(BuildStatus::Running));
        assert!(!BuildStatus::Queued.can_transition_to(BuildStatus::Running));
        assert!(!BuildStatus::Running.can_transition_to(BuildStatus::Preparing));
    }
}
