use crate::domain::BuildStatus;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildRecord {
    pub build_id: String,
    pub status: BuildStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    pub interrupted: Vec<String>,
    pub queued: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerLifecycle {
    pub grace_seconds: u64,
    pub shutdown_requested: bool,
}

impl ServerLifecycle {
    pub fn new(grace_seconds: u64) -> Self {
        Self {
            grace_seconds,
            shutdown_requested: false,
        }
    }

    pub fn request_shutdown(&mut self) {
        self.shutdown_requested = true;
    }

    pub fn recover_from_restart(&self, records: &[BuildRecord]) -> RecoveryReport {
        let interrupted = records
            .iter()
            .filter_map(|record| match record.status {
                BuildStatus::Preparing | BuildStatus::Running | BuildStatus::Cancelling => {
                    Some(record.build_id.clone())
                }
                _ => None,
            })
            .collect::<Vec<_>>();

        let queued = records
            .iter()
            .filter_map(|record| match record.status {
                BuildStatus::Queued => Some(record.build_id.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();

        RecoveryReport {
            interrupted,
            queued,
        }
    }

    pub fn shutdown_queue(&self, queue: &VecDeque<String>) -> Vec<String> {
        queue.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_marks_active_builds_interrupted_and_keeps_queued_work() {
        let lifecycle = ServerLifecycle::new(20);
        let records = vec![
            BuildRecord {
                build_id: "build-queued".to_string(),
                status: BuildStatus::Queued,
            },
            BuildRecord {
                build_id: "build-running".to_string(),
                status: BuildStatus::Running,
            },
            BuildRecord {
                build_id: "build-cancelling".to_string(),
                status: BuildStatus::Cancelling,
            },
            BuildRecord {
                build_id: "build-finished".to_string(),
                status: BuildStatus::Succeeded,
            },
        ];

        let recovered = lifecycle.recover_from_restart(&records);
        assert_eq!(recovered.queued, vec!["build-queued".to_string()]);
        assert_eq!(
            recovered.interrupted,
            vec!["build-running".to_string(), "build-cancelling".to_string()]
        );
    }

    #[test]
    fn graceful_shutdown_keeps_queue_snapshot() {
        let lifecycle = ServerLifecycle::new(15);
        let queue = VecDeque::from(vec!["build-1".to_string(), "build-2".to_string()]);

        let snapshot = lifecycle.shutdown_queue(&queue);
        assert_eq!(snapshot, vec!["build-1".to_string(), "build-2".to_string()]);
        assert_eq!(lifecycle.grace_seconds, 15);
    }
}
