use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scheduler {
    max_concurrent_builds: usize,
    queue: VecDeque<String>,
    active: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    InvalidConcurrency,
}

impl Scheduler {
    pub fn new(max_concurrent_builds: usize) -> Result<Self, SchedulerError> {
        if max_concurrent_builds == 0 {
            return Err(SchedulerError::InvalidConcurrency);
        }

        Ok(Self {
            max_concurrent_builds,
            queue: VecDeque::new(),
            active: Vec::new(),
        })
    }

    pub fn enqueue(&mut self, build_id: impl Into<String>) {
        self.queue.push_back(build_id.into());
    }

    pub fn queued_count(&self) -> usize {
        self.queue.len()
    }

    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    pub fn capacity_remaining(&self) -> usize {
        self.max_concurrent_builds.saturating_sub(self.active.len())
    }

    pub fn claim_next(&mut self) -> Option<String> {
        if self.active.len() >= self.max_concurrent_builds {
            return None;
        }

        let build_id = self.queue.pop_front()?;
        self.active.push(build_id.clone());
        Some(build_id)
    }

    pub fn mark_finished(&mut self, build_id: &str) -> bool {
        let Some(position) = self.active.iter().position(|queued| queued == build_id) else {
            return false;
        };
        self.active.remove(position);
        true
    }

    pub fn cancel(&mut self, build_id: &str) -> bool {
        if let Some(position) = self.queue.iter().position(|queued| queued == build_id) {
            self.queue.remove(position);
            return true;
        }

        self.active
            .iter()
            .position(|queued| queued == build_id)
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn respects_concurrency_limit() {
        let mut scheduler = Scheduler::new(2).unwrap();
        for id in ["build-1", "build-2", "build-3"] {
            scheduler.enqueue(id);
        }

        assert_eq!(scheduler.claim_next(), Some("build-1".to_string()));
        assert_eq!(scheduler.claim_next(), Some("build-2".to_string()));
        assert_eq!(scheduler.claim_next(), None);
        assert_eq!(scheduler.active_count(), 2);
    }

    #[test]
    fn queued_builds_can_be_cancelled() {
        let mut scheduler = Scheduler::new(2).unwrap();
        scheduler.enqueue("build-1");
        scheduler.enqueue("build-2");

        assert!(scheduler.cancel("build-1"));
        assert_eq!(scheduler.queued_count(), 1);
        assert_eq!(scheduler.claim_next(), Some("build-2".to_string()));
    }

    #[test]
    fn completes_running_builds_and_releases_capacity() {
        let mut scheduler = Scheduler::new(1).unwrap();
        scheduler.enqueue("build-1");
        assert_eq!(scheduler.claim_next(), Some("build-1".to_string()));

        assert!(scheduler.mark_finished("build-1"));
        assert_eq!(scheduler.active_count(), 0);
        assert_eq!(scheduler.capacity_remaining(), 1);
    }
}
