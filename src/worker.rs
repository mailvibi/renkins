use crate::config::{
    DEFAULT_MAX_BUILD_OUTPUT_BYTES, MAX_BUILD_OUTPUT_BYTES, MAX_CONCURRENT_BUILDS,
};
use crate::domain::{Build, BuildCancellationReason, BuildStatus};
use crate::executor::{self, StepStatus};
use crate::git;
use crate::logs::{BuildLog, LogStream};
use crate::pipeline::parse_pipeline_yaml;
use crate::storage::{DatabaseStore, StepOutcomeRecord};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Debug, Default)]
pub struct WorkerControl {
    active_builds: Arc<Mutex<HashMap<String, Arc<AtomicBool>>>>,
    claim_gate: Arc<Mutex<()>>,
    shutdown_requested: Arc<AtomicBool>,
    scheduler_ready: Arc<AtomicBool>,
}

impl WorkerControl {
    pub fn is_ready(&self) -> bool {
        self.scheduler_ready.load(Ordering::Acquire)
    }

    pub fn request_shutdown(&self) -> Result<(), String> {
        let claim_gate = self.claim_gate.lock();
        self.shutdown_requested.store(true, Ordering::Release);
        self.scheduler_ready.store(false, Ordering::Release);
        claim_gate
            .map(|_| ())
            .map_err(|_| "worker claim gate is poisoned".to_string())
    }

    fn active_build_ids(&self) -> Result<Vec<String>, String> {
        let active_builds = self
            .active_builds
            .lock()
            .map_err(|_| "worker control lock is poisoned".to_string())?;
        Ok(active_builds.keys().cloned().collect())
    }

    pub fn cancel_active(&self, build_id: &str) -> Result<bool, String> {
        let active_builds = self
            .active_builds
            .lock()
            .map_err(|_| "worker control lock is poisoned".to_string())?;
        if let Some(cancellation_requested) = active_builds.get(build_id) {
            cancellation_requested.store(true, Ordering::Release);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn register(&self, build_id: &str) -> Result<ActiveBuildGuard, String> {
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let mut active_builds = self
            .active_builds
            .lock()
            .map_err(|_| "worker control lock is poisoned".to_string())?;
        match active_builds.entry(build_id.to_string()) {
            Entry::Occupied(_) => {
                return Err(format!("build {build_id} is already registered as active"));
            }
            Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&cancellation_requested));
            }
        }
        Ok(ActiveBuildGuard {
            build_id: build_id.to_string(),
            cancellation_requested,
            control: self.clone(),
        })
    }
}

struct ActiveBuildGuard {
    build_id: String,
    cancellation_requested: Arc<AtomicBool>,
    control: WorkerControl,
}

impl Drop for ActiveBuildGuard {
    fn drop(&mut self) {
        if let Ok(mut active_builds) = self.control.active_builds.lock() {
            active_builds.remove(&self.build_id);
        }
    }
}

struct WorkerThreadGuard {
    control: WorkerControl,
}

impl Drop for WorkerThreadGuard {
    fn drop(&mut self) {
        self.control.scheduler_ready.store(false, Ordering::Release);
    }
}

pub struct WorkerHandle {
    store: Arc<Mutex<DatabaseStore>>,
    control: WorkerControl,
    workers: Vec<thread::JoinHandle<()>>,
}

impl WorkerHandle {
    pub fn shutdown(self, grace_period: Duration) -> Result<(), String> {
        let mut errors = Vec::new();
        if let Err(err) = self.control.request_shutdown() {
            errors.push(err);
        }
        let deadline = Instant::now().checked_add(grace_period);
        if deadline.is_none() {
            errors.push("shutdown grace period is outside the monotonic clock range".to_string());
        }
        loop {
            let active_builds = match self.control.active_build_ids() {
                Ok(active_builds) => active_builds,
                Err(err) => {
                    errors.push(err);
                    break;
                }
            };
            if active_builds.is_empty()
                || deadline.is_none_or(|deadline| Instant::now() >= deadline)
            {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }

        let active_builds = match self.control.active_build_ids() {
            Ok(active_builds) => active_builds,
            Err(err) => {
                errors.push(err);
                Vec::new()
            }
        };
        if !active_builds.is_empty() {
            for build_id in active_builds {
                let cancellation = self
                    .store
                    .lock()
                    .map_err(|_| "database store lock is poisoned".to_string())
                    .and_then(|mut store| {
                        store.request_build_cancellation(
                            &build_id,
                            BuildCancellationReason::ServerShutdown,
                        )
                    });
                match cancellation {
                    Ok(Some(build)) if build.status == BuildStatus::Cancelling => {
                        if let Err(err) = self.control.cancel_active(&build_id) {
                            errors.push(err);
                        }
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => errors.push(format!("active build {build_id} disappeared")),
                    Err(err) => {
                        errors.push(err);
                        if let Err(err) = self.control.cancel_active(&build_id) {
                            errors.push(err);
                        }
                    }
                }
            }
        }

        for worker in self.workers {
            if worker.join().is_err() {
                errors.push("worker thread panicked".to_string());
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

pub fn start_worker(
    store: Arc<Mutex<DatabaseStore>>,
    data_dir: PathBuf,
    control: WorkerControl,
) -> Result<WorkerHandle, String> {
    start_worker_pool(store, data_dir, control, 1)
}

pub fn start_worker_pool(
    store: Arc<Mutex<DatabaseStore>>,
    data_dir: PathBuf,
    control: WorkerControl,
    concurrency: usize,
) -> Result<WorkerHandle, String> {
    start_worker_pool_with_output_limit(
        store,
        data_dir,
        control,
        concurrency,
        DEFAULT_MAX_BUILD_OUTPUT_BYTES,
    )
}

pub fn start_worker_pool_with_output_limit(
    store: Arc<Mutex<DatabaseStore>>,
    data_dir: PathBuf,
    control: WorkerControl,
    concurrency: usize,
    max_build_output_bytes: u64,
) -> Result<WorkerHandle, String> {
    if !(1..=MAX_BUILD_OUTPUT_BYTES).contains(&max_build_output_bytes) {
        return Err(format!(
            "max_build_output_bytes must be between 1 and {MAX_BUILD_OUTPUT_BYTES}"
        ));
    }
    if !(1..=MAX_CONCURRENT_BUILDS).contains(&concurrency) {
        return Err(format!(
            "worker concurrency must be between 1 and {MAX_CONCURRENT_BUILDS}"
        ));
    }
    control.scheduler_ready.store(true, Ordering::Release);
    let mut workers = Vec::with_capacity(concurrency);
    for index in 0..concurrency {
        let worker_store = Arc::clone(&store);
        let data_dir = data_dir.clone();
        let worker_control = control.clone();
        let worker = thread::Builder::new()
            .name(format!("renkins-worker-{index}"))
            .spawn(move || {
                let _thread_guard = WorkerThreadGuard {
                    control: worker_control.clone(),
                };
                loop {
                    if worker_control.shutdown_requested.load(Ordering::Acquire) {
                        break;
                    }
                    match run_one_build_with_output_limit(
                        &worker_store,
                        &data_dir,
                        &worker_control,
                        max_build_output_bytes,
                    ) {
                        Ok(true) => {}
                        Ok(false) if worker_control.shutdown_requested.load(Ordering::Acquire) => {
                            break;
                        }
                        Ok(false) => thread::sleep(POLL_INTERVAL),
                        Err(err) => {
                            eprintln!("worker iteration failed: {err}");
                            thread::sleep(POLL_INTERVAL);
                        }
                    }
                }
            });
        match worker {
            Ok(worker) => workers.push(worker),
            Err(err) => {
                let partial = WorkerHandle {
                    store: Arc::clone(&store),
                    control,
                    workers,
                };
                let shutdown = partial.shutdown(Duration::ZERO);
                return match shutdown {
                    Ok(()) => Err(format!("failed to start build worker thread: {err}")),
                    Err(shutdown_err) => Err(format!(
                        "failed to start build worker thread: {err}; cleanup failed: {shutdown_err}"
                    )),
                };
            }
        }
    }
    Ok(WorkerHandle {
        store,
        control,
        workers,
    })
}

pub fn run_one_build(store: &Arc<Mutex<DatabaseStore>>, data_dir: &Path) -> Result<bool, String> {
    run_one_build_with_control(store, data_dir, &WorkerControl::default())
}

pub fn run_one_build_with_control(
    store: &Arc<Mutex<DatabaseStore>>,
    data_dir: &Path,
    control: &WorkerControl,
) -> Result<bool, String> {
    run_one_build_with_output_limit(store, data_dir, control, DEFAULT_MAX_BUILD_OUTPUT_BYTES)
}

pub(crate) fn run_one_build_with_output_limit(
    store: &Arc<Mutex<DatabaseStore>>,
    data_dir: &Path,
    control: &WorkerControl,
    max_build_output_bytes: u64,
) -> Result<bool, String> {
    let (build, active) = {
        let _claim_gate = control
            .claim_gate
            .lock()
            .map_err(|_| "worker claim gate is poisoned".to_string())?;
        if control.shutdown_requested.load(Ordering::Acquire) {
            return Ok(false);
        }
        let mut store = store
            .lock()
            .map_err(|_| "database store lock is poisoned".to_string())?;
        let Some(build) = store.claim_next_build()? else {
            return Ok(false);
        };
        let active = control.register(&build.id)?;
        (build, active)
    };

    if cancellation_is_requested(store, &build, &active.cancellation_requested)? {
        finish_cancelled(store, &build, data_dir)?;
        return Ok(true);
    }
    process_build(
        store,
        data_dir,
        build,
        &active.cancellation_requested,
        max_build_output_bytes,
    )?;
    Ok(true)
}

fn process_build(
    store: &Arc<Mutex<DatabaseStore>>,
    data_dir: &Path,
    build: Build,
    cancellation_requested: &AtomicBool,
    max_build_output_bytes: u64,
) -> Result<(), String> {
    let job = {
        let store = store
            .lock()
            .map_err(|_| "database store lock is poisoned".to_string())?;
        store.get_job(&build.job_id)?.ok_or_else(|| {
            format!(
                "job {} for build {} no longer exists",
                build.job_id, build.id
            )
        })?
    };

    let log_path = data_dir.join("logs").join(format!("{}.rkl", build.id));
    if let Some(parent) = log_path.parent()
        && let Err(err) = fs::create_dir_all(parent)
    {
        fail_build(store, &build, BuildStatus::Preparing, "log_setup_failed")?;
        return Err(format!("failed to create build log directory: {err}"));
    }
    if let Err(err) = append_log(&log_path, "[build] preparing checkout") {
        fail_build(store, &build, BuildStatus::Preparing, "log_setup_failed")?;
        return Err(err);
    }

    let workspace = match git::prepare_workspace(data_dir, &build.id) {
        Ok(path) => path.join("checkout"),
        Err(err) => {
            fail_build(
                store,
                &build,
                BuildStatus::Preparing,
                "workspace_setup_failed",
            )?;
            return Err(format!("failed to create build workspace: {err}"));
        }
    };
    if cancellation_is_requested(store, &build, cancellation_requested)? {
        return finish_cancelled(store, &build, data_dir);
    }
    let checkout_result = git::checkout_repository_with_cancellation(
        &job.repository_url,
        &workspace,
        &build.requested_ref,
        cancellation_requested,
    );
    if cancellation_is_requested(store, &build, cancellation_requested)? {
        return finish_cancelled(store, &build, data_dir);
    }
    let checkout = match checkout_result {
        Ok(checkout) => checkout,
        Err(err) => {
            fail_build(store, &build, BuildStatus::Preparing, "checkout_failed")?;
            append_log(&log_path, "[build] checkout failed")?;
            return Err(format!("checkout failed for build {}: {err:?}", build.id));
        }
    };

    let recorded = store
        .lock()
        .map_err(|_| "database store lock is poisoned".to_string())?
        .record_resolved_commit(&build.id, &checkout.resolved_commit);
    let recorded = match recorded {
        Ok(recorded) => recorded,
        Err(err) => {
            let fail_result = fail_build(
                store,
                &build,
                BuildStatus::Preparing,
                "commit_persistence_failed",
            );
            return match fail_result {
                Ok(()) => Err(err),
                Err(fail_err) => Err(format!(
                    "{err}; also failed to mark build failed: {fail_err}"
                )),
            };
        }
    };
    if !recorded {
        fail_build(store, &build, BuildStatus::Preparing, "build_state_changed")?;
        return Err(format!(
            "build {} left preparing state before its commit was recorded",
            build.id
        ));
    }

    let pipeline_path = checkout.workspace.join(".renkins.yml");
    let pipeline_yaml = match fs::read_to_string(&pipeline_path) {
        Ok(yaml) => yaml,
        Err(err) => {
            fail_build(
                store,
                &build,
                BuildStatus::Preparing,
                "pipeline_read_failed",
            )?;
            append_log(&log_path, "[build] failed to read .renkins.yml")?;
            return Err(format!(
                "failed to read pipeline for build {} at {}: {err}",
                build.id,
                pipeline_path.display()
            ));
        }
    };
    let pipeline = match parse_pipeline_yaml(&pipeline_yaml) {
        Ok(pipeline) => pipeline,
        Err(err) => {
            fail_build(store, &build, BuildStatus::Preparing, "pipeline_invalid")?;
            append_log(&log_path, "[build] pipeline validation failed")?;
            return Err(format!("invalid pipeline for build {}: {err:?}", build.id));
        }
    };
    if cancellation_is_requested(store, &build, cancellation_requested)? {
        return finish_cancelled(store, &build, data_dir);
    }

    let started = store
        .lock()
        .map_err(|_| "database store lock is poisoned".to_string())?
        .persist_pipeline_and_start(&build.id, &pipeline_yaml);
    let started = match started {
        Ok(started) => started,
        Err(err) => {
            let fail_result = fail_build(
                store,
                &build,
                BuildStatus::Preparing,
                "pipeline_persistence_failed",
            );
            return match fail_result {
                Ok(()) => Err(err),
                Err(fail_err) => Err(format!(
                    "{err}; also failed to mark build failed: {fail_err}"
                )),
            };
        }
    };
    if !started {
        if cancellation_is_requested(store, &build, cancellation_requested)? {
            return finish_cancelled(store, &build, data_dir);
        }
        return Err(format!(
            "pipeline snapshot was not committed for build {}; refusing to execute steps",
            build.id
        ));
    }

    if let Err(err) = append_log(
        &log_path,
        "[build] pipeline snapshot committed; executing steps",
    ) {
        fail_build(store, &build, BuildStatus::Running, "log_write_failed")?;
        return Err(err);
    }
    let timeout_seconds = pipeline
        .timeout_minutes
        .map(|minutes| minutes.saturating_mul(60));
    let outcomes = match executor::execute_pipeline_with_output_limit(
        &checkout.workspace,
        &pipeline.steps,
        &log_path,
        timeout_seconds,
        cancellation_requested,
        max_build_output_bytes,
    ) {
        Ok(outcomes) => outcomes,
        Err(err) => {
            fail_build(store, &build, BuildStatus::Running, "executor_failed")?;
            append_log(&log_path, "[build] step execution failed")?;
            return Err(format!(
                "step execution failed for build {}: {err:?}",
                build.id
            ));
        }
    };

    let persisted_outcomes = outcomes
        .iter()
        .enumerate()
        .map(|(index, outcome)| {
            let status = match outcome.status {
                StepStatus::Succeeded => "succeeded",
                StepStatus::Failed => "failed",
                StepStatus::TimedOut => "timed_out",
                StepStatus::Cancelled => "cancelled",
                StepStatus::OutputLimitExceeded => "failed",
            };
            StepOutcomeRecord {
                step_index: index,
                status,
                exit_code: outcome.exit_code,
                started_at: outcome.started_at,
                finished_at: outcome.finished_at,
            }
        })
        .collect::<Vec<_>>();
    let outcome_write = store
        .lock()
        .map_err(|_| "database store lock is poisoned".to_string())?
        .record_step_outcomes(&build.id, &persisted_outcomes);
    if let Err(err) = outcome_write {
        let fail_result = fail_build(
            store,
            &build,
            BuildStatus::Running,
            "outcome_persistence_failed",
        );
        return match fail_result {
            Ok(()) => Err(err),
            Err(fail_err) => Err(format!(
                "{err}; also failed to mark build failed: {fail_err}"
            )),
        };
    }

    let output_limit_exceeded = outcomes
        .iter()
        .any(|outcome| outcome.status == StepStatus::OutputLimitExceeded);
    let failed = outcomes.iter().any(|outcome| {
        matches!(
            outcome.status,
            StepStatus::Failed | StepStatus::TimedOut | StepStatus::OutputLimitExceeded
        )
    });
    let cancelled = outcomes
        .iter()
        .any(|outcome| outcome.status == StepStatus::Cancelled);
    let (next, failure_kind) = if failed {
        let timed_out = outcomes
            .iter()
            .any(|outcome| outcome.status == StepStatus::TimedOut);
        (
            BuildStatus::Failed,
            Some(if output_limit_exceeded {
                "log_limit"
            } else if timed_out {
                "step_timed_out"
            } else {
                "step_failed"
            }),
        )
    } else {
        (BuildStatus::Succeeded, None)
    };
    let terminal = finish_execution(
        store,
        &build,
        if cancelled {
            BuildStatus::Cancelled
        } else {
            next
        },
        failure_kind,
    )?;

    let output_limit_failed = output_limit_exceeded && terminal == BuildStatus::Failed;
    append_log(
        &log_path,
        if output_limit_failed {
            "[build] output limit exceeded; remaining output truncated"
        } else if terminal == BuildStatus::Cancelled {
            "[build] cancelled"
        } else if failed {
            "[build] failed"
        } else {
            "[build] succeeded"
        },
    )?;
    Ok(())
}

fn cancellation_is_requested(
    store: &Arc<Mutex<DatabaseStore>>,
    build: &Build,
    cancellation_requested: &AtomicBool,
) -> Result<bool, String> {
    if cancellation_requested.load(Ordering::Acquire) {
        return Ok(true);
    }
    let status = store
        .lock()
        .map_err(|_| "database store lock is poisoned".to_string())?
        .get_build(&build.id)?
        .map(|current| current.status);
    if status == Some(BuildStatus::Cancelling) {
        cancellation_requested.store(true, Ordering::Release);
        Ok(true)
    } else {
        Ok(false)
    }
}

fn finish_cancelled(
    store: &Arc<Mutex<DatabaseStore>>,
    build: &Build,
    data_dir: &Path,
) -> Result<(), String> {
    let terminal = finish_execution(store, build, BuildStatus::Cancelled, None)?;
    if terminal == BuildStatus::Cancelled {
        append_log(
            &data_dir.join("logs").join(format!("{}.rkl", build.id)),
            "[build] cancelled",
        )?;
    }
    Ok(())
}

fn finish_execution(
    store: &Arc<Mutex<DatabaseStore>>,
    build: &Build,
    requested_status: BuildStatus,
    failure_kind: Option<&str>,
) -> Result<BuildStatus, String> {
    let store = store
        .lock()
        .map_err(|_| "database store lock is poisoned".to_string())?;
    let current = store
        .get_build(&build.id)?
        .ok_or_else(|| format!("build {} disappeared", build.id))?;
    let (expected, next, failure_kind) = if current.status == BuildStatus::Cancelling {
        (BuildStatus::Cancelling, BuildStatus::Cancelled, None)
    } else if current.status == BuildStatus::Running && requested_status != BuildStatus::Cancelled {
        (BuildStatus::Running, requested_status, failure_kind)
    } else if current.status == BuildStatus::Running {
        let cancelling =
            store.transition_build(&build.id, BuildStatus::Running, BuildStatus::Cancelling)?;
        if !cancelling {
            return Err(format!(
                "build {} could not enter cancelling state",
                build.id
            ));
        }
        let cancelled =
            store.transition_build(&build.id, BuildStatus::Cancelling, BuildStatus::Cancelled)?;
        if !cancelled {
            return Err(format!(
                "build {} could not enter cancelled state",
                build.id
            ));
        }
        return Ok(BuildStatus::Cancelled);
    } else {
        return Ok(current.status);
    };
    let transitioned =
        store.transition_build_with_failure(&build.id, expected, next, failure_kind)?;
    if !transitioned {
        return Err(format!(
            "build {} could not transition from {expected:?} to {next:?}",
            build.id
        ));
    }
    Ok(next)
}

fn fail_build(
    store: &Arc<Mutex<DatabaseStore>>,
    build: &Build,
    expected: BuildStatus,
    failure_kind: &str,
) -> Result<(), String> {
    let store = store
        .lock()
        .map_err(|_| "database store lock is poisoned".to_string())?;
    let current = store
        .get_build(&build.id)?
        .ok_or_else(|| format!("build {} disappeared", build.id))?;
    if current.status == BuildStatus::Cancelling {
        return store
            .transition_build(&build.id, BuildStatus::Cancelling, BuildStatus::Cancelled)
            .and_then(|transitioned| {
                transitioned
                    .then_some(())
                    .ok_or_else(|| format!("build {} could not be cancelled", build.id))
            });
    }
    let transitioned = store.transition_build_with_failure(
        &build.id,
        expected,
        BuildStatus::Failed,
        Some(failure_kind),
    )?;
    transitioned
        .then_some(())
        .ok_or_else(|| format!("build {} could not be marked failed", build.id))
}

fn append_log(path: &Path, message: &str) -> Result<(), String> {
    let log = BuildLog::open_append(path)?;
    log.append(None, LogStream::Build, format!("{message}\n").as_bytes())?;
    log.sync()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::Job;
    use crate::storage::DatabaseStore;
    use std::process::Command;
    use std::time::Instant;
    use tempfile::tempdir;

    fn git(repository: &Path, args: &[&str]) {
        let output = Command::new("git")
            .current_dir(repository)
            .args(args)
            .output()
            .expect("git should start");
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn fixture(pipeline: &str) -> (tempfile::TempDir, Arc<Mutex<DatabaseStore>>) {
        let dir = tempdir().unwrap();
        let repository = dir.path().join("repository");
        fs::create_dir_all(&repository).unwrap();
        git(&repository, &["init", "-b", "main"]);
        git(
            &repository,
            &["config", "user.email", "worker@example.test"],
        );
        git(&repository, &["config", "user.name", "Worker Test"]);
        fs::write(repository.join(".renkins.yml"), pipeline).unwrap();
        git(&repository, &["add", ".renkins.yml"]);
        git(&repository, &["commit", "-m", "add pipeline"]);

        let mut db = DatabaseStore::open(dir.path().join("worker.sqlite3")).unwrap();
        db.initialize_schema().unwrap();
        db.create_job(&Job {
            id: "job-1".to_string(),
            name: "worker test".to_string(),
            repository_url: repository.to_string_lossy().into_owned(),
            default_ref: Some("refs/heads/main".to_string()),
            disabled: false,
        })
        .unwrap();
        db.enqueue_build("build-1", "job-1", "refs/heads/main")
            .unwrap();
        (dir, Arc::new(Mutex::new(db)))
    }

    fn wait_for_status(
        store: &Arc<Mutex<DatabaseStore>>,
        build_id: &str,
        status: BuildStatus,
    ) -> Build {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let build = store.lock().unwrap().get_build(build_id).unwrap().unwrap();
            if build.status == status {
                return build;
            }
            assert!(
                Instant::now() < deadline,
                "build {build_id} did not reach {status:?}; last status: {:?}",
                build.status
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn claims_checks_out_executes_and_persists_build_results() {
        let pipeline = r#"
version: 1
steps:
  - name: create-marker
    run: printf done; printf done > marker.txt
"#;
        let (dir, store) = fixture(pipeline);

        assert!(run_one_build(&store, dir.path()).unwrap());
        assert!(!run_one_build(&store, dir.path()).unwrap());

        let build = store.lock().unwrap().get_build("build-1").unwrap().unwrap();
        assert_eq!(build.status, BuildStatus::Succeeded);
        assert!(build.started_at.is_some());
        assert!(build.finished_at.is_some());
        assert!(
            build
                .finished_at
                .zip(build.started_at)
                .is_some_and(|(finished, started)| finished >= started)
        );
        let checkout = dir.path().join("workspaces/build-1/checkout");
        assert_eq!(
            fs::read_to_string(checkout.join("marker.txt")).unwrap(),
            "done"
        );
        assert!(
            store
                .lock()
                .unwrap()
                .get_pipeline_snapshot("build-1")
                .unwrap()
                .is_some()
        );

        let steps = store.lock().unwrap().get_build_steps("build-1").unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].status, "succeeded");
        assert!(steps[0].exit_code.is_some());
        assert!(steps[0].started_at.is_some());
        assert!(
            steps[0]
                .finished_at
                .zip(steps[0].started_at)
                .is_some_and(|(finished, started)| finished >= started)
        );
        let log_page =
            crate::logs::read_page(&dir.path().join("logs/build-1.rkl"), None, 100, true)
                .unwrap()
                .unwrap();
        assert_eq!(log_page.integrity, "valid");
        assert!(
            log_page
                .records
                .iter()
                .any(|record| record.stream == "stdout" && record.payload == b"done")
        );
    }

    #[test]
    fn output_limit_fails_build_and_records_truncation_marker() {
        let (dir, store) = fixture(
            "version: 1\nsteps:\n  - name: large-output\n    run: head -c 100000 /dev/zero\n",
        );

        assert!(
            run_one_build_with_output_limit(&store, dir.path(), &WorkerControl::default(), 2048,)
                .unwrap()
        );

        let store = store.lock().unwrap();
        assert_eq!(
            store.get_build("build-1").unwrap().unwrap().status,
            BuildStatus::Failed
        );
        assert_eq!(
            store.get_build_failure_kind("build-1").unwrap().as_deref(),
            Some("log_limit")
        );
        let steps = store.get_build_steps("build-1").unwrap();
        assert_eq!(steps[0].status, "failed");
        assert!(steps[0].started_at.is_some());
        drop(store);

        let page = crate::logs::read_page(&dir.path().join("logs/build-1.rkl"), None, 100, false)
            .unwrap()
            .unwrap();
        assert_eq!(
            page.records
                .iter()
                .filter(|record| record.stream == "stdout" || record.stream == "stderr")
                .map(|record| record.payload.len())
                .sum::<usize>(),
            2048
        );
        assert!(page.records.iter().any(|record| {
            record.stream == "build"
                && record.payload == b"[build] output limit exceeded; remaining output truncated\n"
        }));
    }

    #[test]
    fn invalid_pipeline_fails_before_any_step_command_runs() {
        let (dir, store) = fixture("version: 2\nsteps: []\n");

        assert!(run_one_build(&store, dir.path()).is_err());

        let build = store.lock().unwrap().get_build("build-1").unwrap().unwrap();
        assert_eq!(build.status, BuildStatus::Failed);
        assert!(
            !dir.path()
                .join("workspaces/build-1/checkout/marker.txt")
                .exists()
        );
    }

    #[test]
    fn persistence_failure_prevents_step_execution() {
        let pipeline = r#"
version: 1
steps:
  - name: create-marker
    run: printf done > marker.txt
"#;
        let (dir, store) = fixture(pipeline);
        let connection = rusqlite::Connection::open(dir.path().join("worker.sqlite3")).unwrap();
        connection
            .execute_batch(
                "CREATE TRIGGER reject_worker_steps
                 BEFORE INSERT ON build_steps
                 BEGIN
                     SELECT RAISE(ABORT, 'injected persistence failure');
                 END;",
            )
            .unwrap();
        drop(connection);

        assert!(run_one_build(&store, dir.path()).is_err());

        let build = store.lock().unwrap().get_build("build-1").unwrap().unwrap();
        assert_eq!(build.status, BuildStatus::Failed);
        assert!(
            store
                .lock()
                .unwrap()
                .get_pipeline_snapshot("build-1")
                .unwrap()
                .is_none()
        );
        assert!(
            !dir.path()
                .join("workspaces/build-1/checkout/marker.txt")
                .exists()
        );
    }

    #[test]
    fn cancellation_request_stops_a_running_pipeline_and_finishes_build() {
        let pipeline = r#"
version: 1
steps:
  - name: wait
    run: sleep 10
"#;
        let (dir, store) = fixture(pipeline);
        let control = WorkerControl::default();
        let worker_store = Arc::clone(&store);
        let worker_control = control.clone();
        let data_dir = dir.path().to_path_buf();
        let worker = thread::spawn(move || {
            run_one_build_with_control(&worker_store, &data_dir, &worker_control)
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let build = store.lock().unwrap().get_build("build-1").unwrap().unwrap();
            if build.status == BuildStatus::Running {
                break;
            }
            assert!(Instant::now() < deadline, "worker did not start the build");
            thread::sleep(Duration::from_millis(10));
        }

        let cancellation = store
            .lock()
            .unwrap()
            .request_build_cancellation("build-1", BuildCancellationReason::User)
            .unwrap()
            .unwrap();
        assert_eq!(cancellation.status, BuildStatus::Cancelling);
        assert!(control.cancel_active("build-1").unwrap());
        assert!(worker.join().unwrap().unwrap());

        let build = store.lock().unwrap().get_build("build-1").unwrap().unwrap();
        assert_eq!(build.status, BuildStatus::Cancelled);
        let steps = store.lock().unwrap().get_build_steps("build-1").unwrap();
        assert_eq!(steps[0].status, "cancelled");
    }

    #[test]
    fn worker_pool_respects_a_single_build_concurrency_limit() {
        let pipeline = "version: 1\nsteps:\n  - name: wait\n    run: sleep 0.15\n";
        let (dir, store) = fixture(pipeline);
        store
            .lock()
            .unwrap()
            .enqueue_build("build-2", "job-1", "refs/heads/main")
            .unwrap();
        let worker = start_worker_pool(
            Arc::clone(&store),
            dir.path().to_path_buf(),
            WorkerControl::default(),
            1,
        )
        .unwrap();

        wait_for_status(&store, "build-2", BuildStatus::Succeeded);
        worker.shutdown(Duration::from_secs(1)).unwrap();

        let store = store.lock().unwrap();
        let first = store.get_build_steps("build-1").unwrap().remove(0);
        let second = store.get_build_steps("build-2").unwrap().remove(0);
        assert!(
            first
                .finished_at
                .zip(second.started_at)
                .is_some_and(|(first_finished, second_started)| second_started >= first_finished)
        );
    }

    #[test]
    fn worker_pool_runs_builds_concurrently_when_multiple_slots_are_configured() {
        let (dir, store) = fixture("version: 1\nsteps:\n  - name: wait\n    run: sleep 0.5\n");
        store
            .lock()
            .unwrap()
            .enqueue_build("build-2", "job-1", "refs/heads/main")
            .unwrap();
        let worker = start_worker_pool(
            Arc::clone(&store),
            dir.path().to_path_buf(),
            WorkerControl::default(),
            2,
        )
        .unwrap();

        wait_for_status(&store, "build-2", BuildStatus::Succeeded);
        worker.shutdown(Duration::from_secs(1)).unwrap();

        let store = store.lock().unwrap();
        let first = store.get_build_steps("build-1").unwrap().remove(0);
        let second = store.get_build_steps("build-2").unwrap().remove(0);
        assert!(
            first
                .started_at
                .zip(first.finished_at)
                .zip(second.started_at.zip(second.finished_at))
                .is_some_and(
                    |((first_started, first_finished), (second_started, second_finished))| {
                        first_started < second_finished && second_started < first_finished
                    }
                )
        );
    }

    #[test]
    fn worker_shutdown_allows_active_builds_to_finish_within_grace() {
        let (dir, store) = fixture("version: 1\nsteps:\n  - name: wait\n    run: sleep 0.1\n");
        let worker = start_worker_pool(
            Arc::clone(&store),
            dir.path().to_path_buf(),
            WorkerControl::default(),
            1,
        )
        .unwrap();
        wait_for_status(&store, "build-1", BuildStatus::Running);

        worker.shutdown(Duration::from_secs(2)).unwrap();
        assert_eq!(
            store
                .lock()
                .unwrap()
                .get_build("build-1")
                .unwrap()
                .unwrap()
                .status,
            BuildStatus::Succeeded
        );
    }

    #[test]
    fn worker_shutdown_cancels_after_grace_and_leaves_queued_work_unclaimed() {
        let (dir, store) = fixture("version: 1\nsteps:\n  - name: wait\n    run: sleep 10\n");
        store
            .lock()
            .unwrap()
            .enqueue_build("build-2", "job-1", "refs/heads/main")
            .unwrap();
        let worker = start_worker_pool(
            Arc::clone(&store),
            dir.path().to_path_buf(),
            WorkerControl::default(),
            1,
        )
        .unwrap();
        wait_for_status(&store, "build-1", BuildStatus::Running);

        worker.shutdown(Duration::from_millis(50)).unwrap();
        let store = store.lock().unwrap();
        let cancelled = store.get_build("build-1").unwrap().unwrap();
        assert_eq!(cancelled.status, BuildStatus::Cancelled);
        assert_eq!(cancelled.cancel_reason.as_deref(), Some("server_shutdown"));
        assert!(cancelled.cancel_requested_at.is_some());
        assert_eq!(
            store.get_build("build-2").unwrap().unwrap().status,
            BuildStatus::Queued
        );
    }
}
