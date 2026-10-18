use crate::domain::{Build, BuildStatus};
use crate::executor::{self, StepStatus};
use crate::git;
use crate::logs::{BuildLog, LogStream};
use crate::pipeline::parse_pipeline_yaml;
use crate::storage::DatabaseStore;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const POLL_INTERVAL: Duration = Duration::from_millis(250);

pub fn start_worker(store: Arc<Mutex<DatabaseStore>>, data_dir: PathBuf) -> Result<(), String> {
    thread::Builder::new()
        .name("renkins-worker".to_string())
        .spawn(move || {
            loop {
                match run_one_build(&store, &data_dir) {
                    Ok(true) => {}
                    Ok(false) => thread::sleep(POLL_INTERVAL),
                    Err(err) => {
                        eprintln!("worker iteration failed: {err}");
                        thread::sleep(POLL_INTERVAL);
                    }
                }
            }
        })
        .map(|_| ())
        .map_err(|err| format!("failed to start build worker thread: {err}"))
}

pub fn run_one_build(store: &Arc<Mutex<DatabaseStore>>, data_dir: &Path) -> Result<bool, String> {
    let build = {
        let mut store = store
            .lock()
            .map_err(|_| "database store lock is poisoned".to_string())?;
        store.claim_next_build()?
    };
    let Some(build) = build else {
        return Ok(false);
    };

    process_build(store, data_dir, build)?;
    Ok(true)
}

fn process_build(
    store: &Arc<Mutex<DatabaseStore>>,
    data_dir: &Path,
    build: Build,
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
    let checkout =
        match git::checkout_repository(&job.repository_url, &workspace, &build.requested_ref) {
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
    let outcomes = match executor::execute_pipeline(
        &checkout.workspace,
        &pipeline.steps,
        &log_path,
        timeout_seconds,
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
            };
            (index, status, outcome.exit_code)
        })
        .collect::<Vec<_>>();
    let outcome_write = store
        .lock()
        .map_err(|_| "database store lock is poisoned".to_string())?
        .record_step_outcomes(
            &build.id,
            &persisted_outcomes
                .iter()
                .map(|(index, status, code)| (*index, *status, *code))
                .collect::<Vec<_>>(),
        );
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

    let failed = outcomes
        .iter()
        .any(|outcome| matches!(outcome.status, StepStatus::Failed | StepStatus::TimedOut));
    let (next, failure_kind) = if failed {
        let timed_out = outcomes
            .iter()
            .any(|outcome| outcome.status == StepStatus::TimedOut);
        (
            BuildStatus::Failed,
            Some(if timed_out {
                "step_timed_out"
            } else {
                "step_failed"
            }),
        )
    } else {
        (BuildStatus::Succeeded, None)
    };
    let transitioned = store
        .lock()
        .map_err(|_| "database store lock is poisoned".to_string())?
        .transition_build_with_failure(&build.id, BuildStatus::Running, next, failure_kind)?;
    if !transitioned {
        return Err(format!(
            "build {} could not transition to its terminal state",
            build.id
        ));
    }

    append_log(
        &log_path,
        if failed {
            "[build] failed"
        } else {
            "[build] succeeded"
        },
    )?;
    Ok(())
}

fn fail_build(
    store: &Arc<Mutex<DatabaseStore>>,
    build: &Build,
    expected: BuildStatus,
    failure_kind: &str,
) -> Result<(), String> {
    let transitioned = store
        .lock()
        .map_err(|_| "database store lock is poisoned".to_string())?
        .transition_build_with_failure(
            &build.id,
            expected,
            BuildStatus::Failed,
            Some(failure_kind),
        )?;
    if transitioned {
        Ok(())
    } else {
        Err(format!(
            "build {} could not be marked failed after {failure_kind}",
            build.id
        ))
    }
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
}
