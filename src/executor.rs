use crate::logs::{BuildLog, LogStream, MAX_RECORD_PAYLOAD};
use crate::pipeline::StepConfig;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{io, os::unix::process::CommandExt};

const PROCESS_TERMINATION_GRACE: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StepStatus {
    Succeeded,
    Failed,
    TimedOut,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepOutcome {
    pub name: String,
    pub command: String,
    pub status: StepStatus,
    pub exit_code: Option<i32>,
    pub log_path: PathBuf,
    pub started_at: Option<i64>,
    pub finished_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorError {
    SpawnFailed(String),
    InvalidWorkingDirectory,
    InvalidTimeout,
    ClockError(String),
    LogWriteFailed(String),
    CommandFailed(i32),
    TimedOut,
}

fn read_stream<R: std::io::Read + Send + 'static>(
    reader: R,
    log: BuildLog,
    step_index: u32,
    stream: LogStream,
) -> thread::JoinHandle<Result<(), ExecutorError>> {
    thread::spawn(move || {
        let mut reader = reader;
        let mut buffer = [0; MAX_RECORD_PAYLOAD];
        loop {
            let bytes = reader
                .read(&mut buffer)
                .map_err(|err| ExecutorError::LogWriteFailed(err.to_string()))?;
            if bytes == 0 {
                break;
            }
            log.append(Some(step_index), stream, &buffer[..bytes])
                .map_err(ExecutorError::LogWriteFailed)?;
        }
        Ok(())
    })
}

pub fn run_step(
    step: &StepConfig,
    workspace: &Path,
    log_path: &Path,
    timeout_seconds: Option<u64>,
) -> Result<StepOutcome, ExecutorError> {
    let log = BuildLog::open_append(log_path).map_err(ExecutorError::LogWriteFailed)?;
    let cancellation_requested = AtomicBool::new(false);
    run_step_with_log(
        step,
        workspace,
        log_path,
        &log,
        0,
        timeout_seconds,
        &cancellation_requested,
    )
}

fn run_step_with_log(
    step: &StepConfig,
    workspace: &Path,
    log_path: &Path,
    log: &BuildLog,
    step_index: u32,
    timeout_seconds: Option<u64>,
    cancellation_requested: &AtomicBool,
) -> Result<StepOutcome, ExecutorError> {
    let deadline = match timeout_seconds {
        Some(seconds) => Some(
            Instant::now()
                .checked_add(Duration::from_secs(seconds))
                .ok_or(ExecutorError::InvalidTimeout)?,
        ),
        None => None,
    };
    if cancellation_requested.load(Ordering::Acquire) {
        return step_outcome(step, log_path, StepStatus::Cancelled, None, None);
    }
    let started_at = timestamp_ms()?;
    let mut command = Command::new("/bin/sh");
    command
        .current_dir(
            step.working_directory
                .as_deref()
                .map(|directory| workspace.join(directory))
                .unwrap_or_else(|| workspace.to_path_buf()),
        )
        .arg("-eu")
        .arg("-c")
        .arg(&step.run)
        .envs(&step.env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);

    let mut child = command
        .spawn()
        .map_err(|err| ExecutorError::SpawnFailed(err.to_string()))?;

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let stdout_thread = read_stream(stdout, log.clone(), step_index, LogStream::Stdout);
    let stderr_thread = read_stream(stderr, log.clone(), step_index, LogStream::Stderr);
    let process_group = child.id();

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                terminate_process_group(process_group)?;
                join_streams(stdout_thread, stderr_thread)?;
                let code = status.code().unwrap_or(-1);
                return step_outcome(
                    step,
                    log_path,
                    if cancellation_requested.load(Ordering::Acquire) {
                        StepStatus::Cancelled
                    } else if status.success() {
                        StepStatus::Succeeded
                    } else {
                        StepStatus::Failed
                    },
                    Some(code),
                    Some(started_at),
                );
            }
            Ok(None) => {
                if cancellation_requested.load(Ordering::Acquire) {
                    terminate_process_group(process_group)?;
                    child
                        .wait()
                        .map_err(|err| ExecutorError::SpawnFailed(err.to_string()))?;
                    join_streams(stdout_thread, stderr_thread)?;
                    return step_outcome(
                        step,
                        log_path,
                        StepStatus::Cancelled,
                        None,
                        Some(started_at),
                    );
                }
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    terminate_process_group(process_group)?;
                    child
                        .wait()
                        .map_err(|err| ExecutorError::SpawnFailed(err.to_string()))?;
                    join_streams(stdout_thread, stderr_thread)?;
                    return step_outcome(
                        step,
                        log_path,
                        StepStatus::TimedOut,
                        None,
                        Some(started_at),
                    );
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(err) => {
                terminate_process_group(process_group)?;
                child
                    .wait()
                    .map_err(|wait_err| ExecutorError::SpawnFailed(wait_err.to_string()))?;
                join_streams(stdout_thread, stderr_thread)?;
                return Err(ExecutorError::SpawnFailed(err.to_string()));
            }
        }
    }
}

fn step_outcome(
    step: &StepConfig,
    log_path: &Path,
    status: StepStatus,
    exit_code: Option<i32>,
    started_at: Option<i64>,
) -> Result<StepOutcome, ExecutorError> {
    Ok(StepOutcome {
        name: step.name.clone(),
        command: step.run.clone(),
        status,
        exit_code,
        log_path: log_path.to_path_buf(),
        started_at,
        finished_at: timestamp_ms()?,
    })
}

fn timestamp_ms() -> Result<i64, ExecutorError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| ExecutorError::ClockError(err.to_string()))?
        .as_millis()
        .try_into()
        .map_err(|err| ExecutorError::ClockError(format!("timestamp is out of range: {err}")))
}

fn terminate_process_group(process_group: u32) -> Result<(), ExecutorError> {
    if !signal_process_group(process_group, libc::SIGTERM)? {
        return Ok(());
    }
    thread::sleep(PROCESS_TERMINATION_GRACE);
    signal_process_group(process_group, libc::SIGKILL)?;
    Ok(())
}

fn signal_process_group(process_group: u32, signal: i32) -> Result<bool, ExecutorError> {
    let process_group = libc::pid_t::try_from(process_group)
        .map_err(|err| ExecutorError::SpawnFailed(err.to_string()))?;
    let result = unsafe { libc::kill(-process_group, signal) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(false)
    } else {
        Err(ExecutorError::SpawnFailed(format!(
            "failed to signal process group {process_group}: {error}"
        )))
    }
}

fn join_streams(
    stdout_thread: thread::JoinHandle<Result<(), ExecutorError>>,
    stderr_thread: thread::JoinHandle<Result<(), ExecutorError>>,
) -> Result<(), ExecutorError> {
    let stdout_result = stdout_thread
        .join()
        .map_err(|_| ExecutorError::LogWriteFailed("stdout reader thread panicked".to_string()))?;
    let stderr_result = stderr_thread
        .join()
        .map_err(|_| ExecutorError::LogWriteFailed("stderr reader thread panicked".to_string()))?;
    stdout_result?;
    stderr_result
}

pub fn execute_pipeline(
    workspace: &Path,
    steps: &[StepConfig],
    log_path: &Path,
    timeout_seconds: Option<u64>,
) -> Result<Vec<StepOutcome>, ExecutorError> {
    let cancellation_requested = AtomicBool::new(false);
    execute_pipeline_with_cancellation(
        workspace,
        steps,
        log_path,
        timeout_seconds,
        &cancellation_requested,
    )
}

pub fn execute_pipeline_with_cancellation(
    workspace: &Path,
    steps: &[StepConfig],
    log_path: &Path,
    timeout_seconds: Option<u64>,
    cancellation_requested: &AtomicBool,
) -> Result<Vec<StepOutcome>, ExecutorError> {
    let log = BuildLog::open_append(log_path).map_err(ExecutorError::LogWriteFailed)?;
    let mut results = Vec::with_capacity(steps.len());
    let build_deadline = match timeout_seconds {
        Some(seconds) => Some(
            Instant::now()
                .checked_add(Duration::from_secs(seconds))
                .ok_or(ExecutorError::InvalidTimeout)?,
        ),
        None => None,
    };

    for (index, step) in steps.iter().enumerate() {
        let step_index =
            u32::try_from(index).map_err(|err| ExecutorError::LogWriteFailed(err.to_string()))?;
        log.append(
            Some(step_index),
            LogStream::Build,
            format!("[step] {}\n", step.name).as_bytes(),
        )
        .map_err(ExecutorError::LogWriteFailed)?;
        log.sync().map_err(ExecutorError::LogWriteFailed)?;
        let step_timeout = step
            .timeout_minutes
            .map(|minutes| minutes.saturating_mul(60));
        let build_time_remaining = build_deadline.map(|deadline| {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_default();
            remaining
                .as_secs()
                .saturating_add(u64::from(remaining.subsec_nanos() > 0))
        });
        let outcome = if cancellation_requested.load(Ordering::Acquire) {
            step_outcome(step, log_path, StepStatus::Cancelled, None, None)?
        } else if build_time_remaining == Some(0) {
            step_outcome(step, log_path, StepStatus::TimedOut, None, None)?
        } else {
            let timeout = match (step_timeout, build_time_remaining) {
                (Some(step), Some(build)) => Some(step.min(build)),
                (Some(step), None) => Some(step),
                (None, Some(build)) => Some(build),
                (None, None) => None,
            };
            run_step_with_log(
                step,
                workspace,
                log_path,
                &log,
                step_index,
                timeout,
                cancellation_requested,
            )?
        };
        let status = match outcome.status {
            StepStatus::Succeeded => "succeeded",
            StepStatus::Failed => "failed",
            StepStatus::TimedOut => "timed_out",
            StepStatus::Cancelled => "cancelled",
        };
        log.append(
            Some(step_index),
            LogStream::Build,
            format!("[step] {status}\n").as_bytes(),
        )
        .map_err(ExecutorError::LogWriteFailed)?;
        log.sync().map_err(ExecutorError::LogWriteFailed)?;
        results.push(outcome);
        if matches!(
            results.last().unwrap().status,
            StepStatus::Failed | StepStatus::TimedOut | StepStatus::Cancelled
        ) {
            break;
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;

    #[test]
    fn executes_steps_in_order_and_records_success() {
        let dir = tempdir().unwrap();
        let workspace = dir.path();
        let log_path = workspace.join("build.log");
        let steps = vec![
            StepConfig {
                name: "print".to_string(),
                run: "printf 'hello\\n'".to_string(),
                working_directory: None,
                timeout_minutes: None,
                env: Default::default(),
            },
            StepConfig {
                name: "write".to_string(),
                run: "printf 'done\\n' > output.txt".to_string(),
                working_directory: None,
                timeout_minutes: None,
                env: Default::default(),
            },
        ];

        let outcomes = execute_pipeline(workspace, &steps, &log_path, Some(10)).unwrap();
        assert_eq!(outcomes.len(), 2);
        assert!(matches!(outcomes[0].status, StepStatus::Succeeded));
        assert!(matches!(outcomes[1].status, StepStatus::Succeeded));
        assert!(workspace.join("output.txt").exists());
    }

    #[test]
    fn applies_step_working_directory_and_environment() {
        let dir = tempdir().unwrap();
        let workspace = dir.path();
        let working_directory = workspace.join("nested");
        std::fs::create_dir(&working_directory).unwrap();
        let log_path = workspace.join("configured.log");
        let step = StepConfig {
            name: "configured".to_string(),
            run: "printf '%s' \"$STEP_VALUE\" > output.txt".to_string(),
            working_directory: Some("nested".to_string()),
            timeout_minutes: None,
            env: [("STEP_VALUE".to_string(), "present".to_string())].into(),
        };

        let outcome = run_step(&step, workspace, &log_path, Some(5)).unwrap();

        assert_eq!(outcome.status, StepStatus::Succeeded);
        assert_eq!(
            std::fs::read_to_string(working_directory.join("output.txt")).unwrap(),
            "present"
        );
    }

    #[test]
    fn rejects_timeout_outside_monotonic_clock_range_before_spawning() {
        let dir = tempdir().unwrap();
        let marker = dir.path().join("unexpected");
        let step = StepConfig {
            name: "invalid-timeout".to_string(),
            run: format!("touch '{}'", marker.display()),
            working_directory: None,
            timeout_minutes: None,
            env: Default::default(),
        };

        assert!(matches!(
            run_step(
                &step,
                dir.path(),
                &dir.path().join("build.log"),
                Some(u64::MAX)
            ),
            Err(ExecutorError::InvalidTimeout)
        ));
        assert!(!marker.exists());
    }

    #[test]
    fn times_out_long_running_step() {
        let dir = tempdir().unwrap();
        let workspace = dir.path();
        let log_path = workspace.join("timeout.log");
        let step = StepConfig {
            name: "sleep".to_string(),
            run: "sleep 10".to_string(),
            working_directory: None,
            timeout_minutes: None,
            env: Default::default(),
        };

        let outcome = run_step(&step, workspace, &log_path, Some(1)).unwrap();
        assert!(matches!(outcome.status, StepStatus::TimedOut));
        assert_eq!(outcome.exit_code, None);
    }

    #[test]
    fn cancellation_terminates_the_step_process_group() {
        let dir = tempdir().unwrap();
        let marker = dir.path().join("cancelled-step-survived");
        let step = StepConfig {
            name: "cancel".to_string(),
            run: format!(
                "(sleep 0.7; echo unexpected > '{}') & sleep 10",
                marker.display()
            ),
            working_directory: None,
            timeout_minutes: None,
            env: Default::default(),
        };
        let cancellation = Arc::new(AtomicBool::new(false));
        let cancellation_signal = Arc::clone(&cancellation);
        let signal_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(150));
            cancellation_signal.store(true, Ordering::Release);
        });

        let started = Instant::now();
        let log_path = dir.path().join("cancel.log");
        let log = BuildLog::open_append(&log_path).unwrap();
        let outcome = run_step_with_log(
            &step,
            dir.path(),
            &log_path,
            &log,
            0,
            Some(5),
            &cancellation,
        )
        .unwrap();
        signal_thread.join().unwrap();

        assert_eq!(outcome.status, StepStatus::Cancelled);
        assert!(started.elapsed() < Duration::from_secs(2));
        thread::sleep(Duration::from_millis(800));
        assert!(!marker.exists());
    }

    #[test]
    fn terminates_same_group_descendants_that_inherit_output_pipes() {
        let dir = tempdir().unwrap();
        let workspace = dir.path();
        let log_path = workspace.join("descendant.log");
        let marker = workspace.join("descendant-survived");
        let step = StepConfig {
            name: "spawn-child".to_string(),
            run: format!(
                "(sleep 0.5; echo unexpected > '{}') & exit 0",
                marker.display()
            ),
            working_directory: None,
            timeout_minutes: None,
            env: Default::default(),
        };

        let started = Instant::now();
        let outcome = run_step(&step, workspace, &log_path, Some(3)).unwrap();

        assert_eq!(outcome.status, StepStatus::Succeeded);
        assert!(started.elapsed() < Duration::from_millis(400));
        thread::sleep(Duration::from_millis(600));
        assert!(!marker.exists());
    }

    #[test]
    fn timeout_terminates_same_group_descendants() {
        let dir = tempdir().unwrap();
        let workspace = dir.path();
        let log_path = workspace.join("timeout-descendant.log");
        let marker = workspace.join("timeout-descendant-survived");
        let step = StepConfig {
            name: "spawn-child".to_string(),
            run: format!(
                "(sleep 1.3; echo unexpected > '{}') & sleep 10",
                marker.display()
            ),
            working_directory: None,
            timeout_minutes: None,
            env: Default::default(),
        };

        let outcome = run_step(&step, workspace, &log_path, Some(1)).unwrap();

        assert_eq!(outcome.status, StepStatus::TimedOut);
        thread::sleep(Duration::from_millis(500));
        assert!(!marker.exists());
    }
}
