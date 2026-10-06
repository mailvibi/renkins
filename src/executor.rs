use crate::pipeline::StepConfig;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutorError {
    SpawnFailed(String),
    InvalidWorkingDirectory,
    LogWriteFailed(String),
    CommandFailed(i32),
    TimedOut,
}

pub fn ensure_log_file(path: &Path) -> Result<(), ExecutorError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| ExecutorError::LogWriteFailed(err.to_string()))?;
    }
    File::create(path).map_err(|err| ExecutorError::LogWriteFailed(err.to_string()))?;
    Ok(())
}

fn append_line(path: &Path, message: &str) -> Result<(), ExecutorError> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| ExecutorError::LogWriteFailed(err.to_string()))?;
    writeln!(file, "{message}").map_err(|err| ExecutorError::LogWriteFailed(err.to_string()))?;
    file.flush()
        .map_err(|err| ExecutorError::LogWriteFailed(err.to_string()))?;
    Ok(())
}

fn read_stream<R: std::io::Read + Send + 'static>(
    reader: R,
    log_path: PathBuf,
    prefix: String,
) -> thread::JoinHandle<Result<(), ExecutorError>> {
    thread::spawn(move || {
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        loop {
            line.clear();
            let bytes = reader
                .read_line(&mut line)
                .map_err(|err| ExecutorError::LogWriteFailed(err.to_string()))?;
            if bytes == 0 {
                break;
            }
            let payload = format!("[{prefix}] {}", line.trim_end_matches(['\r', '\n']));
            append_line(&log_path, &payload)?;
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
    let mut command = Command::new("/bin/sh");
    command
        .current_dir(workspace)
        .arg("-eu")
        .arg("-c")
        .arg(&step.run)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|err| ExecutorError::SpawnFailed(err.to_string()))?;

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let stdout_log = log_path.to_path_buf();
    let stderr_log = log_path.to_path_buf();
    let stdout_thread = read_stream(stdout, stdout_log, "stdout".to_string());
    let stderr_thread = read_stream(stderr, stderr_log, "stderr".to_string());

    let timeout = timeout_seconds.map(Duration::from_secs);
    let deadline = timeout.map(|d| Instant::now() + d);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let _ = stdout_thread.join();
                let _ = stderr_thread.join();
                let code = status.code().unwrap_or(-1);
                return Ok(StepOutcome {
                    name: step.name.clone(),
                    command: step.run.clone(),
                    status: if status.success() {
                        StepStatus::Succeeded
                    } else {
                        StepStatus::Failed
                    },
                    exit_code: Some(code),
                    log_path: log_path.to_path_buf(),
                });
            }
            Ok(None) => {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    let _ = child.kill();
                    let _ = child.wait();
                    let _ = stdout_thread.join();
                    let _ = stderr_thread.join();
                    return Ok(StepOutcome {
                        name: step.name.clone(),
                        command: step.run.clone(),
                        status: StepStatus::TimedOut,
                        exit_code: None,
                        log_path: log_path.to_path_buf(),
                    });
                }
                thread::sleep(Duration::from_millis(20));
            }
            Err(err) => {
                return Err(ExecutorError::SpawnFailed(err.to_string()));
            }
        }
    }
}

pub fn execute_pipeline(
    workspace: &Path,
    steps: &[StepConfig],
    log_path: &Path,
    timeout_seconds: Option<u64>,
) -> Result<Vec<StepOutcome>, ExecutorError> {
    ensure_log_file(log_path)?;
    let mut results = Vec::with_capacity(steps.len());

    for step in steps {
        let step_log = log_path.to_path_buf();
        append_line(&step_log, &format!("[step] {}", step.name))?;
        let outcome = run_step(step, workspace, &step_log, timeout_seconds)?;
        results.push(outcome);
        if matches!(
            results.last().unwrap().status,
            StepStatus::Failed | StepStatus::TimedOut
        ) {
            break;
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
