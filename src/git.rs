use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use std::{os::unix::process::CommandExt, process::Output};

const CLONE_TIMEOUT: Duration = Duration::from_secs(300);
const GIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
const PROCESS_TERMINATION_GRACE: Duration = Duration::from_millis(100);
const MAX_CAPTURED_OUTPUT: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitRefError {
    Empty,
    InvalidSyntax,
    InvalidCommitId,
    RefNotFound,
    NotACommit,
    CommandFailed(String),
    TimedOut,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutResult {
    pub workspace: PathBuf,
    pub resolved_commit: String,
}

pub fn validate_ref(reference: &str) -> Result<(), GitRefError> {
    let reference = reference.trim();
    if reference.is_empty() {
        return Err(GitRefError::Empty);
    }

    if reference.starts_with("refs/heads/") || reference.starts_with("refs/tags/") {
        let suffix = reference
            .strip_prefix("refs/heads/")
            .or_else(|| reference.strip_prefix("refs/tags/"))
            .unwrap();
        if suffix.is_empty()
            || suffix.starts_with('/')
            || suffix.ends_with('/')
            || suffix.contains("//")
            || suffix.contains("..")
            || suffix.contains('~')
            || suffix.contains('^')
            || suffix.contains(':')
            || suffix.contains('?')
            || suffix.contains('*')
            || suffix.contains('[')
            || suffix.contains('\\')
        {
            return Err(GitRefError::InvalidSyntax);
        }
        return Ok(());
    }

    if reference.len() == 40 || reference.len() == 64 {
        if reference.chars().all(|c| c.is_ascii_hexdigit()) {
            return Ok(());
        }
        return Err(GitRefError::InvalidCommitId);
    }

    Err(GitRefError::InvalidSyntax)
}

pub fn prepare_workspace(data_dir: &Path, build_id: &str) -> Result<PathBuf, std::io::Error> {
    let workspace_dir = data_dir.join("workspaces").join(build_id);
    fs::create_dir_all(&workspace_dir)?;
    Ok(workspace_dir)
}

pub fn resolve_commit(repo_path: &Path, reference: &str) -> Result<String, GitRefError> {
    resolve_commit_with_cancellation(repo_path, reference, None)
}

fn resolve_commit_with_cancellation(
    repo_path: &Path,
    reference: &str,
    cancellation_requested: Option<&AtomicBool>,
) -> Result<String, GitRefError> {
    validate_ref(reference)?;

    let mut command = Command::new("git");
    command
        .current_dir(repo_path)
        .arg("rev-parse")
        .arg("--verify")
        .arg("--quiet")
        .arg(format!("{reference}^{{commit}}"));
    configure_git_environment(&mut command);
    let output =
        run_git_command_with_cancellation(command, GIT_COMMAND_TIMEOUT, cancellation_requested)?;

    if !output.status.success() {
        return Err(GitRefError::RefNotFound);
    }

    let commit = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if commit.is_empty() || commit.len() < 7 {
        return Err(GitRefError::NotACommit);
    }

    Ok(commit)
}

pub fn checkout_repository(
    repo_url: &str,
    workspace: &Path,
    reference: &str,
) -> Result<CheckoutResult, GitRefError> {
    checkout_repository_inner(repo_url, workspace, reference, None)
}

pub(crate) fn checkout_repository_with_cancellation(
    repo_url: &str,
    workspace: &Path,
    reference: &str,
    cancellation_requested: &AtomicBool,
) -> Result<CheckoutResult, GitRefError> {
    checkout_repository_inner(repo_url, workspace, reference, Some(cancellation_requested))
}

fn checkout_repository_inner(
    repo_url: &str,
    workspace: &Path,
    reference: &str,
    cancellation_requested: Option<&AtomicBool>,
) -> Result<CheckoutResult, GitRefError> {
    validate_ref(reference)?;

    if workspace.exists() {
        fs::remove_dir_all(workspace).map_err(|_| GitRefError::RefNotFound)?;
    }

    let mut clone = Command::new("git");
    clone
        .arg("clone")
        .arg("--quiet")
        .arg("--no-recurse-submodules")
        .arg("--")
        .arg(repo_url)
        .arg(workspace);
    configure_git_environment(&mut clone);
    let output = run_git_command_with_cancellation(clone, CLONE_TIMEOUT, cancellation_requested)?;

    if !output.status.success() {
        return Err(command_failure("git clone", &output));
    }

    let resolved_commit =
        resolve_commit_with_cancellation(workspace, reference, cancellation_requested)?;

    let mut checkout = Command::new("git");
    checkout
        .current_dir(workspace)
        .args(["checkout", "--detach", "--quiet"])
        .arg(&resolved_commit);
    configure_git_environment(&mut checkout);
    let output =
        run_git_command_with_cancellation(checkout, GIT_COMMAND_TIMEOUT, cancellation_requested)?;

    if !output.status.success() {
        return Err(command_failure("git checkout", &output));
    }

    Ok(CheckoutResult {
        workspace: workspace.to_path_buf(),
        resolved_commit,
    })
}

fn configure_git_environment(command: &mut Command) {
    command
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .env_remove("GIT_CONFIG_PARAMETERS")
        .process_group(0);
}

fn run_git_command_with_cancellation(
    mut command: Command,
    timeout: Duration,
    cancellation_requested: Option<&AtomicBool>,
) -> Result<Output, GitRefError> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|err| GitRefError::CommandFailed(format!("failed to start git: {err}")))?;
    let process_group = child.id();
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| GitRefError::CommandFailed("git stdout was not captured".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| GitRefError::CommandFailed("git stderr was not captured".to_string()))?;
    let stdout_reader = thread::spawn(move || read_capped(stdout));
    let stderr_reader = thread::spawn(move || read_capped(stderr));
    let deadline = Instant::now() + timeout;

    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if cancellation_requested.is_some_and(|flag| flag.load(Ordering::Acquire)) => {
                let termination = terminate_process_group(&mut child, process_group);
                let _ = (join_reader(stdout_reader)?, join_reader(stderr_reader)?);
                termination?;
                return Err(GitRefError::Cancelled);
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Ok(None) => {
                let termination = terminate_process_group(&mut child, process_group);
                let _ = (join_reader(stdout_reader)?, join_reader(stderr_reader)?);
                termination?;
                return Err(GitRefError::TimedOut);
            }
            Err(err) => {
                let termination = terminate_process_group(&mut child, process_group);
                let _ = (join_reader(stdout_reader)?, join_reader(stderr_reader)?);
                termination?;
                return Err(GitRefError::CommandFailed(format!(
                    "failed while waiting for git: {err}"
                )));
            }
        }
    };

    terminate_process_group(&mut child, process_group)?;
    let stdout = join_reader(stdout_reader)?;
    let stderr = join_reader(stderr_reader)?;

    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

fn read_capped(mut reader: impl Read) -> io::Result<Vec<u8>> {
    let mut captured = Vec::new();
    let mut buffer = [0; 8192];
    loop {
        let bytes = reader.read(&mut buffer)?;
        if bytes == 0 {
            return Ok(captured);
        }
        let remaining = MAX_CAPTURED_OUTPUT.saturating_sub(captured.len());
        captured.extend_from_slice(&buffer[..bytes.min(remaining)]);
    }
}

fn join_reader(reader: thread::JoinHandle<io::Result<Vec<u8>>>) -> Result<Vec<u8>, GitRefError> {
    reader
        .join()
        .map_err(|_| GitRefError::CommandFailed("git output reader panicked".to_string()))?
        .map_err(|err| GitRefError::CommandFailed(format!("failed to read git output: {err}")))
}

fn terminate_process_group(child: &mut Child, process_group: u32) -> Result<(), GitRefError> {
    if signal_process_group(process_group, libc::SIGTERM)? {
        thread::sleep(PROCESS_TERMINATION_GRACE);
        signal_process_group(process_group, libc::SIGKILL)?;
    }
    if child
        .try_wait()
        .map_err(|err| GitRefError::CommandFailed(format!("failed to wait for git: {err}")))?
        .is_none()
    {
        child
            .kill()
            .map_err(|err| GitRefError::CommandFailed(format!("failed to stop git: {err}")))?;
    }
    child
        .wait()
        .map_err(|err| GitRefError::CommandFailed(format!("failed to reap git: {err}")))?;
    Ok(())
}

fn signal_process_group(process_group: u32, signal: i32) -> Result<bool, GitRefError> {
    let process_group = libc::pid_t::try_from(process_group)
        .map_err(|err| GitRefError::CommandFailed(err.to_string()))?;
    let result = unsafe { libc::kill(-process_group, signal) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(false)
    } else {
        Err(GitRefError::CommandFailed(format!(
            "failed to signal git process group {process_group}: {error}"
        )))
    }
}

fn command_failure(operation: &str, output: &Output) -> GitRefError {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        GitRefError::CommandFailed(format!("{operation} exited with {}", output.status))
    } else {
        GitRefError::CommandFailed(format!("{operation} failed: {stderr}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn accepts_fully_qualified_ref_names() {
        assert!(validate_ref("refs/heads/main").is_ok());
        assert!(validate_ref("refs/tags/v1.0.0").is_ok());
    }

    #[test]
    fn rejects_short_or_ambiguous_refs() {
        assert!(matches!(
            validate_ref("main"),
            Err(GitRefError::InvalidSyntax)
        ));
        assert!(matches!(
            validate_ref("abc123"),
            Err(GitRefError::InvalidSyntax)
        ));
    }

    #[test]
    fn rejects_abbreviated_commit_hashes() {
        assert!(matches!(
            validate_ref("0123456789abcdef0123456789abcdef01234567"),
            Ok(())
        ));
        assert!(matches!(
            validate_ref("0123456"),
            Err(GitRefError::InvalidSyntax)
        ));
    }

    #[test]
    fn git_commands_time_out_and_reap_their_process_group() {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("sleep 10");
        configure_git_environment(&mut command);

        let started = Instant::now();
        assert!(matches!(
            run_git_command_with_cancellation(command, Duration::from_millis(100), None),
            Err(GitRefError::TimedOut)
        ));
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn git_commands_cancel_and_reap_their_process_group() {
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let cancellation_signal = Arc::clone(&cancellation_requested);
        let signal_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            cancellation_signal.store(true, Ordering::Release);
        });
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("sleep 10");
        configure_git_environment(&mut command);

        let started = Instant::now();
        assert!(matches!(
            run_git_command_with_cancellation(
                command,
                Duration::from_secs(30),
                Some(&cancellation_requested)
            ),
            Err(GitRefError::Cancelled)
        ));
        signal_thread.join().unwrap();
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn git_command_failures_include_captured_stderr() {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("printf 'failure detail' >&2; exit 7");
        configure_git_environment(&mut command);

        let output =
            run_git_command_with_cancellation(command, Duration::from_secs(1), None).unwrap();
        assert_eq!(
            command_failure("git test", &output),
            GitRefError::CommandFailed("git test failed: failure detail".to_string())
        );
    }

    #[test]
    fn checks_out_a_repo_at_the_expected_commit() {
        let source = tempdir().unwrap();
        let repo_path = source.path().join("repo");

        let init = Command::new("git")
            .args(["init", "--initial-branch=main"])
            .current_dir(&repo_path)
            .status();
        if init.is_err() {
            let _ = fs::create_dir_all(&repo_path);
            Command::new("git")
                .args(["init", "--initial-branch=main"])
                .current_dir(&repo_path)
                .status()
                .unwrap();
        }

        Command::new("git")
            .args(["config", "user.name", "Test User"])
            .current_dir(&repo_path)
            .status()
            .unwrap();
        Command::new("git")
            .args(["config", "user.email", "test@example.com"])
            .current_dir(&repo_path)
            .status()
            .unwrap();

        fs::write(repo_path.join("README.md"), "hello\n").unwrap();
        Command::new("git")
            .args(["add", "README.md"])
            .current_dir(&repo_path)
            .status()
            .unwrap();
        Command::new("git")
            .args(["commit", "-m", "initial"])
            .current_dir(&repo_path)
            .status()
            .unwrap();

        Command::new("git")
            .args(["tag", "v1.0.0"])
            .current_dir(&repo_path)
            .status()
            .unwrap();

        fs::write(repo_path.join("README.md"), "updated\n").unwrap();
        Command::new("git")
            .args(["commit", "-am", "second"])
            .current_dir(&repo_path)
            .status()
            .unwrap();

        let commit = Command::new("git")
            .args(["-C", repo_path.to_str().unwrap(), "rev-parse", "HEAD"])
            .output()
            .unwrap();
        let head = String::from_utf8_lossy(&commit.stdout).trim().to_string();

        let base = tempdir().unwrap();
        let workspace = prepare_workspace(base.path(), "build-1").unwrap();
        let result =
            checkout_repository(repo_path.to_str().unwrap(), &workspace, "refs/tags/v1.0.0")
                .unwrap();

        assert_eq!(
            result.resolved_commit,
            resolve_commit(&workspace, "refs/tags/v1.0.0").unwrap()
        );
        assert_eq!(result.resolved_commit.len(), 40);
        assert!(workspace.join("README.md").exists());

        let checkout_at_head =
            checkout_repository(repo_path.to_str().unwrap(), &workspace, &head).unwrap();
        assert_eq!(checkout_at_head.resolved_commit, head);
    }
}
