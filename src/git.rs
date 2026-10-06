use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitRefError {
    Empty,
    InvalidSyntax,
    InvalidCommitId,
    RefNotFound,
    NotACommit,
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
    validate_ref(reference)?;

    let output = Command::new("git")
        .args(["-C", repo_path.to_str().unwrap()])
        .arg("rev-parse")
        .arg("--verify")
        .arg("--quiet")
        .arg(format!("{reference}^{{commit}}"))
        .output()
        .map_err(|_| GitRefError::RefNotFound)?;

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
    validate_ref(reference)?;

    if workspace.exists() {
        fs::remove_dir_all(workspace).map_err(|_| GitRefError::RefNotFound)?;
    }

    let status = Command::new("git")
        .arg("clone")
        .arg("--quiet")
        .arg(repo_url)
        .arg(workspace)
        .status()
        .map_err(|_| GitRefError::RefNotFound)?;

    if !status.success() {
        return Err(GitRefError::RefNotFound);
    }

    let resolved_commit = resolve_commit(workspace, reference)?;

    let checkout_status = Command::new("git")
        .args([
            "-C",
            workspace.to_str().unwrap(),
            "checkout",
            "--detach",
            &resolved_commit,
        ])
        .status()
        .map_err(|_| GitRefError::RefNotFound)?;

    if !checkout_status.success() {
        return Err(GitRefError::NotACommit);
    }

    Ok(CheckoutResult {
        workspace: workspace.to_path_buf(),
        resolved_commit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
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
