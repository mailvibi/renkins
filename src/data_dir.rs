use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct DataDirectoryLock {
    path: PathBuf,
    _file: File,
}

impl DataDirectoryLock {
    pub fn acquire(path: impl AsRef<Path>) -> Result<Self, String> {
        require_non_root(unsafe { libc::geteuid() })?;

        let configured_path = path.as_ref();
        let newly_created = !configured_path.exists();
        if newly_created {
            fs::create_dir_all(configured_path).map_err(|err| {
                format!(
                    "failed to create data directory {}: {err}",
                    configured_path.display()
                )
            })?;
            fs::set_permissions(configured_path, fs::Permissions::from_mode(0o700)).map_err(
                |err| {
                    format!(
                        "failed to restrict data directory permissions {}: {err}",
                        configured_path.display()
                    )
                },
            )?;
        }
        let path = configured_path.canonicalize().map_err(|err| {
            format!(
                "failed to resolve data directory {}: {err}",
                configured_path.display()
            )
        })?;
        if path == Path::new("/") {
            return Err("data_dir must not be the filesystem root".to_string());
        }

        let metadata = fs::metadata(&path)
            .map_err(|err| format!("failed to inspect data directory {}: {err}", path.display()))?;
        if !metadata.is_dir() {
            return Err(format!("data_dir {} is not a directory", path.display()));
        }
        require_owner(metadata.uid(), unsafe { libc::geteuid() }, &path)?;
        if metadata.permissions().mode() & 0o077 != 0 {
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).map_err(|err| {
                format!(
                    "failed to restrict data directory permissions {}: {err}",
                    path.display()
                )
            })?;
        }

        let lock_path = path.join(".renkins.lock");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&lock_path)
            .map_err(|err| format!("failed to open data-directory lock: {err}"))?;
        let lock_metadata = file
            .metadata()
            .map_err(|err| format!("failed to inspect data-directory lock: {err}"))?;
        if !lock_metadata.is_file() {
            return Err("data-directory lock is not a regular file".to_string());
        }
        require_owner(lock_metadata.uid(), unsafe { libc::geteuid() }, &lock_path)?;
        if lock_metadata.permissions().mode() & 0o077 != 0 {
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|err| {
                    format!("failed to restrict data-directory lock permissions: {err}")
                })?;
        }

        let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if result != 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::WouldBlock {
                return Err("data directory is already in use by another Renkins process".into());
            }
            return Err(format!("failed to acquire data-directory lock: {err}"));
        }

        Ok(Self { path, _file: file })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub fn resolve_file_under_data_dir(
    data_dir: &Path,
    configured_path: impl AsRef<Path>,
) -> Result<PathBuf, String> {
    let configured_path = configured_path.as_ref();
    let candidate = if configured_path.is_absolute() {
        configured_path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|err| format!("failed to resolve current directory: {err}"))?
            .join(configured_path)
    };
    let parent = candidate
        .parent()
        .ok_or_else(|| "configured file path must have a parent directory".to_string())?;
    let parent = parent
        .canonicalize()
        .map_err(|err| format!("failed to resolve configured file directory: {err}"))?;
    if !parent.starts_with(data_dir) {
        return Err("configured file path must remain inside data_dir".to_string());
    }
    let file_name = candidate
        .file_name()
        .ok_or_else(|| "configured file path must name a file".to_string())?;
    let resolved = parent.join(file_name);
    match fs::symlink_metadata(&resolved) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err("configured file path must not be a symlink".to_string())
        }
        Ok(metadata) if !metadata.is_file() => {
            Err("configured file path must name a regular file".to_string())
        }
        Ok(_) => {
            let resolved = resolved.canonicalize().map_err(|err| {
                format!(
                    "failed to resolve configured file {}: {err}",
                    resolved.display()
                )
            })?;
            if resolved.starts_with(data_dir) {
                Ok(resolved)
            } else {
                Err("configured file path must remain inside data_dir".to_string())
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(resolved),
        Err(err) => Err(format!(
            "failed to inspect configured file {}: {err}",
            resolved.display()
        )),
    }
}

fn require_non_root(effective_uid: libc::uid_t) -> Result<(), String> {
    if effective_uid == 0 {
        Err("Renkins must not run as root".to_string())
    } else {
        Ok(())
    }
}

fn require_owner(
    owner: libc::uid_t,
    effective_uid: libc::uid_t,
    path: &Path,
) -> Result<(), String> {
    if owner == effective_uid {
        Ok(())
    } else {
        Err(format!(
            "path {} is not owned by the current service account",
            path.display()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn refuses_root_execution() {
        assert!(require_non_root(0).is_err());
        assert!(require_non_root(1000).is_ok());
    }

    #[test]
    fn data_directory_lock_excludes_a_second_owner() {
        let temp = tempdir().unwrap();
        let data_dir = temp.path().join("renkins");
        let lock = DataDirectoryLock::acquire(&data_dir).unwrap();
        assert_eq!(
            fs::metadata(&data_dir).unwrap().permissions().mode() & 0o077,
            0
        );
        assert_eq!(
            fs::metadata(data_dir.join(".renkins.lock"))
                .unwrap()
                .permissions()
                .mode()
                & 0o077,
            0
        );
        assert!(
            DataDirectoryLock::acquire(&data_dir)
                .unwrap_err()
                .contains("already in use")
        );
        drop(lock);
        assert!(DataDirectoryLock::acquire(&data_dir).is_ok());
        assert!(DataDirectoryLock::acquire("/").is_err());
    }

    #[test]
    fn resolves_configured_files_only_under_data_directory() {
        let temp = tempdir().unwrap();
        let data_dir = temp.path().join("data");
        fs::create_dir(&data_dir).unwrap();
        let token_path = data_dir.join("token");
        assert_eq!(
            resolve_file_under_data_dir(&data_dir, &token_path).unwrap(),
            token_path
        );
        assert!(resolve_file_under_data_dir(&data_dir, temp.path().join("outside")).is_err());
        let outside = temp.path().join("outside-token");
        fs::write(&outside, "secret").unwrap();
        std::os::unix::fs::symlink(&outside, &token_path).unwrap();
        assert!(resolve_file_under_data_dir(&data_dir, &token_path).is_err());
    }
}
