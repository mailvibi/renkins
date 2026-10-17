use crate::domain::{Build, Job};
use rusqlite::{Connection, params};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SCHEMA_VERSION: i32 = 1;
const INITIAL_SCHEMA: &str = include_str!("../migrations/0001_initial.sql");

#[derive(Debug)]
pub struct DatabaseStore {
    pub path: PathBuf,
    conn: Connection,
}

impl DatabaseStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create data directory: {err}"))?;
        }

        let conn = Connection::open(path)
            .map_err(|err| format!("failed to open sqlite database {path:?}: {err}"))?;
        conn.execute_batch(
            "PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;",
        )
        .map_err(|err| format!("failed to configure sqlite database: {err}"))?;
        conn.busy_timeout(Duration::from_secs(5))
            .map_err(|err| format!("failed to configure sqlite busy timeout: {err}"))?;

        Ok(Self {
            path: path.to_path_buf(),
            conn,
        })
    }

    pub fn initialize_schema(&mut self) -> Result<(), String> {
        let current_version: i32 = self
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(|err| format!("failed to read sqlite schema version: {err}"))?;

        if current_version > SCHEMA_VERSION {
            return Err(format!(
                "database schema version {current_version} is newer than supported version {SCHEMA_VERSION}"
            ));
        }

        if current_version == SCHEMA_VERSION {
            return Ok(());
        }

        let transaction = self
            .conn
            .transaction()
            .map_err(|err| format!("failed to begin schema migration: {err}"))?;
        transaction
            .execute_batch(INITIAL_SCHEMA)
            .map_err(|err| format!("failed to apply sqlite schema migration 1: {err}"))?;
        transaction
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(|err| format!("failed to record sqlite schema version: {err}"))?;
        transaction
            .commit()
            .map_err(|err| format!("failed to commit sqlite schema migration: {err}"))
    }

    pub fn create_job(&self, job: &Job) -> Result<(), String> {
        self.conn.execute(
            "INSERT INTO jobs (id, name, repository_url, default_ref, disabled, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                job.id,
                job.name,
                job.repository_url,
                job.default_ref,
                if job.disabled { 1 } else { 0 },
                0_i64,
                0_i64,
            ],
        )
        .map_err(|err| format!("failed to insert job: {err}"))?;

        Ok(())
    }

    pub fn create_build(&self, build: &Build) -> Result<(), String> {
        self.conn.execute(
            "INSERT INTO builds (id, job_id, number, requested_ref, status, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                build.id,
                build.job_id,
                build.number as i64,
                build.requested_ref,
                "queued",
                0_i64,
            ],
        )
        .map_err(|err| format!("failed to insert build: {err}"))?;

        Ok(())
    }

    pub fn count_jobs(&self) -> Result<usize, String> {
        self.conn
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| {
                row.get::<_, usize>(0)
            })
            .map_err(|err| format!("failed to count jobs: {err}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Build, BuildStatus, Job};
    use tempfile::tempdir;

    #[test]
    fn creates_a_sqlite_database_and_schema() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let mut store = DatabaseStore::open(&db_path).expect("database should open");
        store.initialize_schema().expect("schema should initialize");
        assert!(db_path.exists());
    }

    #[test]
    fn persists_job_and_build_records() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let mut store = DatabaseStore::open(&db_path).unwrap();
        store.initialize_schema().unwrap();

        let job = Job {
            id: "job-1".to_string(),
            name: "demo".to_string(),
            repository_url: "https://example.com/repo.git".to_string(),
            default_ref: Some("refs/heads/main".to_string()),
            disabled: false,
        };
        store.create_job(&job).unwrap();

        let build = Build {
            id: "build-1".to_string(),
            job_id: "job-1".to_string(),
            number: 1,
            requested_ref: "refs/heads/main".to_string(),
            status: BuildStatus::Queued,
        };
        store.create_build(&build).unwrap();

        assert_eq!(store.count_jobs().unwrap(), 1);
    }

    #[test]
    fn migration_is_versioned_and_idempotent() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let mut store = DatabaseStore::open(&db_path).unwrap();

        store.initialize_schema().unwrap();
        store.initialize_schema().unwrap();

        let version: i32 = store
            .conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
    }

    #[test]
    fn rejects_database_schema_from_a_newer_version() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let mut store = DatabaseStore::open(&db_path).unwrap();
        store
            .conn
            .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();

        let error = store.initialize_schema().unwrap_err();
        assert!(error.contains("newer than supported version"));
    }

    #[test]
    fn configures_sqlite_connection_pragmas_and_busy_timeout() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let store = DatabaseStore::open(&db_path).unwrap();

        let foreign_keys: i32 = store
            .conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .unwrap();
        let journal_mode: String = store
            .conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        let busy_timeout: i32 = store
            .conn
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .unwrap();
        assert_eq!(foreign_keys, 1);
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
        assert_eq!(busy_timeout, 5_000);
    }
}
