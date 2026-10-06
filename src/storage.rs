use crate::domain::{Build, Job};
use rusqlite::{Connection, params};
use std::fs;
use std::path::{Path, PathBuf};

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

        Ok(Self {
            path: path.to_path_buf(),
            conn,
        })
    }

    pub fn initialize_schema(&self) -> Result<(), String> {
        self.conn
            .execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE,
                    repository_url TEXT NOT NULL,
                    default_ref TEXT,
                    disabled INTEGER NOT NULL DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS builds (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    number INTEGER NOT NULL,
                    requested_ref TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES jobs(id),
                    UNIQUE(job_id, number)
                );

                CREATE TABLE IF NOT EXISTS build_steps (
                    build_id TEXT NOT NULL,
                    step_index INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    command TEXT NOT NULL,
                    status TEXT NOT NULL,
                    PRIMARY KEY(build_id, step_index),
                    FOREIGN KEY(build_id) REFERENCES builds(id)
                );
                "#,
            )
            .map_err(|err| format!("failed to initialize schema: {err}"))?;

        Ok(())
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
        let store = DatabaseStore::open(&db_path).expect("database should open");
        store.initialize_schema().expect("schema should initialize");
        assert!(db_path.exists());
    }

    #[test]
    fn persists_job_and_build_records() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let store = DatabaseStore::open(&db_path).unwrap();
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
}
