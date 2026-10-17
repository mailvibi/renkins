use crate::domain::{Build, Job};
use rusqlite::{Connection, OptionalExtension, params};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

const SCHEMA_VERSION: i32 = 2;
const MIGRATIONS: &[(i32, &str)] = &[
    (1, include_str!("../migrations/0001_initial.sql")),
    (2, include_str!("../migrations/0002_job_build_numbers.sql")),
];
const MAX_PAGE_SIZE: u32 = 100;

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

        let mut version = current_version;
        for (migration_version, migration_sql) in MIGRATIONS {
            if *migration_version <= version {
                continue;
            }

            let transaction = self.conn.transaction().map_err(|err| {
                format!("failed to begin schema migration {migration_version}: {err}")
            })?;
            transaction.execute_batch(migration_sql).map_err(|err| {
                format!("failed to apply sqlite schema migration {migration_version}: {err}")
            })?;
            transaction
                .pragma_update(None, "user_version", migration_version)
                .map_err(|err| {
                    format!("failed to record sqlite schema version {migration_version}: {err}")
                })?;
            transaction.commit().map_err(|err| {
                format!("failed to commit sqlite schema migration {migration_version}: {err}")
            })?;
            version = *migration_version;
        }

        if version != SCHEMA_VERSION {
            return Err(format!(
                "no migration is available for schema version {}",
                version + 1
            ));
        }

        Ok(())
    }

    pub fn create_job(&self, job: &Job) -> Result<(), String> {
        let now = Self::now_unix_millis()?;
        self.conn.execute(
            "INSERT INTO jobs (id, name, repository_url, default_ref, disabled, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                job.id,
                job.name,
                job.repository_url,
                job.default_ref,
                if job.disabled { 1 } else { 0 },
                now,
                now,
            ],
        )
        .map_err(|err| format!("failed to insert job: {err}"))?;

        Ok(())
    }

    pub fn get_job(&self, id: &str) -> Result<Option<Job>, String> {
        self.conn
            .query_row(
                "SELECT id, name, repository_url, default_ref, disabled FROM jobs WHERE id = ?1",
                [id],
                Self::job_from_row,
            )
            .optional()
            .map_err(|err| format!("failed to get job: {err}"))
    }

    pub fn list_jobs(&self, limit: u32, offset: u32) -> Result<Vec<Job>, String> {
        Self::validate_page(limit)?;
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, name, repository_url, default_ref, disabled
                 FROM jobs ORDER BY name, id LIMIT ?1 OFFSET ?2",
            )
            .map_err(|err| format!("failed to prepare job list: {err}"))?;
        let rows = statement
            .query_map(params![limit, offset], Self::job_from_row)
            .map_err(|err| format!("failed to query jobs: {err}"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("failed to read jobs: {err}"))
    }

    pub fn set_job_disabled(&self, id: &str, disabled: bool) -> Result<bool, String> {
        let now = Self::now_unix_millis()?;
        self.conn
            .execute(
                "UPDATE jobs SET disabled = ?1, updated_at = ?2 WHERE id = ?3",
                params![disabled, now, id],
            )
            .map(|changed| changed == 1)
            .map_err(|err| format!("failed to update job: {err}"))
    }

    pub fn enqueue_build(
        &mut self,
        id: &str,
        job_id: &str,
        requested_ref: &str,
    ) -> Result<Build, String> {
        let now = Self::now_unix_millis()?;
        let transaction = self
            .conn
            .transaction()
            .map_err(|err| format!("failed to begin build enqueue transaction: {err}"))?;
        let number: i64 = transaction
            .query_row(
                "UPDATE jobs
                 SET next_build_number = next_build_number + 1, updated_at = ?1
                 WHERE id = ?2 AND disabled = 0
                 RETURNING next_build_number - 1",
                params![now, job_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|err| format!("failed to allocate build number: {err}"))?
            .ok_or_else(|| format!("job '{job_id}' does not exist or is disabled"))?;
        let number = u64::try_from(number)
            .map_err(|_| "allocated build number is outside the supported range".to_string())?;

        transaction
            .execute(
                "INSERT INTO builds (id, job_id, number, requested_ref, status, created_at)
                 VALUES (?1, ?2, ?3, ?4, 'queued', ?5)",
                params![id, job_id, number, requested_ref, now],
            )
            .map_err(|err| format!("failed to insert build: {err}"))?;
        transaction
            .commit()
            .map_err(|err| format!("failed to commit build enqueue transaction: {err}"))?;

        Ok(Build {
            id: id.to_string(),
            job_id: job_id.to_string(),
            number,
            requested_ref: requested_ref.to_string(),
            status: crate::domain::BuildStatus::Queued,
        })
    }

    pub fn get_build(&self, id: &str) -> Result<Option<Build>, String> {
        self.conn
            .query_row(
                "SELECT id, job_id, number, requested_ref, status FROM builds WHERE id = ?1",
                [id],
                Self::build_from_row,
            )
            .optional()
            .map_err(|err| format!("failed to get build: {err}"))
    }

    pub fn list_builds(&self, job_id: &str, limit: u32, offset: u32) -> Result<Vec<Build>, String> {
        Self::validate_page(limit)?;
        let mut statement = self
            .conn
            .prepare(
                "SELECT id, job_id, number, requested_ref, status FROM builds
                 WHERE job_id = ?1 ORDER BY number DESC LIMIT ?2 OFFSET ?3",
            )
            .map_err(|err| format!("failed to prepare build list: {err}"))?;
        let rows = statement
            .query_map(params![job_id, limit, offset], Self::build_from_row)
            .map_err(|err| format!("failed to query builds: {err}"))?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|err| format!("failed to read builds: {err}"))
    }

    pub fn transition_build(
        &self,
        id: &str,
        expected: crate::domain::BuildStatus,
        next: crate::domain::BuildStatus,
    ) -> Result<bool, String> {
        if !expected.can_transition_to(next) {
            return Err(format!(
                "invalid build status transition from {expected:?} to {next:?}"
            ));
        }
        let changed = self
            .conn
            .execute(
                "UPDATE builds SET status = ?1 WHERE id = ?2 AND status = ?3",
                params![Self::status_as_str(next), id, Self::status_as_str(expected)],
            )
            .map_err(|err| format!("failed to transition build: {err}"))?;
        Ok(changed == 1)
    }

    fn validate_page(limit: u32) -> Result<(), String> {
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(format!("page size must be between 1 and {MAX_PAGE_SIZE}"));
        }
        Ok(())
    }

    fn now_unix_millis() -> Result<i64, String> {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|err| format!("system clock is before Unix epoch: {err}"))?
            .as_millis()
            .try_into()
            .map_err(|err| format!("system timestamp is outside SQLite integer range: {err}"))
    }

    fn job_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Job> {
        Ok(Job {
            id: row.get(0)?,
            name: row.get(1)?,
            repository_url: row.get(2)?,
            default_ref: row.get(3)?,
            disabled: row.get(4)?,
        })
    }

    fn build_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Build> {
        let status: String = row.get(4)?;
        let status = Self::status_from_str(&status).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(4, "status".to_string(), rusqlite::types::Type::Text)
        })?;
        let number: i64 = row.get(2)?;
        let number = u64::try_from(number).map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                2,
                "number".to_string(),
                rusqlite::types::Type::Integer,
            )
        })?;
        Ok(Build {
            id: row.get(0)?,
            job_id: row.get(1)?,
            number,
            requested_ref: row.get(3)?,
            status,
        })
    }

    fn status_as_str(status: crate::domain::BuildStatus) -> &'static str {
        use crate::domain::BuildStatus::*;
        match status {
            Queued => "queued",
            Preparing => "preparing",
            Running => "running",
            Cancelling => "cancelling",
            Succeeded => "succeeded",
            Failed => "failed",
            Cancelled => "cancelled",
            Interrupted => "interrupted",
        }
    }

    fn status_from_str(status: &str) -> Option<crate::domain::BuildStatus> {
        use crate::domain::BuildStatus::*;
        match status {
            "queued" => Some(Queued),
            "preparing" => Some(Preparing),
            "running" => Some(Running),
            "cancelling" => Some(Cancelling),
            "succeeded" => Some(Succeeded),
            "failed" => Some(Failed),
            "cancelled" => Some(Cancelled),
            "interrupted" => Some(Interrupted),
            _ => None,
        }
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
    use crate::domain::{BuildStatus, Job};
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

        let build = store
            .enqueue_build("build-1", "job-1", "refs/heads/main")
            .unwrap();

        assert_eq!(store.count_jobs().unwrap(), 1);
        assert_eq!(build.number, 1);
        assert_eq!(store.get_build("build-1").unwrap(), Some(build));
    }

    #[test]
    fn lists_jobs_and_builds_with_bounded_pagination() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let mut store = DatabaseStore::open(&db_path).unwrap();
        store.initialize_schema().unwrap();
        store.create_job(&test_job("job-1", "first")).unwrap();
        store.create_job(&test_job("job-2", "second")).unwrap();
        store
            .enqueue_build("build-1", "job-1", "refs/heads/main")
            .unwrap();
        store
            .enqueue_build("build-2", "job-1", "refs/heads/main")
            .unwrap();

        let jobs = store.list_jobs(1, 1).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, "job-2");

        let builds = store.list_builds("job-1", 1, 0).unwrap();
        assert_eq!(builds.len(), 1);
        assert_eq!(builds[0].id, "build-2");
        assert!(store.list_jobs(0, 0).is_err());
        assert!(store.list_builds("job-1", MAX_PAGE_SIZE + 1, 0).is_err());
    }

    #[test]
    fn build_number_allocation_rolls_back_when_build_insert_fails() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let mut store = DatabaseStore::open(&db_path).unwrap();
        store.initialize_schema().unwrap();
        store.create_job(&test_job("job-1", "demo")).unwrap();
        store
            .enqueue_build("build-1", "job-1", "refs/heads/main")
            .unwrap();

        assert!(
            store
                .enqueue_build("build-1", "job-1", "refs/heads/main")
                .is_err()
        );
        let next = store
            .enqueue_build("build-2", "job-1", "refs/heads/main")
            .unwrap();
        assert_eq!(next.number, 2);
    }

    #[test]
    fn build_transitions_are_conditional_and_job_disabling_blocks_enqueue() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let mut store = DatabaseStore::open(&db_path).unwrap();
        store.initialize_schema().unwrap();
        store.create_job(&test_job("job-1", "demo")).unwrap();
        store
            .enqueue_build("build-1", "job-1", "refs/heads/main")
            .unwrap();

        assert!(
            store
                .transition_build("build-1", BuildStatus::Queued, BuildStatus::Preparing)
                .unwrap()
        );
        assert!(
            !store
                .transition_build("build-1", BuildStatus::Queued, BuildStatus::Cancelled)
                .unwrap()
        );
        assert!(
            store
                .transition_build("build-1", BuildStatus::Preparing, BuildStatus::Succeeded)
                .is_err()
        );
        assert!(store.set_job_disabled("job-1", true).unwrap());
        assert!(
            store
                .enqueue_build("build-2", "job-1", "refs/heads/main")
                .is_err()
        );
        assert!(store.get_job("job-1").unwrap().unwrap().disabled);
        assert!(!store.set_job_disabled("unknown", true).unwrap());
    }

    #[test]
    fn jobs_and_builds_survive_database_reopen() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        {
            let mut store = DatabaseStore::open(&db_path).unwrap();
            store.initialize_schema().unwrap();
            store.create_job(&test_job("job-1", "demo")).unwrap();
            store
                .enqueue_build("build-1", "job-1", "refs/heads/main")
                .unwrap();
        }

        let mut reopened = DatabaseStore::open(&db_path).unwrap();
        reopened.initialize_schema().unwrap();
        assert_eq!(
            reopened.get_job("job-1").unwrap(),
            Some(test_job("job-1", "demo"))
        );
        assert_eq!(reopened.get_build("build-1").unwrap().unwrap().number, 1);
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
    fn upgrades_database_from_previous_schema_version() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let connection = Connection::open(&db_path).unwrap();
        connection
            .execute_batch(include_str!("../migrations/0001_initial.sql"))
            .unwrap();
        connection.pragma_update(None, "user_version", 1).unwrap();
        drop(connection);

        let mut store = DatabaseStore::open(&db_path).unwrap();
        store.initialize_schema().unwrap();
        store.create_job(&test_job("job-1", "demo")).unwrap();
        assert_eq!(
            store
                .enqueue_build("build-1", "job-1", "refs/heads/main")
                .unwrap()
                .number,
            1
        );
    }

    #[test]
    fn concurrent_build_enqueues_allocate_distinct_numbers() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let mut store = DatabaseStore::open(&db_path).unwrap();
        store.initialize_schema().unwrap();
        store.create_job(&test_job("job-1", "demo")).unwrap();
        drop(store);

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let workers = (1..=2)
            .map(|index| {
                let db_path = db_path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let mut store = DatabaseStore::open(db_path).unwrap();
                    barrier.wait();
                    store
                        .enqueue_build(&format!("build-{index}"), "job-1", "refs/heads/main")
                        .unwrap()
                        .number
                })
            })
            .collect::<Vec<_>>();
        let mut numbers = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>();
        numbers.sort_unstable();
        assert_eq!(numbers, vec![1, 2]);
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

    fn test_job(id: &str, name: &str) -> Job {
        Job {
            id: id.to_string(),
            name: name.to_string(),
            repository_url: "https://example.com/repo.git".to_string(),
            default_ref: Some("refs/heads/main".to_string()),
            disabled: false,
        }
    }
}
