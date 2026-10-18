use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::json;

use crate::api::{
    BuildEnqueueRequest, JobCreateRequest, JobResponse, create_job, validate_build_ref,
};
use crate::config::{ServerConfig, read_or_create_token_file, validate_token_header};
use crate::data_dir::{DataDirectoryLock, resolve_file_under_data_dir};
use crate::domain::{Build, BuildCancellationReason, BuildStatus, Job};
use crate::logs::{ReadLogError, read_page};
use crate::storage::{DatabaseStore, EnqueueBuildError};
use crate::worker::WorkerControl;

#[derive(Clone, Debug)]
pub struct AppState {
    token: String,
    store: Arc<Mutex<DatabaseStore>>,
    logs_dir: PathBuf,
    worker_control: WorkerControl,
    data_directory_locked: bool,
}

pub fn build_router(expected_token: &str) -> Result<Router, String> {
    let mut store = DatabaseStore::open_in_memory()?;
    store.initialize_schema()?;
    Ok(build_router_with_store(expected_token, store))
}

pub fn build_router_with_store(expected_token: &str, store: DatabaseStore) -> Router {
    let logs_dir = store
        .path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join("logs");
    build_router_with_shared_store(
        expected_token,
        Arc::new(Mutex::new(store)),
        logs_dir,
        WorkerControl::default(),
        false,
    )
}

fn build_router_with_shared_store(
    expected_token: &str,
    store: Arc<Mutex<DatabaseStore>>,
    logs_dir: PathBuf,
    worker_control: WorkerControl,
    data_directory_locked: bool,
) -> Router {
    Router::new()
        .route("/healthz", get(healthcheck))
        .route("/readyz", get(readycheck))
        .route("/api/v1/jobs", get(list_jobs))
        .route("/api/v1/jobs", post(create_job_route))
        .route("/api/v1/jobs/{job_id}", get(get_job_route))
        .route("/api/v1/jobs/{job_id}/disable", post(disable_job_route))
        .route("/api/v1/jobs/{job_id}/enable", post(enable_job_route))
        .route(
            "/api/v1/jobs/{job_id}/builds",
            get(list_builds_route).post(enqueue_build_route),
        )
        .route("/api/v1/builds/{build_id}", get(get_build_route))
        .route("/api/v1/builds/{build_id}/cancel", post(cancel_build_route))
        .route("/api/v1/builds/{build_id}/logs", get(get_build_logs_route))
        .with_state(AppState {
            token: expected_token.to_string(),
            store,
            logs_dir,
            worker_control,
            data_directory_locked,
        })
}

pub async fn serve(config: &ServerConfig) -> Result<(), String> {
    let data_directory_lock = DataDirectoryLock::acquire(&config.data_dir)?;
    let data_dir = data_directory_lock.path().to_path_buf();
    let token_file = resolve_file_under_data_dir(&data_dir, &config.auth_token_file)?;
    let token = read_or_create_token_file(&token_file)
        .map_err(|err| format!("failed to initialize auth token: {err}"))?;

    let database_path = data_dir.join("renkins.sqlite3");
    let mut store = DatabaseStore::open(&database_path)?;
    store.initialize_schema()?;
    store.recover_abandoned_builds()?;
    recover_build_logs(&data_dir.join("logs"), &store)?;
    let store = Arc::new(Mutex::new(store));
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .map_err(|err| format!("failed to bind {}: {err}", config.listen))?;
    let worker_control = WorkerControl::default();
    let worker = crate::worker::start_worker_pool(
        Arc::clone(&store),
        data_dir.clone(),
        worker_control.clone(),
        config.max_concurrent_builds,
    )?;
    let router = build_router_with_shared_store(
        &token,
        store,
        data_dir.join("logs"),
        worker_control.clone(),
        true,
    );
    let signal_control = worker_control;
    let server_result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            wait_for_shutdown_signal().await;
            if let Err(err) = signal_control.request_shutdown() {
                eprintln!("failed to stop build claims during shutdown: {err}");
            }
        })
        .await
        .map_err(|err| format!("server exited with error: {err}"));

    let worker_result = worker.shutdown(Duration::from_secs(config.shutdown_grace_seconds));
    match (server_result, worker_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(server_err), Ok(())) => Err(server_err),
        (Ok(()), Err(worker_err)) => Err(format!("worker shutdown failed: {worker_err}")),
        (Err(server_err), Err(worker_err)) => Err(format!(
            "{server_err}; worker shutdown failed: {worker_err}"
        )),
    }
}

fn recover_build_logs(logs_dir: &Path, store: &DatabaseStore) -> Result<(), String> {
    fs::create_dir_all(logs_dir)
        .map_err(|err| format!("failed to create build log directory: {err}"))?;
    let entries = fs::read_dir(logs_dir)
        .map_err(|err| format!("failed to scan build log directory: {err}"))?;
    for entry in entries {
        let entry = entry.map_err(|err| format!("failed to read build log directory: {err}"))?;
        let file_type = entry
            .file_type()
            .map_err(|err| format!("failed to inspect build log directory entry: {err}"))?;
        if !file_type.is_file() || entry.path().extension().is_none_or(|ext| ext != "rkl") {
            continue;
        }
        let path = entry.path();
        let build_id = path
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or_else(|| format!("build log has an invalid filename: {}", path.display()))?
            .to_string();
        validate_log_build_id(&build_id)?;
        let integrity = crate::logs::recover_log_tail(&path)
            .map_err(|err| format!("failed to recover build log {}: {err:?}", path.display()))?;
        store.set_log_integrity(&build_id, integrity)?;
    }

    for build_id in store.interrupted_builds_needing_log_marker()? {
        let path = log_path_for_build(logs_dir, &build_id)?;
        ensure_safe_log_path(&path)?;
        crate::logs::append_restart_marker(&path)?;
        store.mark_recovery_marker_written(&build_id)?;
    }
    Ok(())
}

fn log_path_for_build(logs_dir: &Path, build_id: &str) -> Result<PathBuf, String> {
    validate_log_build_id(build_id)?;
    Ok(logs_dir.join(format!("{build_id}.rkl")))
}

fn validate_log_build_id(build_id: &str) -> Result<(), String> {
    if build_id.is_empty()
        || !build_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!(
            "build ID cannot be used as a log filename: {build_id:?}"
        ));
    }
    Ok(())
}

fn ensure_safe_log_path(path: &Path) -> Result<(), String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err(format!(
            "build log path is not a regular file: {}",
            path.display()
        )),
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!(
            "failed to inspect build log path {}: {err}",
            path.display()
        )),
    }
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    result = ctrl_c => {
                        if let Err(err) = result {
                            eprintln!("failed to listen for Ctrl-C: {err}");
                        }
                    }
                    _ = terminate.recv() => {}
                }
            }
            Err(err) => {
                eprintln!("failed to listen for SIGTERM: {err}");
                if let Err(err) = ctrl_c.await {
                    eprintln!("failed to listen for Ctrl-C: {err}");
                }
            }
        }
    }
    #[cfg(not(unix))]
    if let Err(err) = ctrl_c.await {
        eprintln!("failed to listen for Ctrl-C: {err}");
    }
}

pub fn default_server_config() -> ServerConfig {
    ServerConfig::new("127.0.0.1:8080", "./data", "./data/admin-token").unwrap()
}

pub fn load_server_config(config_path: Option<&Path>) -> Result<ServerConfig, String> {
    match config_path {
        Some(path) if path.exists() => crate::config::parse_server_config_file(path),
        Some(path) => Err(format!("config file not found: {}", path.display())),
        None => Ok(default_server_config()),
    }
}

async fn healthcheck() -> StatusCode {
    StatusCode::OK
}

async fn readycheck(State(state): State<AppState>) -> StatusCode {
    if !state.data_directory_locked || !state.worker_control.is_ready() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    match state.store.lock() {
        Ok(store) => match store.check_readiness() {
            Ok(()) => StatusCode::OK,
            Err(err) => {
                eprintln!("database is not ready: {err}");
                StatusCode::SERVICE_UNAVAILABLE
            }
        },
        Err(_) => {
            eprintln!("database store lock is poisoned during readiness check");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn list_jobs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(page): Query<PageParams>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    if !authorized(&state, &headers) {
        return Err(unauthorized_response());
    }

    let jobs = state
        .store
        .lock()
        .map_err(|_| internal_error_response())?
        .list_jobs(page.limit, page.offset)
        .map_err(|_| invalid_pagination_response())?
        .into_iter()
        .map(job_response)
        .collect::<Vec<_>>();

    Ok(Json(json!(jobs)).into_response())
}

#[derive(Debug, serde::Deserialize)]
struct PageParams {
    #[serde(default = "default_page_limit")]
    limit: u32,
    #[serde(default)]
    offset: u32,
}

fn default_page_limit() -> u32 {
    50
}

async fn get_job_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    if !authorized(&state, &headers) {
        return Err(unauthorized_response());
    }
    let job = state
        .store
        .lock()
        .map_err(|_| internal_error_response())?
        .get_job(&job_id)
        .map_err(|_| internal_error_response())?;
    match job {
        Some(job) => Ok(Json(job_response(job)).into_response()),
        None => Ok(not_found_response("job_not_found", "job does not exist")),
    }
}

async fn disable_job_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    set_job_disabled(state, headers, job_id, true).await
}

async fn enable_job_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(job_id): AxumPath<String>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    set_job_disabled(state, headers, job_id, false).await
}

async fn set_job_disabled(
    state: AppState,
    headers: HeaderMap,
    job_id: String,
    disabled: bool,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    if !authorized(&state, &headers) {
        return Err(unauthorized_response());
    }
    let store = state.store.lock().map_err(|_| internal_error_response())?;
    if !store
        .set_job_disabled(&job_id, disabled)
        .map_err(|_| internal_error_response())?
    {
        return Ok(not_found_response("job_not_found", "job does not exist"));
    }
    let job = store
        .get_job(&job_id)
        .map_err(|_| internal_error_response())?
        .ok_or_else(internal_error_response)?;
    Ok(Json(job_response(job)).into_response())
}

async fn list_builds_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(job_id): AxumPath<String>,
    Query(page): Query<PageParams>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    if !authorized(&state, &headers) {
        return Err(unauthorized_response());
    }
    let store = state.store.lock().map_err(|_| internal_error_response())?;
    if store
        .get_job(&job_id)
        .map_err(|_| internal_error_response())?
        .is_none()
    {
        return Ok(not_found_response("job_not_found", "job does not exist"));
    }
    let builds = store
        .list_builds(&job_id, page.limit, page.offset)
        .map_err(|_| invalid_pagination_response())?;
    Ok(Json(json!(builds.iter().map(build_response).collect::<Vec<_>>())).into_response())
}

async fn get_build_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(build_id): AxumPath<String>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    if !authorized(&state, &headers) {
        return Err(unauthorized_response());
    }
    let store = state.store.lock().map_err(|_| internal_error_response())?;
    let Some(build) = store
        .get_build(&build_id)
        .map_err(|_| internal_error_response())?
    else {
        return Ok(not_found_response(
            "build_not_found",
            "build does not exist",
        ));
    };
    let snapshot = store
        .get_pipeline_snapshot(&build_id)
        .map_err(|_| internal_error_response())?;
    let steps = store
        .get_build_steps(&build_id)
        .map_err(|_| internal_error_response())?;
    let steps = steps
        .iter()
        .map(|step| {
            json!({
                "step_index": step.step_index,
                "name": step.name,
                "command": step.command,
                "status": step.status,
                "exit_code": step.exit_code,
                "started_at_ms": step.started_at,
                "finished_at_ms": step.finished_at,
                "duration_ms": duration_ms(step.started_at, step.finished_at)
            })
        })
        .collect::<Vec<_>>();
    let resolved_commit = store
        .get_resolved_commit(&build_id)
        .map_err(|_| internal_error_response())?;
    Ok(Json(json!({
        "id": build.id,
        "job_id": build.job_id,
        "number": build.number,
        "requested_ref": build.requested_ref,
        "resolved_commit": resolved_commit,
        "status": build_status(build.status),
        "created_at_ms": build.created_at,
        "started_at_ms": build.started_at,
        "finished_at_ms": build.finished_at,
        "duration_ms": duration_ms(build.started_at, build.finished_at),
        "cancel_requested_at_ms": build.cancel_requested_at,
        "cancel_reason": build.cancel_reason,
        "pipeline": snapshot,
        "steps": steps
    }))
    .into_response())
}

async fn cancel_build_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(build_id): AxumPath<String>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    if !authorized(&state, &headers) {
        return Err(unauthorized_response());
    }
    let build = state
        .store
        .lock()
        .map_err(|_| internal_error_response())?
        .request_build_cancellation(&build_id, BuildCancellationReason::User)
        .map_err(|_| internal_error_response())?;
    let Some(build) = build else {
        return Ok(not_found_response(
            "build_not_found",
            "build does not exist",
        ));
    };
    if build.status == BuildStatus::Cancelling {
        state
            .worker_control
            .cancel_active(&build_id)
            .map_err(|_| internal_error_response())?;
    }
    Ok(Json(json!({
        "id": build.id,
        "job_id": build.job_id,
        "number": build.number,
        "status": build_status(build.status),
        "cancel_requested_at_ms": build.cancel_requested_at,
        "cancel_reason": build.cancel_reason
    }))
    .into_response())
}

#[derive(Debug, serde::Deserialize)]
struct LogParams {
    cursor: Option<String>,
    #[serde(default = "default_log_limit")]
    limit: usize,
}

fn default_log_limit() -> usize {
    100
}

async fn get_build_logs_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    AxumPath(build_id): AxumPath<String>,
    Query(params): Query<LogParams>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    if !authorized(&state, &headers) {
        return Err(unauthorized_response());
    }
    if params.limit == 0 || params.limit > 100 {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "code": "invalid_log_limit",
                "message": "limit must be between 1 and 100"
            })),
        )
            .into_response());
    }

    let build = state
        .store
        .lock()
        .map_err(|_| internal_error_response())?
        .get_build(&build_id)
        .map_err(|_| internal_error_response())?;
    let Some(build) = build else {
        return Ok(not_found_response(
            "build_not_found",
            "build does not exist",
        ));
    };
    let terminal = matches!(
        build.status,
        BuildStatus::Succeeded
            | BuildStatus::Failed
            | BuildStatus::Cancelled
            | BuildStatus::Interrupted
    );
    let log_path =
        log_path_for_build(&state.logs_dir, &build.id).map_err(|_| internal_error_response())?;
    let stored_integrity = state
        .store
        .lock()
        .map_err(|_| internal_error_response())?
        .get_log_integrity(&build.id)
        .map_err(|_| internal_error_response())?
        .unwrap_or_else(|| "valid".to_string());
    if terminal && stored_integrity == "expired" {
        return Ok(log_expired_response());
    }
    if terminal
        && !log_path
            .try_exists()
            .map_err(|_| internal_error_response())?
    {
        state
            .store
            .lock()
            .map_err(|_| internal_error_response())?
            .set_log_integrity(&build.id, "expired")
            .map_err(|_| internal_error_response())?;
        return Ok(log_expired_response());
    }
    let page = match read_page(&log_path, params.cursor.as_deref(), params.limit, terminal) {
        Ok(Some(page)) => page,
        Ok(None) if terminal => {
            state
                .store
                .lock()
                .map_err(|_| internal_error_response())?
                .set_log_integrity(&build.id, "expired")
                .map_err(|_| internal_error_response())?;
            return Ok(log_expired_response());
        }
        Ok(None) => {
            return Ok(Json(json!({
                "build_id": build.id,
                "records": [],
                "next_cursor": params.cursor.unwrap_or_else(|| "0000000000000000".to_string()),
                "has_more": false,
                "log_integrity": stored_integrity
            }))
            .into_response());
        }
        Err(ReadLogError::InvalidCursor) => {
            return Ok((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "code": "invalid_log_cursor",
                    "message": "cursor must identify a complete log record boundary"
                })),
            )
                .into_response());
        }
        Err(ReadLogError::Io(_)) => return Err(internal_error_response()),
    };

    let (log_integrity, warning) = {
        let store = state.store.lock().map_err(|_| internal_error_response())?;
        if page.integrity != "valid" {
            store
                .set_log_integrity(&build.id, page.integrity)
                .map_err(|_| internal_error_response())?;
        }
        let integrity = store
            .get_log_integrity(&build.id)
            .map_err(|_| internal_error_response())?
            .unwrap_or_else(|| page.integrity.to_string());
        let warning = if integrity == "corrupt" {
            Some("log_corrupt")
        } else {
            page.warning
        };
        (integrity, warning)
    };

    Ok(Json(json!({
        "build_id": build.id,
        "records": page.records.iter().map(|record| json!({
            "timestamp_ms": record.timestamp_ms,
            "step_index": record.step_index,
            "stream": record.stream,
            "payload_base64": crate::logs::encode_base64(&record.payload)
        })).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor,
        "has_more": page.has_more,
        "log_integrity": log_integrity,
        "warning": warning
    }))
    .into_response())
}

fn log_expired_response() -> Response {
    (
        StatusCode::GONE,
        Json(json!({
            "code": "log_expired",
            "message": "build log is no longer available"
        })),
    )
        .into_response()
}

fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    state.has_valid_authorization(header)
}

fn not_found_response(code: &'static str, message: &'static str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "code": code, "message": message })),
    )
        .into_response()
}

fn invalid_pagination_response() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "code": "invalid_pagination",
            "message": "limit must be between 1 and 100"
        })),
    )
}

async fn create_job_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    payload: Json<JobCreateRequest>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    if !state.has_valid_authorization(header) {
        return Err(unauthorized_response());
    }

    match create_job(&payload.0) {
        Ok(validated) => {
            let job = Job {
                id: uuid::Uuid::new_v4().to_string(),
                name: validated.name,
                repository_url: validated.repository_url,
                default_ref: validated.default_ref,
                disabled: false,
            };
            state
                .store
                .lock()
                .map_err(|_| internal_error_response())?
                .create_job(&job)
                .map_err(|_| internal_error_response())?;
            Ok((
                StatusCode::CREATED,
                Json(json!({
                    "id": job.id,
                    "name": job.name,
                    "repository_url": job.repository_url,
                    "default_ref": job.default_ref,
                    "disabled": job.disabled
                })),
            )
                .into_response())
        }
        Err(api_err) => Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "code": api_err.code,
                "message": api_err.message
            })),
        )
            .into_response()),
    }
}

async fn enqueue_build_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    job_id: AxumPath<String>,
    payload: Json<BuildEnqueueRequest>,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    if !state.has_valid_authorization(header) {
        return Err(unauthorized_response());
    }

    let job_id = job_id.0;
    let store = state.store.lock().map_err(|_| internal_error_response())?;
    let Some(job) = store
        .get_job(&job_id)
        .map_err(|_| internal_error_response())?
    else {
        return Ok((
            StatusCode::NOT_FOUND,
            Json(json!({
                "code": "job_not_found",
                "message": "job does not exist"
            })),
        )
            .into_response());
    };

    let requested_ref =
        match validate_build_ref(payload.ref_name.as_deref().or(job.default_ref.as_deref())) {
            Ok(Some(reference)) => reference,
            Ok(None) => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "code": "missing_ref",
                        "message": "ref is required when the job has no default_ref"
                    })),
                )
                    .into_response());
            }
            Err(api_err) => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "code": api_err.code,
                        "message": api_err.message
                    })),
                )
                    .into_response());
            }
        };

    let mut store = store;
    let build_id = uuid::Uuid::new_v4().to_string();
    match store.enqueue_build(&build_id, &job_id, &requested_ref) {
        Ok(build) => Ok((
            StatusCode::ACCEPTED,
            Json(json!({
                "id": build.id,
                "job_id": build.job_id,
                "number": build.number,
                "status": "queued"
            })),
        )
            .into_response()),
        Err(EnqueueBuildError::JobNotFoundOrDisabled) => Ok((
            StatusCode::CONFLICT,
            Json(json!({
                "code": "job_disabled",
                "message": "job does not exist or is disabled"
            })),
        )
            .into_response()),
        Err(EnqueueBuildError::Storage(_)) => Err(internal_error_response()),
    }
}

fn job_response(job: Job) -> JobResponse {
    JobResponse {
        id: job.id,
        name: job.name,
        repository_url: job.repository_url,
        default_ref: job.default_ref,
        disabled: job.disabled,
    }
}

fn build_response(build: &Build) -> serde_json::Value {
    json!({
        "id": build.id,
        "job_id": build.job_id,
        "number": build.number,
        "requested_ref": build.requested_ref,
        "status": build_status(build.status),
        "created_at_ms": build.created_at,
        "started_at_ms": build.started_at,
        "finished_at_ms": build.finished_at,
        "duration_ms": duration_ms(build.started_at, build.finished_at),
        "cancel_requested_at_ms": build.cancel_requested_at,
        "cancel_reason": build.cancel_reason
    })
}

fn duration_ms(started_at: Option<i64>, finished_at: Option<i64>) -> Option<i64> {
    started_at
        .zip(finished_at)
        .and_then(|(started_at, finished_at)| finished_at.checked_sub(started_at))
}

fn build_status(status: BuildStatus) -> &'static str {
    match status {
        BuildStatus::Queued => "queued",
        BuildStatus::Preparing => "preparing",
        BuildStatus::Running => "running",
        BuildStatus::Cancelling => "cancelling",
        BuildStatus::Succeeded => "succeeded",
        BuildStatus::Failed => "failed",
        BuildStatus::Cancelled => "cancelled",
        BuildStatus::Interrupted => "interrupted",
    }
}

impl AppState {
    fn has_valid_authorization(&self, header: Option<&str>) -> bool {
        let Some(header) = header else {
            return false;
        };
        validate_token_header(header, &self.token)
    }
}

fn unauthorized_response() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "code": "unauthorized",
            "message": "missing or invalid bearer token"
        })),
    )
}

fn internal_error_response() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "code": "internal_error",
            "message": "an internal storage error occurred"
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StepOutcomeRecord;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use std::io::Write;
    use tempfile::tempdir;
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_checks_are_public() {
        let app = build_router("secret-token").unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn readiness_tracks_schema_and_scheduler_state() {
        let mut store = DatabaseStore::open_in_memory().unwrap();
        store.initialize_schema().unwrap();
        let store = Arc::new(Mutex::new(store));
        let control = WorkerControl::default();
        let dir = tempdir().unwrap();
        let _data_directory_lock = DataDirectoryLock::acquire(dir.path().join("data")).unwrap();
        let app = build_router_with_shared_store(
            "secret-token",
            Arc::clone(&store),
            dir.path().join("logs"),
            control.clone(),
            true,
        );
        let ready_request = || {
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap()
        };
        let response = app.clone().oneshot(ready_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let worker = crate::worker::start_worker_pool(
            Arc::clone(&store),
            dir.path().to_path_buf(),
            control.clone(),
            1,
        )
        .unwrap();
        let response = app.clone().oneshot(ready_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        control.request_shutdown().unwrap();
        let response = app.oneshot(ready_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        worker.shutdown(Duration::ZERO).unwrap();
    }

    #[test]
    fn startup_recovers_log_tails_and_persists_integrity() {
        let dir = tempdir().unwrap();
        let logs_dir = dir.path().join("logs");
        let db_path = dir.path().join("renkins.sqlite3");
        let mut store = DatabaseStore::open(&db_path).unwrap();
        store.initialize_schema().unwrap();
        store
            .create_job(&crate::domain::Job {
                id: "job-1".to_string(),
                name: "recovery".to_string(),
                repository_url: "https://example.com/repo.git".to_string(),
                default_ref: None,
                disabled: false,
            })
            .unwrap();
        store
            .enqueue_build("build-1", "job-1", "refs/heads/main")
            .unwrap();
        store
            .enqueue_build("build-2", "job-1", "refs/heads/main")
            .unwrap();
        store.claim_next_build().unwrap().unwrap();
        store.claim_next_build().unwrap().unwrap();

        let recovered_path = logs_dir.join("build-1.rkl");
        let recovered_log = crate::logs::BuildLog::open_append(&recovered_path).unwrap();
        recovered_log
            .append(None, crate::logs::LogStream::Build, b"before restart")
            .unwrap();
        drop(recovered_log);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&recovered_path)
            .unwrap()
            .write_all(&[1, 2])
            .unwrap();

        let corrupt_path = logs_dir.join("build-2.rkl");
        let corrupt_log = crate::logs::BuildLog::open_append(&corrupt_path).unwrap();
        corrupt_log
            .append(None, crate::logs::LogStream::Build, b"corrupt")
            .unwrap();
        drop(corrupt_log);
        let mut bytes = std::fs::read(&corrupt_path).unwrap();
        bytes[18] ^= 0xff;
        std::fs::write(&corrupt_path, bytes).unwrap();

        store.recover_abandoned_builds().unwrap();
        recover_build_logs(&logs_dir, &store).unwrap();

        assert_eq!(
            store.get_build("build-1").unwrap().unwrap().status,
            BuildStatus::Interrupted
        );
        assert_eq!(
            store.get_log_integrity("build-1").unwrap().as_deref(),
            Some("recovered_tail")
        );
        assert_eq!(
            store.get_log_integrity("build-2").unwrap().as_deref(),
            Some("corrupt")
        );
        let page = crate::logs::read_page(&recovered_path, None, 10, false)
            .unwrap()
            .unwrap();
        assert_eq!(page.records.len(), 2);
        assert_eq!(
            page.records[1].payload,
            b"[build] interrupted by server restart; valid log prefix recovered"
        );

        let recovered_size = std::fs::metadata(&recovered_path).unwrap().len();
        recover_build_logs(&logs_dir, &store).unwrap();
        assert_eq!(
            std::fs::metadata(&recovered_path).unwrap().len(),
            recovered_size
        );
        assert_eq!(
            store.get_log_integrity("build-1").unwrap().as_deref(),
            Some("recovered_tail")
        );
    }

    #[tokio::test]
    async fn protected_routes_require_bearer_token() {
        let app = build_router("secret-token").unwrap();

        let missing = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs")
                    .method("POST")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"name":"demo","repository_url":"https://example.com/repo.git"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

        let ok = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs")
                    .method("POST")
                    .header("content-type", "application/json")
                    .header("Authorization", "Bearer secret-token")
                    .body(Body::from(
                        r#"{"name":"demo","repository_url":"https://example.com/repo.git"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn jobs_and_queued_builds_are_persisted_across_router_restarts() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let mut store = DatabaseStore::open(&db_path).unwrap();
        store.initialize_schema().unwrap();
        let app = build_router_with_store("secret-token", store);

        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/jobs")
                    .method("POST")
                    .header("content-type", "application/json")
                    .header("Authorization", "Bearer secret-token")
                    .body(Body::from(
                        r#"{"name":"demo","repository_url":"https://example.com/repo.git","default_ref":"refs/heads/main"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let created_body = to_bytes(created.into_body(), usize::MAX).await.unwrap();
        let created_json: serde_json::Value = serde_json::from_slice(&created_body).unwrap();
        let job_id = created_json["id"].as_str().unwrap();
        assert_ne!(job_id, "job-1");

        let enqueue = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/jobs/{job_id}/builds"))
                    .method("POST")
                    .header("content-type", "application/json")
                    .header("Authorization", "Bearer secret-token")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(enqueue.status(), StatusCode::ACCEPTED);
        let enqueue_body = to_bytes(enqueue.into_body(), usize::MAX).await.unwrap();
        let enqueue_json: serde_json::Value = serde_json::from_slice(&enqueue_body).unwrap();
        assert_eq!(enqueue_json["number"], 1);

        let job_detail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/jobs/{job_id}"))
                    .header("Authorization", "Bearer secret-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(job_detail.status(), StatusCode::OK);

        let builds = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/jobs/{job_id}/builds?limit=10"))
                    .header("Authorization", "Bearer secret-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(builds.status(), StatusCode::OK);
        let builds_body = to_bytes(builds.into_body(), usize::MAX).await.unwrap();
        let builds_json: serde_json::Value = serde_json::from_slice(&builds_body).unwrap();
        assert_eq!(builds_json[0]["status"], "queued");

        let build_id = enqueue_json["id"].as_str().unwrap();
        let build_detail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/builds/{build_id}"))
                    .header("Authorization", "Bearer secret-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(build_detail.status(), StatusCode::OK);

        for (action, expected_disabled) in [("disable", true), ("enable", false)] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/v1/jobs/{job_id}/{action}"))
                        .method("POST")
                        .header("Authorization", "Bearer secret-token")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["disabled"], expected_disabled);
        }

        let invalid_page = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/jobs/{job_id}/builds?limit=101"))
                    .header("Authorization", "Bearer secret-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid_page.status(), StatusCode::BAD_REQUEST);

        drop(app);
        let mut store = DatabaseStore::open(&db_path).unwrap();
        store.initialize_schema().unwrap();
        let jobs = store.list_jobs(10, 0).unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, job_id);
        let builds = store.list_builds(job_id, 10, 0).unwrap();
        assert_eq!(builds.len(), 1);
        assert_eq!(builds[0].status, crate::domain::BuildStatus::Queued);
        assert_eq!(builds[0].requested_ref, "refs/heads/main");
    }

    #[tokio::test]
    async fn build_logs_are_authenticated_paginated_and_report_expiry() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("renkins.sqlite3");
        let mut store = DatabaseStore::open(&db_path).unwrap();
        store.initialize_schema().unwrap();
        store
            .create_job(&Job {
                id: "job-1".to_string(),
                name: "logs".to_string(),
                repository_url: "https://example.com/repo.git".to_string(),
                default_ref: None,
                disabled: false,
            })
            .unwrap();
        store
            .enqueue_build("build-1", "job-1", "refs/heads/main")
            .unwrap();
        let app = build_router_with_store("secret-token", store);

        let missing_token = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/builds/build-1/logs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_token.status(), StatusCode::UNAUTHORIZED);

        let log_path = dir.path().join("logs/build-1.rkl");
        let log = crate::logs::BuildLog::open_append(&log_path).unwrap();
        log.append(Some(0), crate::logs::LogStream::Stdout, &[b'h', b'i', 0xff])
            .unwrap();
        log.sync().unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/builds/build-1/logs?limit=1")
                    .header("Authorization", concat!("Bearer ", "secret-token"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let page: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(page["records"][0]["payload_base64"], "aGn/");
        assert_eq!(page["records"][0]["stream"], "stdout");
        assert_eq!(page["records"][0]["step_index"], 0);
        assert_eq!(page["log_integrity"], "valid");

        let mut log_bytes = std::fs::read(&log_path).unwrap();
        let last_byte = log_bytes.len() - 1;
        log_bytes[last_byte] ^= 0xff;
        std::fs::write(&log_path, log_bytes).unwrap();
        let corrupt = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/builds/build-1/logs")
                    .header("Authorization", concat!("Bearer ", "secret-token"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(corrupt.status(), StatusCode::OK);
        let body = to_bytes(corrupt.into_body(), 64 * 1024).await.unwrap();
        let corrupt_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(corrupt_json["log_integrity"], "corrupt");
        assert_eq!(corrupt_json["warning"], "log_corrupt");
        assert_eq!(
            DatabaseStore::open(&db_path)
                .unwrap()
                .get_log_integrity("build-1")
                .unwrap()
                .as_deref(),
            Some("corrupt")
        );

        let invalid_cursor = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/builds/build-1/logs?cursor=0000000000000001")
                    .header("Authorization", concat!("Bearer ", "secret-token"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid_cursor.status(), StatusCode::BAD_REQUEST);

        let expired_store = DatabaseStore::open(&db_path).unwrap();
        expired_store
            .transition_build("build-1", BuildStatus::Queued, BuildStatus::Cancelled)
            .unwrap();
        std::fs::remove_file(log_path).unwrap();
        drop(app);
        let app = build_router_with_store("secret-token", expired_store);
        let expired = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/builds/build-1/logs?cursor=0000000000000001")
                    .header("Authorization", concat!("Bearer ", "secret-token"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(expired.status(), StatusCode::GONE);
        assert_eq!(
            DatabaseStore::open(&db_path)
                .unwrap()
                .get_log_integrity("build-1")
                .unwrap()
                .as_deref(),
            Some("expired")
        );
    }

    #[tokio::test]
    async fn queued_build_cancellation_is_authenticated_and_idempotent() {
        let mut store = DatabaseStore::open_in_memory().unwrap();
        store.initialize_schema().unwrap();
        store
            .create_job(&Job {
                id: "job-1".to_string(),
                name: "cancel".to_string(),
                repository_url: "https://example.com/repo.git".to_string(),
                default_ref: None,
                disabled: false,
            })
            .unwrap();
        store
            .enqueue_build("build-1", "job-1", "refs/heads/main")
            .unwrap();
        let app = build_router_with_store("secret-token", store);

        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/builds/build-1/cancel")
                    .method("POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/api/v1/builds/build-1/cancel")
                        .method("POST")
                        .header("Authorization", concat!("Bearer ", "secret-token"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
            let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(response["status"], "cancelled");
            assert_eq!(response["cancel_reason"], "user");
            assert!(response["cancel_requested_at_ms"].is_number());
        }

        let missing = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/builds/no-such-build/cancel")
                    .method("POST")
                    .header("Authorization", concat!("Bearer ", "secret-token"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn build_detail_includes_persisted_build_and_step_timings() {
        let mut store = DatabaseStore::open_in_memory().unwrap();
        store.initialize_schema().unwrap();
        store
            .create_job(&Job {
                id: "job-1".to_string(),
                name: "timing".to_string(),
                repository_url: "https://example.com/repo.git".to_string(),
                default_ref: None,
                disabled: false,
            })
            .unwrap();
        store
            .enqueue_build("build-1", "job-1", "refs/heads/main")
            .unwrap();
        assert!(store.claim_next_build().unwrap().is_some());
        assert!(
            store
                .persist_pipeline_and_start(
                    "build-1",
                    "version: 1\nsteps:\n  - name: test\n    run: true\n",
                )
                .unwrap()
        );
        store
            .record_step_outcomes(
                "build-1",
                &[StepOutcomeRecord {
                    step_index: 0,
                    status: "succeeded",
                    exit_code: Some(0),
                    started_at: Some(111),
                    finished_at: 222,
                }],
            )
            .unwrap();
        assert!(
            store
                .transition_build("build-1", BuildStatus::Running, BuildStatus::Succeeded)
                .unwrap()
        );
        let app = build_router_with_store("secret-token", store);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/builds/build-1")
                    .header("Authorization", concat!("Bearer ", "secret-token"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(response["created_at_ms"].is_number());
        assert!(response["started_at_ms"].is_number());
        assert!(response["finished_at_ms"].is_number());
        assert_eq!(
            response["duration_ms"],
            response["finished_at_ms"].as_i64().unwrap()
                - response["started_at_ms"].as_i64().unwrap()
        );
        assert_eq!(response["steps"][0]["started_at_ms"], 111);
        assert_eq!(response["steps"][0]["finished_at_ms"], 222);
        assert_eq!(response["steps"][0]["duration_ms"], 111);
    }
}
