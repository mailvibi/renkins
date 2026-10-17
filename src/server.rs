use std::path::Path;
use std::sync::{Arc, Mutex};

use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::json;

use crate::api::{
    BuildEnqueueRequest, JobCreateRequest, JobResponse, create_job, validate_build_ref,
};
use crate::config::{ServerConfig, read_or_create_token_file, validate_token_header};
use crate::domain::Job;
use crate::storage::{DatabaseStore, EnqueueBuildError};

#[derive(Clone, Debug)]
pub struct AppState {
    token: String,
    store: Arc<Mutex<DatabaseStore>>,
}

pub fn build_router(expected_token: &str) -> Result<Router, String> {
    let mut store = DatabaseStore::open_in_memory()?;
    store.initialize_schema()?;
    Ok(build_router_with_store(expected_token, store))
}

pub fn build_router_with_store(expected_token: &str, store: DatabaseStore) -> Router {
    Router::new()
        .route("/healthz", get(healthcheck))
        .route("/readyz", get(healthcheck))
        .route("/api/v1/jobs", get(list_jobs))
        .route("/api/v1/jobs", post(create_job_route))
        .route("/api/v1/jobs/{job_id}/builds", post(enqueue_build_route))
        .with_state(AppState {
            token: expected_token.to_string(),
            store: Arc::new(Mutex::new(store)),
        })
}

pub async fn serve(config: &ServerConfig) -> Result<(), String> {
    let token = read_or_create_token_file(&config.auth_token_file)
        .map_err(|err| format!("failed to initialize auth token: {err}"))?;

    let database_path = Path::new(&config.data_dir).join("renkins.sqlite3");
    let mut store = DatabaseStore::open(&database_path)?;
    store.initialize_schema()?;
    store.recover_abandoned_builds()?;
    let router = build_router_with_store(&token, store);
    let listener = tokio::net::TcpListener::bind(&config.listen)
        .await
        .map_err(|err| format!("failed to bind {}: {err}", config.listen))?;

    axum::serve(listener, router)
        .await
        .map_err(|err| format!("server exited with error: {err}"))?;

    Ok(())
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

async fn list_jobs(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let header = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    if !state.has_valid_authorization(header) {
        return Err(unauthorized_response());
    }

    let jobs = state
        .store
        .lock()
        .map_err(|_| internal_error_response())?
        .list_jobs(100, 0)
        .map_err(|_| internal_error_response())?
        .into_iter()
        .map(job_response)
        .collect::<Vec<_>>();

    Ok(Json(json!(jobs)).into_response())
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
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
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
        assert_eq!(response.status(), StatusCode::OK);
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
}
