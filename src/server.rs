use std::collections::HashMap;
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

use crate::api::{BuildEnqueueRequest, JobCreateRequest, JobResponse, create_job, enqueue_build};
use crate::config::{ServerConfig, read_or_create_token_file, validate_token_header};

#[derive(Clone, Debug)]
pub struct AppState {
    token: String,
    jobs: Arc<Mutex<HashMap<String, JobResponse>>>,
}

pub fn build_router(expected_token: &str) -> Router {
    Router::new()
        .route("/healthz", get(healthcheck))
        .route("/readyz", get(healthcheck))
        .route("/api/v1/jobs", get(list_jobs))
        .route("/api/v1/jobs", post(create_job_route))
        .route("/api/v1/jobs/{job_id}/builds", post(enqueue_build_route))
        .with_state(AppState {
            token: expected_token.to_string(),
            jobs: Arc::new(Mutex::new(HashMap::new())),
        })
}

pub async fn serve(config: &ServerConfig) -> Result<(), String> {
    let token = read_or_create_token_file(&config.auth_token_file)
        .map_err(|err| format!("failed to initialize auth token: {err}"))?;

    let router = build_router(&token);
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
        .jobs
        .lock()
        .map_err(|_| unauthorized_response())?
        .values()
        .cloned()
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
        Ok(job) => {
            state
                .jobs
                .lock()
                .map_err(|_| unauthorized_response())?
                .insert(job.id.clone(), job.clone());
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
    let Some(_job) = state
        .jobs
        .lock()
        .map_err(|_| unauthorized_response())?
        .get(&job_id)
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

    match enqueue_build(&payload.0) {
        Ok(build) => Ok((
            StatusCode::ACCEPTED,
            Json(json!({
                "id": build.id,
                "job_id": build.job_id,
                "status": build.status
            })),
        )
            .into_response()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_checks_are_public() {
        let app = build_router("secret-token");

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
        let app = build_router("secret-token");

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
}
