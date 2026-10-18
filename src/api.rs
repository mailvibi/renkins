use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCreateRequest {
    pub name: String,
    pub repository_url: String,
    pub default_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobResponse {
    pub id: String,
    pub name: String,
    pub repository_url: String,
    pub default_ref: Option<String>,
    pub disabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildEnqueueRequest {
    pub ref_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BuildResponse {
    pub id: String,
    pub job_id: String,
    pub status: String,
}

pub fn validate_name(name: &str) -> Result<(), ApiError> {
    if name.trim().is_empty() || name.len() > 128 {
        return Err(ApiError {
            code: "invalid_name",
            message: "job name must be non-empty and <= 128 characters".to_string(),
        });
    }
    Ok(())
}

pub fn validate_repository_url(url: &str) -> Result<(), ApiError> {
    if !url.starts_with("https://") && !url.starts_with("git@") {
        return Err(ApiError {
            code: "invalid_repository_url",
            message: "repository_url must be https or SSH-style git@".to_string(),
        });
    }
    Ok(())
}

pub fn validate_build_ref(reference: Option<&str>) -> Result<Option<String>, ApiError> {
    let Some(reference) = reference else {
        return Ok(None);
    };

    if reference.trim().is_empty() {
        return Err(ApiError {
            code: "invalid_ref",
            message: "ref must not be empty".to_string(),
        });
    }
    if crate::git::validate_ref(reference).is_err() {
        return Err(ApiError {
            code: "invalid_ref",
            message: "ref must be a fully qualified branch/tag ref or full commit ID".to_string(),
        });
    }

    Ok(Some(reference.to_string()))
}

pub fn create_job(request: &JobCreateRequest) -> Result<JobResponse, ApiError> {
    validate_name(&request.name)?;
    validate_repository_url(&request.repository_url)?;
    let default_ref = validate_build_ref(request.default_ref.as_deref())?;

    Ok(JobResponse {
        id: "job-1".to_string(),
        name: request.name.clone(),
        repository_url: request.repository_url.clone(),
        default_ref,
        disabled: false,
    })
}

pub fn enqueue_build(request: &BuildEnqueueRequest) -> Result<BuildResponse, ApiError> {
    validate_build_ref(request.ref_name.as_deref())?;

    Ok(BuildResponse {
        id: "build-1".to_string(),
        job_id: "job-1".to_string(),
        status: "queued".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_requests_validate_names_and_urls() {
        let job = JobCreateRequest {
            name: "demo".to_string(),
            repository_url: "https://example.com/repo.git".to_string(),
            default_ref: Some("refs/heads/main".to_string()),
        };
        assert!(create_job(&job).is_ok());

        let invalid_name = JobCreateRequest {
            name: "".to_string(),
            repository_url: "https://example.com/repo.git".to_string(),
            default_ref: None,
        };
        assert!(matches!(
            create_job(&invalid_name),
            Err(ApiError {
                code: "invalid_name",
                ..
            })
        ));

        let invalid_url = JobCreateRequest {
            name: "demo".to_string(),
            repository_url: "ftp://example.com/repo.git".to_string(),
            default_ref: None,
        };
        assert!(matches!(
            create_job(&invalid_url),
            Err(ApiError {
                code: "invalid_repository_url",
                ..
            })
        ));

        assert!(validate_build_ref(Some("main")).is_err());
        assert!(validate_build_ref(Some("refs/heads/main")).is_ok());
    }

    #[test]
    fn build_enqueue_accepts_fully_qualified_refs() {
        let build = enqueue_build(&BuildEnqueueRequest {
            ref_name: Some("refs/heads/main".to_string()),
        });
        assert!(build.is_ok());
    }
}
