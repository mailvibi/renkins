use serde::Deserialize;
use std::path::{Component, Path};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub version: u64,
    pub timeout_minutes: Option<u64>,
    #[serde(default)]
    pub steps: Vec<StepConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepConfig {
    pub name: String,
    pub run: String,
    #[serde(default)]
    pub working_directory: Option<String>,
    #[serde(default)]
    pub timeout_minutes: Option<u64>,
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipelineError {
    MissingVersion,
    InvalidVersion,
    MissingSteps,
    EmptyStepName,
    EmptyCommand,
    InvalidTimeout,
    WorkingDirectoryTraversal,
    UnknownField,
    ReservedEnvironmentKey,
}

pub fn parse_pipeline_yaml(input: &str) -> Result<PipelineConfig, PipelineError> {
    let raw: serde_yaml::Value =
        serde_yaml::from_str(input).map_err(|_| PipelineError::UnknownField)?;

    let version = match raw.get("version") {
        None => return Err(PipelineError::MissingVersion),
        Some(value) => value.as_u64().ok_or(PipelineError::InvalidVersion)?,
    };

    let pipeline: PipelineConfig =
        serde_yaml::from_str(input).map_err(|_| PipelineError::UnknownField)?;

    if version != 1 || pipeline.version != 1 {
        return Err(PipelineError::InvalidVersion);
    }

    if pipeline.timeout_minutes == Some(0) {
        return Err(PipelineError::InvalidTimeout);
    }

    if pipeline.steps.is_empty() || pipeline.steps.len() > 100 {
        return Err(PipelineError::MissingSteps);
    }

    for step in &pipeline.steps {
        if step.name.trim().is_empty() {
            return Err(PipelineError::EmptyStepName);
        }
        if step.run.trim().is_empty() {
            return Err(PipelineError::EmptyCommand);
        }
        if let Some(timeout) = step.timeout_minutes
            && timeout == 0
        {
            return Err(PipelineError::InvalidTimeout);
        }

        if let Some(working_directory) = &step.working_directory {
            let path = Path::new(working_directory);
            if path.is_absolute()
                || path
                    .components()
                    .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
            {
                return Err(PipelineError::WorkingDirectoryTraversal);
            }
        }

        for key in step.env.keys() {
            let normalized = key.trim();
            let upper = normalized.to_ascii_uppercase();
            if upper.starts_with("RENKINS_")
                || matches!(
                    upper.as_str(),
                    "HOME" | "PATH" | "PWD" | "SHELL" | "USER" | "UID" | "GID"
                )
            {
                return Err(PipelineError::ReservedEnvironmentKey);
            }
        }
    }

    Ok(pipeline)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_minimal_valid_pipeline() {
        let yaml = r#"
version: 1
steps:
  - name: test
    run: cargo test
"#;

        let pipeline = parse_pipeline_yaml(yaml).expect("valid pipeline should parse");
        assert_eq!(pipeline.version, 1);
        assert_eq!(pipeline.steps.len(), 1);
        assert_eq!(pipeline.steps[0].name, "test");
    }

    #[test]
    fn rejects_invalid_version() {
        let yaml = "version: 2\nsteps: []\n";
        assert!(matches!(
            parse_pipeline_yaml(yaml),
            Err(PipelineError::InvalidVersion)
        ));
    }

    #[test]
    fn rejects_traversal_in_working_directory() {
        let yaml = r#"
version: 1
steps:
  - name: test
    run: cargo test
    working_directory: ../unsafe
"#;
        assert!(matches!(
            parse_pipeline_yaml(yaml),
            Err(PipelineError::WorkingDirectoryTraversal)
        ));
    }
}
