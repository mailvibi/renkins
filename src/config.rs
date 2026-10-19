use std::fs;
use std::io::Write;
use std::net::ToSocketAddrs;
use std::path::Path;

pub const DEFAULT_MAX_CONCURRENT_BUILDS: usize = 1;
pub const MAX_CONCURRENT_BUILDS: usize = 32;
pub const DEFAULT_SHUTDOWN_GRACE_SECONDS: u64 = 30;
pub const MAX_SHUTDOWN_GRACE_SECONDS: u64 = 3_600;
pub const DEFAULT_MAX_BUILD_OUTPUT_BYTES: u64 = 10 * 1024 * 1024;
pub const MAX_BUILD_OUTPUT_BYTES: u64 = 1024 * 1024 * 1024;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub listen: String,
    pub data_dir: String,
    pub auth_token_file: String,
    pub max_concurrent_builds: usize,
    pub shutdown_grace_seconds: u64,
    pub max_build_output_bytes: u64,
}

impl ServerConfig {
    pub fn new(
        listen: impl Into<String>,
        data_dir: impl Into<String>,
        auth_token_file: impl Into<String>,
    ) -> Result<Self, String> {
        let listen = listen.into();
        let data_dir = data_dir.into();
        let auth_token_file = auth_token_file.into();

        let listen = parse_listen_address(&listen)?;
        if data_dir.trim().is_empty() {
            return Err("data_dir must be set".to_string());
        }
        if auth_token_file.trim().is_empty() {
            return Err("auth_token_file must be set".to_string());
        }

        Ok(Self {
            listen,
            data_dir,
            auth_token_file,
            max_concurrent_builds: DEFAULT_MAX_CONCURRENT_BUILDS,
            shutdown_grace_seconds: DEFAULT_SHUTDOWN_GRACE_SECONDS,
            max_build_output_bytes: DEFAULT_MAX_BUILD_OUTPUT_BYTES,
        })
    }

    pub fn with_worker_settings(
        mut self,
        max_concurrent_builds: usize,
        shutdown_grace_seconds: u64,
    ) -> Result<Self, String> {
        if !(1..=MAX_CONCURRENT_BUILDS).contains(&max_concurrent_builds) {
            return Err(format!(
                "max_concurrent_builds must be between 1 and {MAX_CONCURRENT_BUILDS}"
            ));
        }
        if shutdown_grace_seconds > MAX_SHUTDOWN_GRACE_SECONDS {
            return Err(format!(
                "shutdown_grace_seconds must not exceed {MAX_SHUTDOWN_GRACE_SECONDS}"
            ));
        }
        self.max_concurrent_builds = max_concurrent_builds;
        self.shutdown_grace_seconds = shutdown_grace_seconds;
        Ok(self)
    }

    pub fn with_max_build_output_bytes(
        mut self,
        max_build_output_bytes: u64,
    ) -> Result<Self, String> {
        if !(1..=MAX_BUILD_OUTPUT_BYTES).contains(&max_build_output_bytes) {
            return Err(format!(
                "max_build_output_bytes must be between 1 and {MAX_BUILD_OUTPUT_BYTES}"
            ));
        }
        self.max_build_output_bytes = max_build_output_bytes;
        Ok(self)
    }
}

pub fn parse_server_config_file(path: impl AsRef<Path>) -> Result<ServerConfig, String> {
    let contents = fs::read_to_string(path.as_ref())
        .map_err(|err| format!("failed to read config file {:?}: {err}", path.as_ref()))?;
    parse_server_config_str(&contents)
}

pub fn parse_server_config_str(contents: &str) -> Result<ServerConfig, String> {
    #[derive(serde::Deserialize)]
    struct RawServerConfig {
        listen: String,
        data_dir: String,
        auth_token_file: String,
        #[serde(default = "default_max_concurrent_builds")]
        max_concurrent_builds: usize,
        #[serde(default = "default_shutdown_grace_seconds")]
        shutdown_grace_seconds: u64,
        #[serde(default = "default_max_build_output_bytes")]
        max_build_output_bytes: u64,
    }

    let config: RawServerConfig =
        toml::from_str(contents).map_err(|err| format!("invalid server config: {err}"))?;

    ServerConfig::new(config.listen, config.data_dir, config.auth_token_file)?
        .with_worker_settings(config.max_concurrent_builds, config.shutdown_grace_seconds)?
        .with_max_build_output_bytes(config.max_build_output_bytes)
}

fn default_max_concurrent_builds() -> usize {
    DEFAULT_MAX_CONCURRENT_BUILDS
}

fn default_shutdown_grace_seconds() -> u64 {
    DEFAULT_SHUTDOWN_GRACE_SECONDS
}

fn default_max_build_output_bytes() -> u64 {
    DEFAULT_MAX_BUILD_OUTPUT_BYTES
}

pub fn parse_listen_address(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("listen address must not be empty".to_string());
    }

    let resolved = value
        .to_socket_addrs()
        .map_err(|err| format!("invalid listen address '{value}': {err}"))?
        .collect::<Vec<_>>();

    if resolved.is_empty() {
        return Err(format!("could not resolve listen address '{value}'"));
    }

    require_loopback(value)?;
    Ok(value.to_string())
}

pub fn require_loopback(listen: &str) -> Result<(), String> {
    let candidates = listen
        .trim()
        .to_socket_addrs()
        .map_err(|err| format!("invalid listen address '{listen}': {err}"))?
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        return Err(format!(
            "listen address '{listen}' did not resolve to any socket"
        ));
    }

    if !candidates.iter().all(|addr| match addr.ip() {
        std::net::IpAddr::V4(ip) => ip.is_loopback(),
        std::net::IpAddr::V6(ip) => ip.is_loopback(),
    }) {
        return Err(format!(
            "listen address '{listen}' must bind only to loopback interfaces"
        ));
    }

    Ok(())
}

pub fn validate_token_header(header: &str, expected_token: &str) -> bool {
    let Some((scheme, token)) = header.split_once(' ') else {
        return false;
    };

    if !scheme.eq_ignore_ascii_case("Bearer") || expected_token.is_empty() {
        return false;
    }

    secure_compare(token.as_bytes(), expected_token.as_bytes())
}

fn secure_compare(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    let mut result = 0u8;
    for (left_byte, right_byte) in left.iter().zip(right.iter()) {
        result |= left_byte ^ right_byte;
    }
    result == 0
}

pub fn read_or_create_token_file(path: impl AsRef<Path>) -> Result<String, String> {
    let path = path.as_ref();
    if !path.exists() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("failed to create token dir {:?}: {err}", parent))?;
        }

        let token = uuid::Uuid::new_v4().to_string();

        #[cfg(unix)]
        {
            use std::fs::OpenOptions;
            use std::os::unix::fs::OpenOptionsExt;

            let mut options = OpenOptions::new();
            options.write(true).create(true).truncate(true).mode(0o600);
            let mut file = options
                .open(path)
                .map_err(|err| format!("failed to create token file {:?}: {err}", path))?;
            writeln!(file, "{token}")
                .map_err(|err| format!("failed to write token file {:?}: {err}", path))?;
        }

        #[cfg(not(unix))]
        {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)
                .map_err(|err| format!("failed to create token file {:?}: {err}", path))?;
            writeln!(file, "{token}")
                .map_err(|err| format!("failed to write token file {:?}: {err}", path))?;
        }
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|err| format!("failed to lock token file permissions {:?}: {err}", path))?;
        return Ok(token);
    }

    let token = fs::read_to_string(path)
        .map_err(|err| format!("failed to read token file {:?}: {err}", path))?
        .trim()
        .to_string();
    if token.is_empty() {
        return Err(format!("token file {:?} is empty", path));
    }

    #[cfg(unix)]
    {
        let mode = fs::metadata(path)
            .map_err(|err| format!("failed to inspect token file {:?}: {err}", path))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Err(format!(
                "token file {:?} has overly broad permissions; use 0600",
                path
            ));
        }
    }

    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_loopback_listen_addresses() {
        assert_eq!(
            parse_listen_address("127.0.0.1:8080").unwrap(),
            "127.0.0.1:8080"
        );
        assert_eq!(parse_listen_address("[::1]:8080").unwrap(), "[::1]:8080");
    }

    #[test]
    fn reject_non_loopback_bindings() {
        assert!(parse_listen_address("0.0.0.0:8080").is_err());
        assert!(parse_listen_address("192.168.1.10:8080").is_err());
    }

    #[test]
    fn bearer_tokens_match_in_constant_time() {
        let token = "admin-token-123";
        assert!(validate_token_header("Bearer admin-token-123", token));
        assert!(!validate_token_header("Bearer wrong-token", token));
        assert!(!validate_token_header("Basic admin-token-123", token));
    }

    #[test]
    fn parses_server_toml_config() {
        let config = r#"
            listen = "127.0.0.1:8080"
            data_dir = "./data"
            auth_token_file = "./data/admin-token"
        "#;

        let parsed = parse_server_config_str(config).unwrap();
        assert_eq!(parsed.listen, "127.0.0.1:8080");
        assert_eq!(parsed.data_dir, "./data");
        assert_eq!(parsed.auth_token_file, "./data/admin-token");
        assert_eq!(parsed.max_concurrent_builds, DEFAULT_MAX_CONCURRENT_BUILDS);
        assert_eq!(
            parsed.shutdown_grace_seconds,
            DEFAULT_SHUTDOWN_GRACE_SECONDS
        );
        assert_eq!(
            parsed.max_build_output_bytes,
            DEFAULT_MAX_BUILD_OUTPUT_BYTES
        );
    }

    #[test]
    fn parses_and_validates_worker_settings() {
        let config = r#"
            listen = "127.0.0.1:8080"
            data_dir = "./data"
            auth_token_file = "./data/admin-token"
            max_concurrent_builds = 4
            shutdown_grace_seconds = 15
            max_build_output_bytes = 2048
        "#;

        let parsed = parse_server_config_str(config).unwrap();
        assert_eq!(parsed.max_concurrent_builds, 4);
        assert_eq!(parsed.shutdown_grace_seconds, 15);
        assert_eq!(parsed.max_build_output_bytes, 2048);

        assert!(
            ServerConfig::new("127.0.0.1:8080", "./data", "./data/token")
                .unwrap()
                .with_worker_settings(0, 15)
                .is_err()
        );
        assert!(
            ServerConfig::new("127.0.0.1:8080", "./data", "./data/token")
                .unwrap()
                .with_worker_settings(1, MAX_SHUTDOWN_GRACE_SECONDS + 1)
                .is_err()
        );
        assert!(
            ServerConfig::new("127.0.0.1:8080", "./data", "./data/token")
                .unwrap()
                .with_max_build_output_bytes(0)
                .is_err()
        );
        assert!(
            ServerConfig::new("127.0.0.1:8080", "./data", "./data/token")
                .unwrap()
                .with_max_build_output_bytes(MAX_BUILD_OUTPUT_BYTES + 1)
                .is_err()
        );
    }

    #[test]
    fn creates_a_secure_token_file_when_missing() {
        let dir = std::env::temp_dir().join(format!("renkins-token-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("admin-token");

        let token = read_or_create_token_file(&path).unwrap();
        assert!(!token.is_empty());
        assert!(path.exists());

        #[cfg(unix)]
        {
            let metadata = std::fs::metadata(&path).unwrap();
            let mode = metadata.permissions().mode();
            assert_eq!(mode & 0o077, 0);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
