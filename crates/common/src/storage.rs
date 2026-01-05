//! Storage configuration and OpenDAL operator building.

use opendal::Operator;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur during storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("OpenDAL error: {0}")]
    OpenDal(#[from] opendal::Error),

    #[error("Configuration error: {0}")]
    Config(String),
}

/// S3-compatible storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 endpoint URL (e.g., http://localhost:9000 for MinIO)
    pub endpoint: String,
    /// Access key ID (if empty, uses AWS default credential chain)
    #[serde(default)]
    pub access_key: String,
    /// Secret access key (if empty, uses AWS default credential chain)
    #[serde(default)]
    pub secret_key: String,
    /// Bucket name
    pub bucket: String,
    /// AWS region
    pub region: String,
    /// Use AWS default credential chain (reads from env vars)
    #[serde(default)]
    pub use_default_credentials: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:9000".to_string(),
            access_key: "admin".to_string(),
            secret_key: "password".to_string(),
            bucket: "warehouse".to_string(),
            region: "us-east-1".to_string(),
            use_default_credentials: false,
        }
    }
}

impl S3Config {
    /// Build an OpenDAL operator from this configuration.
    pub fn build_operator(&self) -> Result<Operator, StorageError> {
        build_opendal_operator(self)
    }
}

/// Build an OpenDAL operator for S3.
pub fn build_opendal_operator(s3_config: &S3Config) -> Result<Operator, StorageError> {
    let mut builder = opendal::services::S3::default()
        .bucket(&s3_config.bucket)
        .region(&s3_config.region)
        .endpoint(&s3_config.endpoint);

    // Use explicit credentials or AWS default credential chain
    if s3_config.use_default_credentials {
        // Try environment variables first
        let has_env_creds = std::env::var("AWS_ACCESS_KEY_ID").is_ok()
            && std::env::var("AWS_SECRET_ACCESS_KEY").is_ok();

        if has_env_creds {
            // Use env vars and disable IMDS (for local development)
            builder = builder
                .access_key_id(&std::env::var("AWS_ACCESS_KEY_ID").unwrap())
                .secret_access_key(&std::env::var("AWS_SECRET_ACCESS_KEY").unwrap());
            if let Ok(token) = std::env::var("AWS_SESSION_TOKEN") {
                builder = builder.session_token(&token);
            }
            builder = builder.disable_ec2_metadata();
        }
        // If no env vars, let opendal use full AWS credential chain (including IMDS on EC2)
    } else {
        // Explicit credentials provided in config
        builder = builder
            .access_key_id(&s3_config.access_key)
            .secret_access_key(&s3_config.secret_key)
            .disable_config_load()
            .disable_ec2_metadata();
    }

    Ok(Operator::new(builder)?.finish())
}
