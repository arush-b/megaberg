//! Configuration types for megaberg CLI.
use std::collections::HashMap;

use anyhow::{Context, Result};
use common::S3Config;
use engine::planner::CatalogConfig;
use opendal::Operator;
use serde::Deserialize;

/// S3 configuration (JSON format)
#[derive(Debug, Deserialize)]
pub struct S3ConfigJson {
    pub endpoint: String,
    /// Optional - if not provided, uses default credential chain
    #[serde(default)]
    pub access_key: Option<String>,
    /// Optional - if not provided, uses default credential chain
    #[serde(default)]
    pub secret_key: Option<String>,
    pub bucket: String,
    pub region: String,
    /// If true, uses AWS default credential chain (env vars, ~/.aws/credentials, IAM role, etc.)
    #[serde(default = "default_true")]
    pub use_default_credentials: bool,
}

fn default_true() -> bool {
    true
}

impl S3ConfigJson {
    /// Convert to common::S3Config
    pub fn to_s3_config(&self) -> S3Config {
        S3Config {
            endpoint: self.endpoint.clone(),
            access_key: self.access_key.clone().unwrap_or_default(),
            secret_key: self.secret_key.clone().unwrap_or_default(),
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            use_default_credentials: self.use_default_credentials,
        }
    }

    /// Build an OpenDAL operator for S3
    pub fn build_operator(&self) -> Result<Operator> {
        self.to_s3_config()
            .build_operator()
            .context("Failed to create OpenDAL S3 operator")
    }
}

impl From<&S3ConfigJson> for S3Config {
    fn from(cfg: &S3ConfigJson) -> Self {
        cfg.to_s3_config()
    }
}

/// Catalog configuration
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CatalogConfigJson {
    Rest {
        uri: String,
        #[serde(default)]
        warehouse: Option<String>,
    },
    Glue {
        warehouse: String,
        #[serde(default)]
        catalog_id: Option<String>,
        #[serde(default)]
        region: Option<String>,
    },
}

impl From<&CatalogConfigJson> for CatalogConfig {
    fn from(cfg: &CatalogConfigJson) -> Self {
        match cfg {
            CatalogConfigJson::Rest { uri, warehouse } => CatalogConfig::Rest {
                uri: uri.clone(),
                warehouse: warehouse.clone(),
                props: HashMap::new(),
            },
            CatalogConfigJson::Glue {
                warehouse,
                catalog_id,
                region,
            } => CatalogConfig::Glue {
                warehouse: warehouse.clone(),
                catalog_id: catalog_id.clone(),
                region: region.clone(),
                props: HashMap::new(),
            },
        }
    }
}

/// Table configuration
#[derive(Debug, Deserialize)]
pub struct TableConfigJson {
    pub namespace: String,
    pub name: String,
}

/// Compaction configuration
#[derive(Debug, Clone, Deserialize)]
pub struct CompactionConfigJson {
    #[serde(default = "default_target_file_size_mb")]
    pub target_file_size_mb: u64,
    #[serde(default = "default_min_files_per_partition")]
    pub min_files_per_partition: usize,
}

pub fn default_target_file_size_mb() -> u64 {
    256
}

pub fn default_min_files_per_partition() -> usize {
    2
}

impl Default for CompactionConfigJson {
    fn default() -> Self {
        Self {
            target_file_size_mb: default_target_file_size_mb(),
            min_files_per_partition: default_min_files_per_partition(),
        }
    }
}

/// Top-level config
#[derive(Debug, Deserialize)]
pub struct Config {
    pub s3: S3ConfigJson,
    pub catalog: CatalogConfigJson,
    pub table: TableConfigJson,
    #[serde(default)]
    pub compaction: Option<CompactionConfigJson>,
}
