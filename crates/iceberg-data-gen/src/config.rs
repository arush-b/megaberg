use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub s3: S3Config,
    pub catalog: CatalogConfig,
    pub table: TableConfig,
    pub generate: GenerateConfig,
}

#[derive(Debug, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    #[serde(default)]
    pub disable_config_load: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CatalogConfig {
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

#[derive(Debug, Deserialize)]
pub struct TableConfig {
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct GenerateConfig {
    #[serde(default = "default_total_size_mb")]
    pub total_size_mb: usize,
    #[serde(default = "default_file_size_kb")]
    pub file_size_kb: usize,
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,
    #[serde(default = "default_compression")]
    pub compression: String,
    /// Partition cardinality settings
    #[serde(default)]
    pub partitions: PartitionConfig,
}

#[derive(Debug, Deserialize)]
pub struct PartitionConfig {
    /// Number of distinct aliases (default: 1)
    #[serde(default = "default_one")]
    pub num_aliases: usize,
    /// Number of distinct workspaces per alias (default: 1)
    #[serde(default = "default_one")]
    pub workspaces_per_alias: usize,
    /// Number of distinct clusters per workspace (default: 1)
    #[serde(default = "default_one")]
    pub clusters_per_workspace: usize,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            num_aliases: 1,
            workspaces_per_alias: 1,
            clusters_per_workspace: 1,
        }
    }
}

fn default_total_size_mb() -> usize {
    1024 // 1GB
}
fn default_file_size_kb() -> usize {
    10 // 10KB
}
fn default_parallelism() -> usize {
    4
}
fn default_compression() -> String {
    "snappy".to_string()
}
fn default_one() -> usize {
    1
}

// End of config.rs
