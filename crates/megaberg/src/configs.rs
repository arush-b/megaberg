//! Configuration types for megaberg CLI.
//!
//! Two layers:
//! - `ConfigFile`: JSON-deserializable structure (what the user writes)
//! - `AppConfig`: Validated, ready-to-use config (what the code uses)

use std::collections::HashMap;

use anyhow::{Context, Result};
use engine::executor::S3Config;
use engine::planner::{
    CatalogConfig, LookbackWindow, PartitionGranularity, PartitionTimeField, PartitionTimeSpec,
    TimeComponent, TimeWindowConfig,
};
use opendal::Operator;
use serde::Deserialize;

// ============================================================================
// AppConfig - The validated runtime configuration
// ============================================================================

/// Validated, ready-to-use configuration.
/// Built from ConfigFile after validation and default resolution.
#[derive(Debug, Clone)]
pub struct AppConfig {
    pub s3: S3Config,
    pub source: SourceConfig,
    pub sink: SinkConfig,
    pub compaction: CompactionConfig,
}

/// Source configuration (fully resolved).
#[derive(Debug, Clone)]
pub enum SourceConfig {
    Iceberg {
        catalog: CatalogConfig,
        namespace: String,
        table: String,
    },
    Parquet {
        base_path: String,
        bucket: String,
        time_window: Option<TimeWindowConfig>,
    },
}

/// Sink configuration (fully resolved).
#[derive(Debug, Clone)]
pub enum SinkConfig {
    /// Commit to Iceberg catalog.
    Iceberg {
        catalog: CatalogConfig,
        namespace: String,
        table: String,
    },
    /// Write Parquet files (no catalog commit).
    Parquet { output_path: String },
}

impl AppConfig {
    /// Build an OpenDAL operator for S3.
    pub fn build_operator(&self) -> Result<Operator> {
        self.s3
            .build_operator()
            .context("Failed to create S3 operator")
    }
}

/// Compaction configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct CompactionConfig {
    #[serde(default = "default_target_file_size_mb")]
    pub target_file_size_mb: u64,
    #[serde(default = "default_min_files_per_partition")]
    pub min_files_per_partition: usize,
}

fn default_target_file_size_mb() -> u64 {
    256
}

fn default_min_files_per_partition() -> usize {
    2
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            target_file_size_mb: default_target_file_size_mb(),
            min_files_per_partition: default_min_files_per_partition(),
        }
    }
}

// ============================================================================
// ConfigFile - JSON deserialization layer
// ============================================================================

/// Top-level config file structure (JSON input).
#[derive(Debug, Deserialize)]
pub struct ConfigFile {
    pub s3: S3ConfigFile,
    pub source: SourceConfigFile,
    #[serde(default)]
    pub sink: Option<SinkConfigFile>,
    #[serde(default)]
    pub compaction: Option<CompactionConfig>,
}

/// S3 configuration from JSON.
#[derive(Debug, Deserialize)]
pub struct S3ConfigFile {
    pub endpoint: String,
    #[serde(default)]
    pub access_key: Option<String>,
    #[serde(default)]
    pub secret_key: Option<String>,
    pub bucket: String,
    pub region: String,
    #[serde(default = "default_true")]
    pub use_default_credentials: bool,
}

fn default_true() -> bool {
    true
}

impl From<S3ConfigFile> for S3Config {
    fn from(cfg: S3ConfigFile) -> Self {
        S3Config {
            endpoint: cfg.endpoint,
            access_key: cfg.access_key.unwrap_or_default(),
            secret_key: cfg.secret_key.unwrap_or_default(),
            bucket: cfg.bucket,
            region: cfg.region,
            use_default_credentials: cfg.use_default_credentials,
        }
    }
}

/// Source configuration from JSON.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SourceConfigFile {
    Iceberg {
        catalog: CatalogConfigFile,
        namespace: String,
        table: String,
    },
    Parquet {
        base_path: String,
        #[serde(default)]
        time_window: Option<TimeWindowConfigFile>,
    },
}

/// Sink configuration from JSON.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SinkConfigFile {
    Iceberg {
        #[serde(default)]
        catalog: Option<CatalogConfigFile>,
        #[serde(default)]
        namespace: Option<String>,
        #[serde(default)]
        table: Option<String>,
    },
    Parquet {
        #[serde(default)]
        output_path: Option<String>,
    },
}

/// Catalog configuration from JSON.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CatalogConfigFile {
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

impl From<CatalogConfigFile> for CatalogConfig {
    fn from(cfg: CatalogConfigFile) -> Self {
        match cfg {
            CatalogConfigFile::Rest { uri, warehouse } => CatalogConfig::Rest {
                uri,
                warehouse,
                props: HashMap::new(),
            },
            CatalogConfigFile::Glue {
                warehouse,
                catalog_id,
                region,
            } => CatalogConfig::Glue {
                warehouse,
                catalog_id,
                region,
                props: HashMap::new(),
            },
        }
    }
}

// ============================================================================
// Time Window Config (for Parquet source)
// ============================================================================

/// Time window configuration from JSON.
#[derive(Debug, Clone, Deserialize)]
pub struct TimeWindowConfigFile {
    pub partition_spec: PartitionTimeSpecFile,
    pub lookback: LookbackWindowFile,
    #[serde(default = "default_skip_recent")]
    pub skip_recent: u32,
}

fn default_skip_recent() -> u32 {
    1
}

/// Partition time specification from JSON.
#[derive(Debug, Clone, Deserialize)]
pub struct PartitionTimeSpecFile {
    pub fields: Vec<PartitionTimeFieldFile>,
    #[serde(default = "default_timezone")]
    pub timezone: String,
    #[serde(default)]
    pub granularity: PartitionGranularityFile,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

/// A single partition time field from JSON.
#[derive(Debug, Clone, Deserialize)]
pub struct PartitionTimeFieldFile {
    pub name: String,
    pub format: String,
    pub component: TimeComponentFile,
}

/// Time component type.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeComponentFile {
    Date,
    Year,
    Month,
    Day,
    Hour,
    Minute,
}

impl From<TimeComponentFile> for TimeComponent {
    fn from(tc: TimeComponentFile) -> Self {
        match tc {
            TimeComponentFile::Date => TimeComponent::Date,
            TimeComponentFile::Year => TimeComponent::Year,
            TimeComponentFile::Month => TimeComponent::Month,
            TimeComponentFile::Day => TimeComponent::Day,
            TimeComponentFile::Hour => TimeComponent::Hour,
            TimeComponentFile::Minute => TimeComponent::Minute,
        }
    }
}

/// Partition granularity.
#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PartitionGranularityFile {
    Day,
    #[default]
    Hour,
    Minute,
}

impl From<PartitionGranularityFile> for PartitionGranularity {
    fn from(pg: PartitionGranularityFile) -> Self {
        match pg {
            PartitionGranularityFile::Day => PartitionGranularity::Day,
            PartitionGranularityFile::Hour => PartitionGranularity::Hour,
            PartitionGranularityFile::Minute => PartitionGranularity::Minute,
        }
    }
}

/// Lookback window configuration from JSON.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum LookbackWindowFile {
    Partitions {
        count: u32,
    },
    Duration {
        #[serde(default)]
        hours: u32,
        #[serde(default)]
        minutes: u32,
    },
}

impl From<LookbackWindowFile> for LookbackWindow {
    fn from(lw: LookbackWindowFile) -> Self {
        match lw {
            LookbackWindowFile::Partitions { count } => LookbackWindow::Partitions { count },
            LookbackWindowFile::Duration { hours, minutes } => {
                LookbackWindow::Duration { hours, minutes }
            }
        }
    }
}

impl TimeWindowConfigFile {
    fn into_time_window_config(self) -> TimeWindowConfig {
        TimeWindowConfig {
            partition_spec: PartitionTimeSpec {
                fields: self
                    .partition_spec
                    .fields
                    .into_iter()
                    .map(|f| PartitionTimeField {
                        name: f.name,
                        format: f.format,
                        component: f.component.into(),
                    })
                    .collect(),
                timezone: self.partition_spec.timezone,
                granularity: self.partition_spec.granularity.into(),
            },
            lookback: self.lookback.into(),
            skip_recent: self.skip_recent,
        }
    }
}

// ============================================================================
// ConfigFile -> AppConfig conversion
// ============================================================================

impl ConfigFile {
    /// Validate and convert to AppConfig.
    pub fn into_app_config(self) -> Result<AppConfig> {
        let s3: S3Config = self.s3.into();
        let bucket = s3.bucket.clone();

        // Convert source
        let source = match self.source {
            SourceConfigFile::Iceberg {
                catalog,
                namespace,
                table,
            } => SourceConfig::Iceberg {
                catalog: catalog.into(),
                namespace,
                table,
            },
            SourceConfigFile::Parquet {
                base_path,
                time_window,
            } => SourceConfig::Parquet {
                base_path,
                bucket: bucket.clone(),
                time_window: time_window.map(|tw| tw.into_time_window_config()),
            },
        };

        // Resolve sink (defaults to source if not specified)
        let sink = resolve_sink(self.sink, &source)?;

        let compaction = self.compaction.unwrap_or_default();

        Ok(AppConfig {
            s3,
            source,
            sink,
            compaction,
        })
    }
}

/// Resolve sink configuration, inheriting from source if needed.
fn resolve_sink(sink: Option<SinkConfigFile>, source: &SourceConfig) -> Result<SinkConfig> {
    match sink {
        // No sink specified - derive from source
        None => match source {
            SourceConfig::Iceberg {
                catalog,
                namespace,
                table,
            } => Ok(SinkConfig::Iceberg {
                catalog: catalog.clone(),
                namespace: namespace.clone(),
                table: table.clone(),
            }),
            SourceConfig::Parquet { base_path, .. } => Ok(SinkConfig::Parquet {
                output_path: base_path.clone(),
            }),
        },

        // Iceberg sink - may inherit from source
        Some(SinkConfigFile::Iceberg {
            catalog,
            namespace,
            table,
        }) => {
            // Try to inherit from Iceberg source if fields are missing
            let (default_catalog, default_namespace, default_table) = match source {
                SourceConfig::Iceberg {
                    catalog,
                    namespace,
                    table,
                } => (Some(catalog), Some(namespace), Some(table)),
                _ => (None, None, None),
            };

            let resolved_catalog = match catalog {
                Some(c) => c.into(),
                None => default_catalog
                    .cloned()
                    .context("Iceberg sink requires catalog (source is not Iceberg)")?,
            };

            let resolved_namespace = namespace
                .or_else(|| default_namespace.cloned())
                .context("Iceberg sink requires namespace")?;

            let resolved_table = table
                .or_else(|| default_table.cloned())
                .context("Iceberg sink requires table")?;

            Ok(SinkConfig::Iceberg {
                catalog: resolved_catalog,
                namespace: resolved_namespace,
                table: resolved_table,
            })
        }

        // Parquet sink
        Some(SinkConfigFile::Parquet { output_path }) => {
            let path = output_path.unwrap_or_else(|| {
                // Default to source base_path for parquet
                match source {
                    SourceConfig::Parquet { base_path, .. } => base_path.clone(),
                    _ => "output/".to_string(),
                }
            });
            Ok(SinkConfig::Parquet { output_path: path })
        }
    }
}
