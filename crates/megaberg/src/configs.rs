//! Configuration types for megaberg CLI.
//!
//! Two layers:
//! - `ConfigFile`: JSON-deserializable structure (what the user writes)
//! - `AppConfig`: Validated, ready-to-use config (what the code uses)
//!
//! Key design: `TableLocation` is shared between source and sink,
//! while `SourceConfig` adds discovery-specific options like `time_window`.

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
    pub sink: TableLocation,
    pub compaction: CompactionConfig,
}

/// Where a table is located (shared by source and sink).
#[derive(Debug, Clone, PartialEq)]
pub enum TableLocation {
    /// Iceberg table in a catalog.
    Iceberg {
        catalog: CatalogConfig,
        namespace: String,
        table: String,
    },
    /// Parquet files in S3.
    Parquet { path: String, bucket: String },
}

/// Source configuration = location + discovery options.
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// Where to read from.
    pub location: TableLocation,
    /// Optional time-windowed discovery (for incremental compaction).
    pub time_window: Option<TimeWindowConfig>,
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
    /// Optional sink - defaults to source location if not specified.
    #[serde(default)]
    pub sink: Option<TableLocationFile>,
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

/// Source configuration from JSON (location + discovery options).
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

/// Table location from JSON (for explicit sink specification).
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TableLocationFile {
    Iceberg {
        catalog: CatalogConfigFile,
        namespace: String,
        table: String,
    },
    Parquet {
        path: String,
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

impl CatalogConfigFile {
    /// Convert to CatalogConfig, injecting S3 props for FileIO.
    fn into_catalog_config(self, s3: &S3Config) -> CatalogConfig {
        match self {
            CatalogConfigFile::Rest { uri, warehouse } => {
                let mut props = HashMap::new();
                // S3 settings for iceberg-rust FileIO
                props.insert("s3.endpoint".to_string(), s3.endpoint.clone());
                props.insert("s3.region".to_string(), s3.region.clone());
                if !s3.access_key.is_empty() {
                    props.insert("s3.access-key-id".to_string(), s3.access_key.clone());
                }
                if !s3.secret_key.is_empty() {
                    props.insert("s3.secret-access-key".to_string(), s3.secret_key.clone());
                }
                // MinIO-specific settings
                props.insert("s3.allow-http".to_string(), "true".to_string());
                props.insert("s3.path-style-access".to_string(), "true".to_string());
                if !s3.use_default_credentials {
                    props.insert("s3.disable-config-load".to_string(), "true".to_string());
                    props.insert("s3.disable-ec2-metadata".to_string(), "true".to_string());
                }

                CatalogConfig::Rest {
                    uri,
                    warehouse,
                    props,
                }
            }
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
// Time Window Config (for partition discovery)
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

impl SourceConfigFile {
    /// Convert to SourceConfig, injecting S3 props into catalog config.
    fn into_source_config(self, s3: &S3Config) -> SourceConfig {
        match self {
            SourceConfigFile::Iceberg {
                catalog,
                namespace,
                table,
            } => SourceConfig {
                location: TableLocation::Iceberg {
                    catalog: catalog.into_catalog_config(s3),
                    namespace,
                    table,
                },
                time_window: None,
            },
            SourceConfigFile::Parquet {
                base_path,
                time_window,
            } => SourceConfig {
                location: TableLocation::Parquet {
                    path: base_path,
                    bucket: s3.bucket.clone(),
                },
                time_window: time_window.map(|tw| tw.into_time_window_config()),
            },
        }
    }
}

impl TableLocationFile {
    /// Convert to TableLocation, injecting S3 props into catalog config.
    fn into_table_location(self, s3: &S3Config) -> TableLocation {
        match self {
            TableLocationFile::Iceberg {
                catalog,
                namespace,
                table,
            } => TableLocation::Iceberg {
                catalog: catalog.into_catalog_config(s3),
                namespace,
                table,
            },
            TableLocationFile::Parquet { path } => TableLocation::Parquet {
                path,
                bucket: s3.bucket.clone(),
            },
        }
    }
}

impl ConfigFile {
    /// Validate and convert to AppConfig.
    pub fn into_app_config(self) -> Result<AppConfig> {
        let s3: S3Config = self.s3.into();

        // Convert source (location + discovery options)
        let source = self.source.into_source_config(&s3);

        // Sink defaults to source location (the elegant one-liner!)
        let sink = self
            .sink
            .map(|s| s.into_table_location(&s3))
            .unwrap_or_else(|| source.location.clone());

        let compaction = self.compaction.unwrap_or_default();

        Ok(AppConfig {
            s3,
            source,
            sink,
            compaction,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a minimal S3 config for tests
    fn test_s3_config() -> S3ConfigFile {
        S3ConfigFile {
            endpoint: "http://localhost:9000".to_string(),
            access_key: Some("admin".to_string()),
            secret_key: Some("password".to_string()),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            use_default_credentials: false,
        }
    }

    // ========================================================================
    // Parquet source tests
    // ========================================================================

    #[test]
    fn test_parquet_source_sink_defaults_to_source() {
        // When no sink specified, sink should default to source location
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "parquet",
                "base_path": "data/events/"
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        // Source should be Parquet
        assert!(matches!(app_config.source.location, TableLocation::Parquet { .. }));

        // Sink should equal source location
        assert_eq!(app_config.source.location, app_config.sink);

        // Verify path and bucket
        if let TableLocation::Parquet { path, bucket } = &app_config.sink {
            assert_eq!(path, "data/events/");
            assert_eq!(bucket, "test-bucket");
        } else {
            panic!("Expected Parquet sink");
        }
    }

    #[test]
    fn test_parquet_source_with_explicit_parquet_sink() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "parquet",
                "base_path": "data/raw/"
            },
            "sink": {
                "type": "parquet",
                "path": "data/compacted/"
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        // Source and sink should be different
        assert_ne!(app_config.source.location, app_config.sink);

        // Verify source
        if let TableLocation::Parquet { path, .. } = &app_config.source.location {
            assert_eq!(path, "data/raw/");
        } else {
            panic!("Expected Parquet source");
        }

        // Verify sink
        if let TableLocation::Parquet { path, bucket } = &app_config.sink {
            assert_eq!(path, "data/compacted/");
            assert_eq!(bucket, "test-bucket"); // Should inherit bucket from S3 config
        } else {
            panic!("Expected Parquet sink");
        }
    }

    #[test]
    fn test_parquet_source_with_iceberg_sink() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "parquet",
                "base_path": "data/raw/"
            },
            "sink": {
                "type": "iceberg",
                "catalog": {
                    "type": "glue",
                    "warehouse": "s3://test-bucket/warehouse"
                },
                "namespace": "analytics",
                "table": "events"
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        // Source should be Parquet
        assert!(matches!(app_config.source.location, TableLocation::Parquet { .. }));

        // Sink should be Iceberg
        assert!(matches!(app_config.sink, TableLocation::Iceberg { .. }));

        // Verify Iceberg sink details
        if let TableLocation::Iceberg { catalog, namespace, table } = &app_config.sink {
            assert_eq!(namespace, "analytics");
            assert_eq!(table, "events");
            assert!(matches!(catalog, CatalogConfig::Glue { .. }));
        } else {
            panic!("Expected Iceberg sink");
        }
    }

    #[test]
    fn test_parquet_source_with_time_window() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "parquet",
                "base_path": "data/events/",
                "time_window": {
                    "partition_spec": {
                        "fields": [
                            {"name": "dt", "format": "%Y-%m-%d", "component": "date"},
                            {"name": "hour", "format": "%H", "component": "hour"}
                        ],
                        "granularity": "hour"
                    },
                    "lookback": {"type": "duration", "hours": 5},
                    "skip_recent": 1
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        // Verify time_window is set
        assert!(app_config.source.time_window.is_some());

        let tw = app_config.source.time_window.unwrap();
        assert_eq!(tw.skip_recent, 1);
        assert_eq!(tw.partition_spec.fields.len(), 2);
        assert_eq!(tw.partition_spec.fields[0].name, "dt");
        assert_eq!(tw.partition_spec.fields[1].name, "hour");
        assert!(matches!(tw.partition_spec.granularity, PartitionGranularity::Hour));
        assert!(matches!(tw.lookback, LookbackWindow::Duration { hours: 5, minutes: 0 }));
    }

    #[test]
    fn test_parquet_source_time_window_with_partitions_lookback() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "parquet",
                "base_path": "data/events/",
                "time_window": {
                    "partition_spec": {
                        "fields": [
                            {"name": "dt", "format": "%Y-%m-%d", "component": "date"}
                        ],
                        "granularity": "day"
                    },
                    "lookback": {"type": "partitions", "count": 7},
                    "skip_recent": 2
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        let tw = app_config.source.time_window.unwrap();
        assert_eq!(tw.skip_recent, 2);
        assert!(matches!(tw.partition_spec.granularity, PartitionGranularity::Day));
        assert!(matches!(tw.lookback, LookbackWindow::Partitions { count: 7 }));
    }

    // ========================================================================
    // Iceberg source tests
    // ========================================================================

    #[test]
    fn test_iceberg_source_sink_defaults_to_source() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "iceberg",
                "catalog": {
                    "type": "glue",
                    "warehouse": "s3://test-bucket/warehouse"
                },
                "namespace": "analytics",
                "table": "events"
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        // Source and sink should be the same
        assert_eq!(app_config.source.location, app_config.sink);

        // Verify both are Iceberg with same details
        if let TableLocation::Iceberg { namespace, table, .. } = &app_config.source.location {
            assert_eq!(namespace, "analytics");
            assert_eq!(table, "events");
        } else {
            panic!("Expected Iceberg source");
        }
    }

    #[test]
    fn test_iceberg_source_with_different_iceberg_sink() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "iceberg",
                "catalog": {
                    "type": "glue",
                    "warehouse": "s3://test-bucket/warehouse"
                },
                "namespace": "raw",
                "table": "events_raw"
            },
            "sink": {
                "type": "iceberg",
                "catalog": {
                    "type": "glue",
                    "warehouse": "s3://test-bucket/warehouse"
                },
                "namespace": "analytics",
                "table": "events_compacted"
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        // Source and sink should be different
        assert_ne!(app_config.source.location, app_config.sink);

        // Verify source
        if let TableLocation::Iceberg { namespace, table, .. } = &app_config.source.location {
            assert_eq!(namespace, "raw");
            assert_eq!(table, "events_raw");
        } else {
            panic!("Expected Iceberg source");
        }

        // Verify sink
        if let TableLocation::Iceberg { namespace, table, .. } = &app_config.sink {
            assert_eq!(namespace, "analytics");
            assert_eq!(table, "events_compacted");
        } else {
            panic!("Expected Iceberg sink");
        }
    }

    #[test]
    fn test_iceberg_source_with_parquet_sink() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "iceberg",
                "catalog": {
                    "type": "rest",
                    "uri": "http://localhost:8181"
                },
                "namespace": "default",
                "table": "events"
            },
            "sink": {
                "type": "parquet",
                "path": "data/exported/"
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        // Source should be Iceberg
        assert!(matches!(app_config.source.location, TableLocation::Iceberg { .. }));

        // Sink should be Parquet
        assert!(matches!(app_config.sink, TableLocation::Parquet { .. }));

        if let TableLocation::Parquet { path, bucket } = &app_config.sink {
            assert_eq!(path, "data/exported/");
            assert_eq!(bucket, "test-bucket");
        }
    }

    // ========================================================================
    // Catalog type tests
    // ========================================================================

    #[test]
    fn test_rest_catalog_config() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "iceberg",
                "catalog": {
                    "type": "rest",
                    "uri": "http://localhost:8181",
                    "warehouse": "s3://lakehouse/"
                },
                "namespace": "default",
                "table": "events"
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        if let TableLocation::Iceberg { catalog, .. } = &app_config.source.location {
            if let CatalogConfig::Rest { uri, warehouse, props } = catalog {
                assert_eq!(uri, "http://localhost:8181");
                assert_eq!(warehouse.as_deref(), Some("s3://lakehouse/"));
                assert!(props.is_empty());
            } else {
                panic!("Expected REST catalog");
            }
        } else {
            panic!("Expected Iceberg source");
        }
    }

    #[test]
    fn test_glue_catalog_config() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "iceberg",
                "catalog": {
                    "type": "glue",
                    "warehouse": "s3://lakehouse/",
                    "catalog_id": "123456789012",
                    "region": "us-west-2"
                },
                "namespace": "default",
                "table": "events"
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        if let TableLocation::Iceberg { catalog, .. } = &app_config.source.location {
            if let CatalogConfig::Glue { warehouse, catalog_id, region, props } = catalog {
                assert_eq!(warehouse, "s3://lakehouse/");
                assert_eq!(catalog_id.as_deref(), Some("123456789012"));
                assert_eq!(region.as_deref(), Some("us-west-2"));
                assert!(props.is_empty());
            } else {
                panic!("Expected Glue catalog");
            }
        } else {
            panic!("Expected Iceberg source");
        }
    }

    // ========================================================================
    // Compaction config tests
    // ========================================================================

    #[test]
    fn test_compaction_defaults() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "parquet",
                "base_path": "data/"
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        // Verify defaults
        assert_eq!(app_config.compaction.target_file_size_mb, 256);
        assert_eq!(app_config.compaction.min_files_per_partition, 2);
    }

    #[test]
    fn test_compaction_custom_values() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "parquet",
                "base_path": "data/"
            },
            "compaction": {
                "target_file_size_mb": 512,
                "min_files_per_partition": 5
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        assert_eq!(app_config.compaction.target_file_size_mb, 512);
        assert_eq!(app_config.compaction.min_files_per_partition, 5);
    }

    // ========================================================================
    // S3 config tests
    // ========================================================================

    #[test]
    fn test_s3_config_with_credentials() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "access_key": "mykey",
                "secret_key": "mysecret",
                "bucket": "test-bucket",
                "region": "us-east-1",
                "use_default_credentials": false
            },
            "source": {
                "type": "parquet",
                "base_path": "data/"
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        assert_eq!(app_config.s3.endpoint, "http://localhost:9000");
        assert_eq!(app_config.s3.access_key, "mykey");
        assert_eq!(app_config.s3.secret_key, "mysecret");
        assert_eq!(app_config.s3.bucket, "test-bucket");
        assert_eq!(app_config.s3.region, "us-east-1");
        assert!(!app_config.s3.use_default_credentials);
    }

    #[test]
    fn test_s3_config_defaults() {
        // Test that use_default_credentials defaults to true
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "parquet",
                "base_path": "data/"
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        // Credentials should default to empty strings
        assert_eq!(app_config.s3.access_key, "");
        assert_eq!(app_config.s3.secret_key, "");
        // use_default_credentials should default to true
        assert!(app_config.s3.use_default_credentials);
    }

    // ========================================================================
    // Time window defaults tests
    // ========================================================================

    #[test]
    fn test_time_window_defaults() {
        let config_json = r#"{
            "s3": {
                "endpoint": "http://localhost:9000",
                "bucket": "test-bucket",
                "region": "us-east-1"
            },
            "source": {
                "type": "parquet",
                "base_path": "data/",
                "time_window": {
                    "partition_spec": {
                        "fields": [
                            {"name": "dt", "format": "%Y-%m-%d", "component": "date"}
                        ]
                    },
                    "lookback": {"type": "duration", "hours": 24}
                }
            }
        }"#;

        let config: ConfigFile = serde_json::from_str(config_json).unwrap();
        let app_config = config.into_app_config().unwrap();

        let tw = app_config.source.time_window.unwrap();

        // skip_recent should default to 1
        assert_eq!(tw.skip_recent, 1);

        // timezone should default to UTC
        assert_eq!(tw.partition_spec.timezone, "UTC");

        // granularity should default to Hour
        assert!(matches!(tw.partition_spec.granularity, PartitionGranularity::Hour));
    }

    // ========================================================================
    // TableLocation equality tests (for instance sharing logic)
    // ========================================================================

    #[test]
    fn test_table_location_equality_iceberg_same() {
        let loc1 = TableLocation::Iceberg {
            catalog: CatalogConfig::Glue {
                warehouse: "s3://bucket/warehouse".to_string(),
                catalog_id: None,
                region: Some("us-east-1".to_string()),
                props: HashMap::new(),
            },
            namespace: "default".to_string(),
            table: "events".to_string(),
        };

        let loc2 = TableLocation::Iceberg {
            catalog: CatalogConfig::Glue {
                warehouse: "s3://bucket/warehouse".to_string(),
                catalog_id: None,
                region: Some("us-east-1".to_string()),
                props: HashMap::new(),
            },
            namespace: "default".to_string(),
            table: "events".to_string(),
        };

        assert_eq!(loc1, loc2);
    }

    #[test]
    fn test_table_location_equality_iceberg_different_table() {
        let loc1 = TableLocation::Iceberg {
            catalog: CatalogConfig::Glue {
                warehouse: "s3://bucket/warehouse".to_string(),
                catalog_id: None,
                region: None,
                props: HashMap::new(),
            },
            namespace: "default".to_string(),
            table: "events".to_string(),
        };

        let loc2 = TableLocation::Iceberg {
            catalog: CatalogConfig::Glue {
                warehouse: "s3://bucket/warehouse".to_string(),
                catalog_id: None,
                region: None,
                props: HashMap::new(),
            },
            namespace: "default".to_string(),
            table: "events_compacted".to_string(),
        };

        assert_ne!(loc1, loc2);
    }

    #[test]
    fn test_table_location_equality_parquet_same() {
        let loc1 = TableLocation::Parquet {
            path: "data/events/".to_string(),
            bucket: "test-bucket".to_string(),
        };

        let loc2 = TableLocation::Parquet {
            path: "data/events/".to_string(),
            bucket: "test-bucket".to_string(),
        };

        assert_eq!(loc1, loc2);
    }

    #[test]
    fn test_table_location_equality_parquet_different() {
        let loc1 = TableLocation::Parquet {
            path: "data/raw/".to_string(),
            bucket: "test-bucket".to_string(),
        };

        let loc2 = TableLocation::Parquet {
            path: "data/compacted/".to_string(),
            bucket: "test-bucket".to_string(),
        };

        assert_ne!(loc1, loc2);
    }

    #[test]
    fn test_table_location_equality_mixed_types() {
        let parquet = TableLocation::Parquet {
            path: "data/events/".to_string(),
            bucket: "test-bucket".to_string(),
        };

        let iceberg = TableLocation::Iceberg {
            catalog: CatalogConfig::Rest {
                uri: "http://localhost:8181".to_string(),
                warehouse: None,
                props: HashMap::new(),
            },
            namespace: "default".to_string(),
            table: "events".to_string(),
        };

        assert_ne!(parquet, iceberg);
    }
}
