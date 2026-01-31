//! Table source and sink creation.

use std::sync::Arc;

use anyhow::{Context, Result};
use engine::planner::{IcebergTable, ParquetSource, TableSink, TableSource};
use opendal::Operator;
use tracing::info;

use crate::configs::{AppConfig, SourceConfig, TableLocation};

/// Create source and sink, sharing the instance when they point to the same table.
pub async fn create_source_and_sink(
    config: &AppConfig,
    operator: Operator,
) -> Result<(Arc<dyn TableSource>, Arc<dyn TableSink>)> {
    if let TableLocation::Iceberg {
        catalog,
        namespace,
        table,
    } = &config.source.location
    {
        if config.source.location == config.sink {
            info!("Source and sink are the same Iceberg table, sharing instance");

            let iceberg_table = Arc::new(
                IcebergTable::new(catalog, namespace, table)
                    .await
                    .context("Failed to load table from catalog")?,
            );

            info!(
                location = %TableSource::location(iceberg_table.as_ref()),
                "Loaded Iceberg table (shared source/sink)"
            );

            let source: Arc<dyn TableSource> = iceberg_table.clone();
            let sink: Arc<dyn TableSink> = iceberg_table;
            return Ok((source, sink));
        }
    }

    // Different source and sink (or Parquet) - create separately
    let source = create_source(config, operator.clone()).await?;
    let sink = create_sink(config, operator).await?;

    Ok((source, sink))
}

/// Create a TableSource based on configuration.
async fn create_source(config: &AppConfig, operator: Operator) -> Result<Arc<dyn TableSource>> {
    let SourceConfig {
        location,
        time_window,
    } = &config.source;

    match location {
        TableLocation::Iceberg {
            catalog,
            namespace,
            table,
        } => {
            info!("Loading source table from Iceberg catalog...");

            let iceberg_table = IcebergTable::new(catalog, namespace, table)
                .await
                .context("Failed to load table from catalog")?;

            info!(
                location = %TableSource::location(&iceberg_table),
                "Loaded source table from Iceberg catalog"
            );
            let source: Arc<dyn TableSource> = Arc::new(iceberg_table);
            Ok(source)
        }
        TableLocation::Parquet { path, bucket } => {
            info!(path = %path, "Using Parquet source with S3 listing");

            let mut parquet_source = ParquetSource::new(operator, path.clone(), bucket.clone());

            if let Some(ref tw_config) = time_window {
                info!(
                    skip_recent = tw_config.skip_recent,
                    granularity = ?tw_config.partition_spec.granularity,
                    "Enabling time-windowed discovery"
                );
                parquet_source = parquet_source.with_time_window(tw_config.clone());
            }

            let source: Arc<dyn TableSource> = Arc::new(parquet_source);
            Ok(source)
        }
    }
}

/// Create a TableSink based on configuration.
async fn create_sink(config: &AppConfig, operator: Operator) -> Result<Arc<dyn TableSink>> {
    match &config.sink {
        TableLocation::Iceberg {
            catalog,
            namespace,
            table,
        } => {
            info!("Loading sink table from Iceberg catalog...");

            let iceberg_table = IcebergTable::new(catalog, namespace, table)
                .await
                .context("Failed to load sink table from catalog")?;

            info!(
                location = %TableSink::location(&iceberg_table),
                "Loaded sink table from Iceberg catalog"
            );
            let sink: Arc<dyn TableSink> = Arc::new(iceberg_table);
            Ok(sink)
        }
        TableLocation::Parquet { path, bucket } => {
            info!(path = %path, "Using Parquet sink");

            let parquet_sink = ParquetSource::new(operator, path.clone(), bucket.clone());

            let sink: Arc<dyn TableSink> = Arc::new(parquet_sink);
            Ok(sink)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::executor::S3Config;
    use engine::planner::discovery::TableFormat;
    use engine::planner::{
        LookbackWindow, PartitionGranularity, PartitionTimeField, PartitionTimeSpec, TimeComponent,
        TimeWindowConfig,
    };

    use crate::configs::{AppConfig, CompactionConfig, SourceConfig};

    /// Create a memory-based operator for testing (no S3 needed)
    fn create_memory_operator() -> Operator {
        let builder = opendal::services::Memory::default();
        opendal::Operator::new(builder).unwrap().finish()
    }

    /// Create a minimal S3Config for testing
    fn test_s3_config() -> S3Config {
        S3Config {
            endpoint: "http://localhost:9000".to_string(),
            access_key: "admin".to_string(),
            secret_key: "password".to_string(),
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            use_default_credentials: false,
        }
    }

    // ========================================================================
    // Parquet Source/Sink Creation Tests
    // ========================================================================

    #[tokio::test]
    async fn test_create_parquet_source_basic() {
        let config = AppConfig {
            s3: test_s3_config(),
            source: SourceConfig {
                location: TableLocation::Parquet {
                    path: "data/events/".to_string(),
                    bucket: "test-bucket".to_string(),
                },
                time_window: None,
            },
            sink: TableLocation::Parquet {
                path: "data/events/".to_string(),
                bucket: "test-bucket".to_string(),
            },
            compaction: CompactionConfig::default(),
        };

        let operator = create_memory_operator();
        let (source, sink) = create_source_and_sink(&config, operator).await.unwrap();

        // Verify source is created and accessible
        assert_eq!(source.format(), TableFormat::Parquet);
        assert_eq!(source.location(), "data/events/");

        // Verify sink is created and accessible
        assert_eq!(sink.format(), TableFormat::Parquet);
        assert_eq!(sink.location(), "data/events/");
    }

    #[tokio::test]
    async fn test_create_parquet_source_with_time_window() {
        let time_window = TimeWindowConfig {
            partition_spec: PartitionTimeSpec {
                fields: vec![
                    PartitionTimeField {
                        name: "dt".to_string(),
                        format: "%Y-%m-%d".to_string(),
                        component: TimeComponent::Date,
                    },
                    PartitionTimeField {
                        name: "hour".to_string(),
                        format: "%H".to_string(),
                        component: TimeComponent::Hour,
                    },
                ],
                timezone: "UTC".to_string(),
                granularity: PartitionGranularity::Hour,
            },
            lookback: LookbackWindow::Duration {
                hours: 5,
                minutes: 0,
            },
            skip_recent: 1,
        };

        let config = AppConfig {
            s3: test_s3_config(),
            source: SourceConfig {
                location: TableLocation::Parquet {
                    path: "data/events/".to_string(),
                    bucket: "test-bucket".to_string(),
                },
                time_window: Some(time_window),
            },
            sink: TableLocation::Parquet {
                path: "data/compacted/".to_string(),
                bucket: "test-bucket".to_string(),
            },
            compaction: CompactionConfig::default(),
        };

        let operator = create_memory_operator();
        let (source, sink) = create_source_and_sink(&config, operator).await.unwrap();

        // Source should be Parquet with time window configured
        assert_eq!(source.format(), TableFormat::Parquet);
        assert_eq!(source.location(), "data/events/");

        // Sink should be different path
        assert_eq!(sink.format(), TableFormat::Parquet);
        assert_eq!(sink.location(), "data/compacted/");
    }

    #[tokio::test]
    async fn test_create_parquet_source_different_sink() {
        let config = AppConfig {
            s3: test_s3_config(),
            source: SourceConfig {
                location: TableLocation::Parquet {
                    path: "data/raw/".to_string(),
                    bucket: "test-bucket".to_string(),
                },
                time_window: None,
            },
            sink: TableLocation::Parquet {
                path: "data/compacted/".to_string(),
                bucket: "test-bucket".to_string(),
            },
            compaction: CompactionConfig::default(),
        };

        let operator = create_memory_operator();
        let (source, sink) = create_source_and_sink(&config, operator).await.unwrap();

        // Source and sink should have different locations
        assert_eq!(source.location(), "data/raw/");
        assert_eq!(sink.location(), "data/compacted/");
        assert_ne!(source.location(), sink.location());
    }

    #[tokio::test]
    async fn test_parquet_source_sink_same_location_not_shared() {
        // Unlike Iceberg, Parquet sources don't share instances even if same location
        // (because ParquetSource is cheap to create)
        let config = AppConfig {
            s3: test_s3_config(),
            source: SourceConfig {
                location: TableLocation::Parquet {
                    path: "data/events/".to_string(),
                    bucket: "test-bucket".to_string(),
                },
                time_window: None,
            },
            sink: TableLocation::Parquet {
                path: "data/events/".to_string(),
                bucket: "test-bucket".to_string(),
            },
            compaction: CompactionConfig::default(),
        };

        let operator = create_memory_operator();
        let (source, sink) = create_source_and_sink(&config, operator).await.unwrap();

        // Both should work independently
        assert_eq!(source.format(), TableFormat::Parquet);
        assert_eq!(sink.format(), TableFormat::Parquet);
        assert_eq!(source.location(), sink.location());

        // They are separate instances (not shared like Iceberg)
        // This is the expected behavior - Parquet doesn't need instance sharing
    }

    // ========================================================================
    // Trait Method Accessibility Tests
    // ========================================================================

    #[tokio::test]
    async fn test_source_trait_methods_accessible() {
        let config = AppConfig {
            s3: test_s3_config(),
            source: SourceConfig {
                location: TableLocation::Parquet {
                    path: "data/".to_string(),
                    bucket: "test-bucket".to_string(),
                },
                time_window: None,
            },
            sink: TableLocation::Parquet {
                path: "data/".to_string(),
                bucket: "test-bucket".to_string(),
            },
            compaction: CompactionConfig::default(),
        };

        let operator = create_memory_operator();
        let (source, _sink) = create_source_and_sink(&config, operator).await.unwrap();

        // Test that TableSource trait methods are accessible
        let format = source.format();
        assert_eq!(format, TableFormat::Parquet);

        let location = source.location();
        assert_eq!(location, "data/");

        // discover() should work (returns empty for memory backend with no data)
        let partition_map = source.discover().await.unwrap();
        assert!(partition_map.is_empty()); // No files in memory backend
    }

    #[tokio::test]
    async fn test_sink_trait_methods_accessible() {
        let config = AppConfig {
            s3: test_s3_config(),
            source: SourceConfig {
                location: TableLocation::Parquet {
                    path: "data/".to_string(),
                    bucket: "test-bucket".to_string(),
                },
                time_window: None,
            },
            sink: TableLocation::Parquet {
                path: "data/".to_string(),
                bucket: "test-bucket".to_string(),
            },
            compaction: CompactionConfig::default(),
        };

        let operator = create_memory_operator();
        let (_source, sink) = create_source_and_sink(&config, operator).await.unwrap();

        // Test that TableSink trait methods are accessible
        let format = sink.format();
        assert_eq!(format, TableFormat::Parquet);

        let location = sink.location();
        assert_eq!(location, "data/");

        // commit() should work (Parquet sink just logs, no actual catalog commit)
        let result = sink.commit(vec![], vec![]).await.unwrap();
        assert_eq!(result.files_added, 0);
        assert_eq!(result.files_removed, 0);
        assert!(result.snapshot_id.is_none()); // Parquet has no snapshots
    }

    // ========================================================================
    // Arc/Trait Object Tests
    // ========================================================================

    #[tokio::test]
    async fn test_source_sink_are_arc_dyn_trait() {
        let config = AppConfig {
            s3: test_s3_config(),
            source: SourceConfig {
                location: TableLocation::Parquet {
                    path: "data/".to_string(),
                    bucket: "test-bucket".to_string(),
                },
                time_window: None,
            },
            sink: TableLocation::Parquet {
                path: "data/".to_string(),
                bucket: "test-bucket".to_string(),
            },
            compaction: CompactionConfig::default(),
        };

        let operator = create_memory_operator();
        let (source, sink) = create_source_and_sink(&config, operator).await.unwrap();

        // Test that we can clone the Arc (important for sharing)
        let source_clone = source.clone();
        let sink_clone = sink.clone();

        // Both references should work
        assert_eq!(source.location(), source_clone.location());
        assert_eq!(sink.location(), sink_clone.location());

        // Test Arc strong count (should be 2 after clone)
        assert_eq!(Arc::strong_count(&source), 2);
        assert_eq!(Arc::strong_count(&sink), 2);
    }
}
