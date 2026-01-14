//! Table source and sink creation.

use std::sync::Arc;

use anyhow::{Context, Result};
use engine::planner::{IcebergTable, ParquetSource, TableSink, TableSource};
use opendal::Operator;
use tracing::info;

use crate::configs::{AppConfig, SinkConfig, SourceConfig};

/// Create source and sink, sharing the instance when they point to the same table.
pub async fn create_source_and_sink(
    config: &AppConfig,
    operator: Operator,
) -> Result<(Arc<dyn TableSource>, Arc<dyn TableSink>)> {
    // Check if source and sink are the same Iceberg table
    if let (
        SourceConfig::Iceberg {
            catalog: src_catalog,
            namespace: src_ns,
            table: src_table,
        },
        SinkConfig::Iceberg {
            catalog: sink_catalog,
            namespace: sink_ns,
            table: sink_table,
        },
    ) = (&config.source, &config.sink)
    {
        if src_catalog == sink_catalog && src_ns == sink_ns && src_table == sink_table {
            info!("Source and sink are the same Iceberg table, sharing instance");

            let iceberg_table = Arc::new(
                IcebergTable::new(src_catalog, src_ns, src_table)
                    .await
                    .context("Failed to load table from catalog")?,
            );

            info!(
                location = %TableSource::location(iceberg_table.as_ref()),
                "Loaded Iceberg table (shared source/sink)"
            );

            return Ok((iceberg_table.clone(), iceberg_table));
        }
    }

    // Different source and sink - create separately
    let source = create_source(config, operator.clone()).await?;
    let sink = create_sink(config, operator).await?;

    Ok((source, sink))
}

/// Create a TableSource based on configuration.
async fn create_source(config: &AppConfig, operator: Operator) -> Result<Arc<dyn TableSource>> {
    match &config.source {
        SourceConfig::Iceberg {
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
            Ok(Arc::new(iceberg_table))
        }
        SourceConfig::Parquet {
            base_path,
            bucket,
            time_window,
        } => {
            info!(base_path = %base_path, "Using Parquet source with S3 listing");

            let mut parquet_source =
                ParquetSource::new(operator, base_path.clone(), bucket.clone());

            if let Some(ref tw_config) = time_window {
                info!(
                    skip_recent = tw_config.skip_recent,
                    granularity = ?tw_config.partition_spec.granularity,
                    "Enabling time-windowed discovery"
                );
                parquet_source = parquet_source.with_time_window(tw_config.clone());
            }

            Ok(Arc::new(parquet_source))
        }
    }
}

/// Create a TableSink based on configuration.
async fn create_sink(config: &AppConfig, operator: Operator) -> Result<Arc<dyn TableSink>> {
    match &config.sink {
        SinkConfig::Iceberg {
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
            Ok(Arc::new(iceberg_table))
        }
        SinkConfig::Parquet { output_path } => {
            info!(output_path = %output_path, "Using Parquet sink");

            let parquet_sink =
                ParquetSource::new(operator, output_path.clone(), config.s3.bucket.clone());

            Ok(Arc::new(parquet_sink))
        }
    }
}
