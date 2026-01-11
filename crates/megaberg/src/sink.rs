//! IcebergSink implementation for committing compaction results.
//!
//! This wraps the existing commit functions to implement the TableSink trait.

use std::collections::HashMap;

use anyhow::{Context, Result};
use async_trait::async_trait;
use engine::planner::table::{CommitResult, OutputFileInfo, TableSink};
use engine::planner::TableFormat;
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use tracing::info;

use crate::commit::{commit_compaction, commit_compaction_glue, OutputFile, TaskResult};

/// Configuration for Iceberg catalog connection.
#[derive(Clone)]
pub enum IcebergCatalogConfig {
    Rest {
        uri: String,
        warehouse: Option<String>,
        s3_endpoint: Option<String>,
        s3_region: Option<String>,
        s3_access_key: Option<String>,
        s3_secret_key: Option<String>,
    },
    Glue {
        warehouse: String,
        region: Option<String>,
    },
}

/// Iceberg table sink - commits compaction results to Iceberg catalog.
pub struct IcebergSink {
    table: Table,
    s3_bucket: String,
    namespace: String,
    table_name: String,
    catalog_config: IcebergCatalogConfig,
}

impl IcebergSink {
    /// Create a new IcebergSink with pre-loaded table.
    pub fn new(
        table: Table,
        s3_bucket: String,
        namespace: String,
        table_name: String,
        catalog_config: IcebergCatalogConfig,
    ) -> Self {
        Self {
            table,
            s3_bucket,
            namespace,
            table_name,
            catalog_config,
        }
    }

    /// Load table from REST catalog and create sink.
    pub async fn from_rest(
        uri: &str,
        warehouse: Option<&str>,
        namespace: &str,
        table_name: &str,
        s3_bucket: &str,
        s3_config: Option<S3ConfigForCatalog>,
    ) -> Result<Self> {
        let mut props = HashMap::new();
        props.insert("uri".to_string(), uri.to_string());
        if let Some(wh) = warehouse {
            props.insert("warehouse".to_string(), wh.to_string());
        }

        let (s3_endpoint, s3_region, s3_access_key, s3_secret_key) = if let Some(s3) = &s3_config {
            props.insert("s3.endpoint".to_string(), s3.endpoint.clone());
            props.insert("s3.region".to_string(), s3.region.clone());
            if let Some(ref ak) = s3.access_key {
                props.insert("s3.access-key-id".to_string(), ak.clone());
            }
            if let Some(ref sk) = s3.secret_key {
                props.insert("s3.secret-access-key".to_string(), sk.clone());
            }
            props.insert("s3.allow-http".to_string(), "true".to_string());
            props.insert("s3.path-style-access".to_string(), "true".to_string());
            (
                Some(s3.endpoint.clone()),
                Some(s3.region.clone()),
                s3.access_key.clone(),
                s3.secret_key.clone(),
            )
        } else {
            (None, None, None, None)
        };

        let catalog = RestCatalogBuilder::default()
            .load("rest", props)
            .await
            .context("Failed to build REST catalog")?;

        let table_ident = TableIdent::new(
            NamespaceIdent::new(namespace.to_string()),
            table_name.to_string(),
        );

        let table = catalog
            .load_table(&table_ident)
            .await
            .context("Failed to load table")?;

        Ok(Self {
            table,
            s3_bucket: s3_bucket.to_string(),
            namespace: namespace.to_string(),
            table_name: table_name.to_string(),
            catalog_config: IcebergCatalogConfig::Rest {
                uri: uri.to_string(),
                warehouse: warehouse.map(|s| s.to_string()),
                s3_endpoint,
                s3_region,
                s3_access_key,
                s3_secret_key,
            },
        })
    }

    /// Load table from Glue catalog and create sink.
    pub async fn from_glue(
        warehouse: &str,
        region: Option<&str>,
        namespace: &str,
        table_name: &str,
        s3_bucket: &str,
    ) -> Result<Self> {
        let mut props = HashMap::new();
        props.insert("warehouse".to_string(), warehouse.to_string());
        if let Some(r) = region {
            props.insert("region".to_string(), r.to_string());
        }

        let catalog = GlueCatalogBuilder::default()
            .load("glue", props)
            .await
            .context("Failed to build Glue catalog")?;

        let table_ident = TableIdent::new(
            NamespaceIdent::new(namespace.to_string()),
            table_name.to_string(),
        );

        let table = catalog
            .load_table(&table_ident)
            .await
            .context("Failed to load table")?;

        Ok(Self {
            table,
            s3_bucket: s3_bucket.to_string(),
            namespace: namespace.to_string(),
            table_name: table_name.to_string(),
            catalog_config: IcebergCatalogConfig::Glue {
                warehouse: warehouse.to_string(),
                region: region.map(|s| s.to_string()),
            },
        })
    }
}

/// S3 configuration for catalog connection.
#[derive(Clone)]
pub struct S3ConfigForCatalog {
    pub endpoint: String,
    pub region: String,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
}

#[async_trait]
impl TableSink for IcebergSink {
    fn format(&self) -> TableFormat {
        TableFormat::Iceberg
    }

    async fn commit(
        &self,
        added_files: Vec<OutputFileInfo>,
        removed_files: Vec<String>,
    ) -> Result<CommitResult> {
        // Convert to existing TaskResult format used by commit functions
        let task_result = TaskResult {
            input_files: removed_files.clone(),
            output_files: added_files
                .iter()
                .map(|f| OutputFile {
                    path: f.path.clone(),
                    size_bytes: f.size_bytes,
                    record_count: f.record_count,
                })
                .collect(),
        };

        match &self.catalog_config {
            IcebergCatalogConfig::Rest { uri, .. } => {
                commit_compaction(
                    &self.table,
                    vec![task_result],
                    &self.s3_bucket,
                    uri,
                    &self.namespace,
                    &self.table_name,
                )
                .await?;
            }
            IcebergCatalogConfig::Glue { warehouse, region } => {
                // Rebuild Glue catalog for commit
                let mut props = HashMap::new();
                props.insert("warehouse".to_string(), warehouse.clone());
                if let Some(r) = region {
                    props.insert("region".to_string(), r.clone());
                }

                let catalog = GlueCatalogBuilder::default()
                    .load("glue", props)
                    .await
                    .context("Failed to rebuild Glue catalog for commit")?;

                commit_compaction_glue(&catalog, &self.table, vec![task_result], &self.s3_bucket)
                    .await?;
            }
        }

        info!(
            added = added_files.len(),
            removed = removed_files.len(),
            "Committed compaction to Iceberg"
        );

        Ok(CommitResult {
            snapshot_id: None, // Could extract from commit if needed
            files_added: added_files.len(),
            files_removed: removed_files.len(),
        })
    }
}
