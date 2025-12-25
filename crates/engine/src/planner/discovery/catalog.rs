//! Catalog abstraction for loading Iceberg table metadata from REST or Glue catalogs.
//!
//! This module provides the authoritative source of truth for table metadata,
//! avoiding the pitfall of reading directly from object storage where orphaned
//! metadata files from failed commits could be incorrectly selected.

use std::collections::HashMap;

use iceberg::table::Table;
use iceberg::{Catalog, TableIdent, NamespaceIdent};
use tracing::info;

/// Configuration for connecting to an Iceberg catalog
#[derive(Debug, Clone)]
pub enum CatalogConfig {
    /// REST catalog (Tabular, Polaris, Nessie, etc.)
    Rest {
        /// REST catalog URI (e.g., "http://localhost:8181")
        uri: String,
        /// Optional warehouse location
        warehouse: Option<String>,
        /// Additional properties (credentials, etc.)
        props: HashMap<String, String>,
    },
    /// AWS Glue catalog
    Glue {
        /// Warehouse location (required, e.g., "s3://bucket/warehouse")
        warehouse: String,
        /// Optional Glue catalog ID (for cross-account access)
        catalog_id: Option<String>,
        /// AWS region
        region: Option<String>,
        /// Additional properties (credentials, etc.)
        props: HashMap<String, String>,
    },
}

/// Result of loading a table from catalog - contains info needed for discovery
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// Table location (e.g., "s3://bucket/warehouse/db/table")
    pub location: String,
    /// Current snapshot ID
    pub snapshot_id: i64,
    /// Path to the manifest list file for current snapshot
    pub manifest_list_path: String,
}

/// Load table info from a catalog (REST or Glue)
pub async fn load_table_info(
    config: &CatalogConfig,
    namespace: &str,
    table_name: &str,
) -> anyhow::Result<TableInfo> {
    let table = load_table(config, namespace, table_name).await?;
    extract_table_info(&table)
}

/// Load a table from the catalog
async fn load_table(
    config: &CatalogConfig,
    namespace: &str,
    table_name: &str,
) -> anyhow::Result<Table> {
    let table_ident = TableIdent::new(
        NamespaceIdent::new(namespace.to_string()),
        table_name.to_string(),
    );

    match config {
        CatalogConfig::Rest { uri, warehouse, props } => {
            load_from_rest(uri, warehouse.as_deref(), props, &table_ident).await
        }
        CatalogConfig::Glue { warehouse, catalog_id, region, props } => {
            load_from_glue(warehouse, catalog_id.as_deref(), region.as_deref(), props, &table_ident).await
        }
    }
}

/// Load table from REST catalog
async fn load_from_rest(
    uri: &str,
    warehouse: Option<&str>,
    props: &HashMap<String, String>,
    table_ident: &TableIdent,
) -> anyhow::Result<Table> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_rest::RestCatalogBuilder;

    info!(uri, ?warehouse, %table_ident, "loading table from REST catalog");

    let mut catalog_props = props.clone();
    catalog_props.insert("uri".to_string(), uri.to_string());
    if let Some(wh) = warehouse {
        catalog_props.insert("warehouse".to_string(), wh.to_string());
    }

    let catalog = RestCatalogBuilder::default()
        .load("rest", catalog_props)
        .await?;

    let table = catalog.load_table(table_ident).await?;

    info!(
        location = %table.metadata().location(),
        "loaded table from REST catalog"
    );

    Ok(table)
}

/// Load table from Glue catalog
async fn load_from_glue(
    warehouse: &str,
    catalog_id: Option<&str>,
    region: Option<&str>,
    props: &HashMap<String, String>,
    table_ident: &TableIdent,
) -> anyhow::Result<Table> {
    use iceberg::CatalogBuilder;
    use iceberg_catalog_glue::GlueCatalogBuilder;

    info!(warehouse, ?catalog_id, ?region, %table_ident, "loading table from Glue catalog");

    let mut catalog_props = props.clone();
    catalog_props.insert("warehouse".to_string(), warehouse.to_string());
    if let Some(id) = catalog_id {
        catalog_props.insert("catalog_id".to_string(), id.to_string());
    }
    if let Some(r) = region {
        catalog_props.insert("region_name".to_string(), r.to_string());
    }

    let catalog = GlueCatalogBuilder::default()
        .load("glue", catalog_props)
        .await?;

    let table = catalog.load_table(table_ident).await?;

    info!(
        location = %table.metadata().location(),
        "loaded table from Glue catalog"
    );

    Ok(table)
}

/// Extract the info we need from the loaded table
fn extract_table_info(table: &Table) -> anyhow::Result<TableInfo> {
    let metadata = table.metadata();
    let location = metadata.location().to_string();

    // Get current snapshot
    let snapshot_id = metadata
        .current_snapshot_id()
        .ok_or_else(|| anyhow::anyhow!("table has no current snapshot"))?;

    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| anyhow::anyhow!("snapshot {} not found in metadata", snapshot_id))?;

    let manifest_list_path = snapshot.manifest_list().to_string();

    info!(
        %location,
        snapshot_id,
        %manifest_list_path,
        "extracted table info"
    );

    Ok(TableInfo {
        location,
        snapshot_id,
        manifest_list_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_config_rest() {
        let config = CatalogConfig::Rest {
            uri: "http://localhost:8181".to_string(),
            warehouse: Some("s3://bucket/warehouse".to_string()),
            props: HashMap::new(),
        };

        match config {
            CatalogConfig::Rest { uri, .. } => {
                assert_eq!(uri, "http://localhost:8181");
            }
            _ => panic!("expected REST config"),
        }
    }

    #[test]
    fn test_catalog_config_glue() {
        let config = CatalogConfig::Glue {
            warehouse: "s3://bucket/warehouse".to_string(),
            catalog_id: None,
            region: Some("us-east-1".to_string()),
            props: HashMap::new(),
        };

        match config {
            CatalogConfig::Glue { warehouse, region, .. } => {
                assert_eq!(warehouse, "s3://bucket/warehouse");
                assert_eq!(region, Some("us-east-1".to_string()));
            }
            _ => panic!("expected Glue config"),
        }
    }
}
