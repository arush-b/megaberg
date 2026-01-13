//! Iceberg table implementation (source + sink).
//!
//! Uses iceberg-rust to load table metadata and schema from catalogs,
//! and commit compaction results back to the catalog.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::spec::{Literal, ManifestContentType, ManifestStatus, PrimitiveLiteral};
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use tracing::{debug, info, warn};

use super::super::discovery::{FileInfo, PartitionFileMap, TableFormat};
use super::sink::{CommitResult, OutputFileInfo, TableSink};
use super::source::TableSource;

/// Default number of manifests to read in parallel.
const DEFAULT_MANIFEST_PARALLELISM: usize = 8;

/// Configuration for connecting to an Iceberg catalog.
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogConfig {
    /// REST catalog (Tabular, Polaris, Nessie, etc.)
    Rest {
        uri: String,
        warehouse: Option<String>,
        props: HashMap<String, String>,
    },
    /// AWS Glue catalog
    Glue {
        warehouse: String,
        catalog_id: Option<String>,
        region: Option<String>,
        props: HashMap<String, String>,
    },
}

/// Iceberg table - implements both TableSource and TableSink.
///
/// Uses iceberg-rust to:
/// - Load table metadata from REST or Glue catalogs (source)
/// - Commit compaction results back to the catalog (sink)
pub struct IcebergTable {
    /// The iceberg-rust table (contains metadata, schema, snapshots)
    table: Table,
    /// Number of manifests to read in parallel
    manifest_parallelism: usize,
}

impl IcebergTable {
    /// Create a new IcebergTable by loading from the catalog.
    pub async fn new(config: &CatalogConfig, namespace: &str, table_name: &str) -> Result<Self> {
        let table = Self::load_table(config, namespace, table_name).await?;
        Ok(Self {
            table,
            manifest_parallelism: DEFAULT_MANIFEST_PARALLELISM,
        })
    }

    /// Create from an existing iceberg-rust Table.
    pub fn from_table(table: Table) -> Self {
        Self {
            table,
            manifest_parallelism: DEFAULT_MANIFEST_PARALLELISM,
        }
    }

    /// Set the number of manifests to read in parallel.
    pub fn with_manifest_parallelism(mut self, parallelism: usize) -> Self {
        self.manifest_parallelism = parallelism.max(1);
        self
    }

    /// Get the underlying iceberg-rust Table.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Load table from catalog.
    async fn load_table(
        config: &CatalogConfig,
        namespace: &str,
        table_name: &str,
    ) -> Result<Table> {
        use iceberg::CatalogBuilder;

        let table_ident = TableIdent::new(
            NamespaceIdent::new(namespace.to_string()),
            table_name.to_string(),
        );

        let (catalog_type, catalog_props) = match config {
            CatalogConfig::Rest {
                uri,
                warehouse,
                props,
            } => {
                info!(uri, ?warehouse, %table_ident, "loading table from REST catalog");
                let mut catalog_props = props.clone();
                catalog_props.insert("uri".to_string(), uri.clone());
                if let Some(wh) = warehouse {
                    catalog_props.insert("warehouse".to_string(), wh.clone());
                }
                ("rest", catalog_props)
            }
            CatalogConfig::Glue {
                warehouse,
                catalog_id,
                region,
                props,
            } => {
                info!(warehouse, ?catalog_id, ?region, %table_ident, "loading table from Glue catalog");
                let mut catalog_props = props.clone();
                catalog_props.insert("warehouse".to_string(), warehouse.clone());
                if let Some(id) = catalog_id {
                    catalog_props.insert("catalog_id".to_string(), id.clone());
                }
                if let Some(r) = region {
                    catalog_props.insert("region_name".to_string(), r.clone());
                }
                ("glue", catalog_props)
            }
        };

        let table = match catalog_type {
            "rest" => {
                let catalog = RestCatalogBuilder::default()
                    .load("rest", catalog_props)
                    .await
                    .context("Failed to create REST catalog")?;
                catalog
                    .load_table(&table_ident)
                    .await
                    .context("Failed to load table from REST catalog")?
            }
            "glue" => {
                let catalog = GlueCatalogBuilder::default()
                    .load("glue", catalog_props)
                    .await
                    .context("Failed to create Glue catalog")?;
                catalog
                    .load_table(&table_ident)
                    .await
                    .context("Failed to load table from Glue catalog")?
            }
            _ => unreachable!(),
        };

        info!(location = %table.metadata().location(), "loaded table from catalog");
        Ok(table)
    }

    /// Discover files using iceberg-rust's native manifest reading.
    async fn discover_files(&self) -> Result<Vec<(String, FileInfo)>> {
        let metadata = self.table.metadata();
        let file_io = self.table.file_io();

        // Get current snapshot
        let snapshot_id = metadata
            .current_snapshot_id()
            .context("Table has no current snapshot")?;
        let snapshot = metadata
            .snapshot_by_id(snapshot_id)
            .context("Current snapshot not found")?;

        // Load manifest list
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .context("Failed to load manifest list")?;

        // Filter to DATA manifests only (skip DELETE manifests)
        let data_manifests: Vec<_> = manifest_list
            .entries()
            .iter()
            .filter(|m| m.content == ManifestContentType::Data)
            .cloned()
            .collect();

        let skipped = manifest_list.entries().len() - data_manifests.len();
        if skipped > 0 {
            debug!(skipped, "skipped delete manifests");
        }

        info!(
            data_manifests = data_manifests.len(),
            skipped_delete_manifests = skipped,
            "loaded manifest list"
        );

        // Get partition spec for formatting partition keys
        let partition_spec = metadata.default_partition_spec();
        let partition_fields: Vec<_> = partition_spec
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();

        // Read manifests in parallel with bounded concurrency
        let file_io = file_io.clone();
        let parallelism = self.manifest_parallelism;

        let results: Vec<Result<Vec<(String, FileInfo)>>> = stream::iter(data_manifests)
            .map(|manifest_file| {
                let fio = file_io.clone();
                let fields = partition_fields.clone();
                async move { Self::read_manifest_entries(&fio, &manifest_file, &fields).await }
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;

        // Collect all entries
        let mut all_entries = Vec::new();
        for result in results {
            let entries = result?;
            all_entries.extend(entries);
        }

        Ok(all_entries)
    }

    /// Read entries from a single manifest file.
    async fn read_manifest_entries(
        file_io: &FileIO,
        manifest_file: &iceberg::spec::ManifestFile,
        partition_field_names: &[String],
    ) -> Result<Vec<(String, FileInfo)>> {
        let manifest_path = manifest_file.manifest_path.as_str();
        debug!(%manifest_path, "reading manifest");

        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .with_context(|| format!("Failed to load manifest: {}", manifest_path))?;

        let mut entries = Vec::new();

        for entry in manifest.entries() {
            // Skip deleted entries
            if entry.status() == ManifestStatus::Deleted {
                continue;
            }

            let data_file = entry.data_file();
            let partition = data_file.partition();

            // Format partition key using field names and values
            let partition_key = Self::format_partition_key(partition, partition_field_names);

            entries.push((
                partition_key,
                FileInfo {
                    path: data_file.file_path().to_string(),
                    size_bytes: data_file.file_size_in_bytes(),
                    record_count: data_file.record_count(),
                },
            ));
        }

        debug!(file_count = entries.len(), %manifest_path, "parsed manifest");
        Ok(entries)
    }

    /// Format partition struct as a key string like "year=2024/month=01".
    fn format_partition_key(
        partition: &iceberg::spec::Struct,
        partition_field_names: &[String],
    ) -> String {
        if partition_field_names.is_empty() {
            return String::new();
        }

        let mut parts = Vec::new();

        for (i, field_name) in partition_field_names.iter().enumerate() {
            let value = partition.fields().get(i).and_then(|v| v.as_ref());

            let value_str = match value {
                Some(literal) => Self::literal_to_string(literal),
                None => "__HIVE_DEFAULT_PARTITION__".to_string(),
            };

            parts.push(format!("{}={}", field_name, value_str));
        }

        parts.join("/")
    }

    /// Convert an iceberg Literal to a string representation for partition keys.
    fn literal_to_string(literal: &Literal) -> String {
        match literal {
            Literal::Primitive(prim) => Self::primitive_to_string(prim),
            // For complex types, use debug format as fallback
            _ => format!("{:?}", literal),
        }
    }

    /// Convert a PrimitiveLiteral to string.
    ///
    /// Note: In iceberg-rust 0.7, time-related types (Date, Time, Timestamp)
    /// are stored as Int or Long values. The type context is in the partition
    /// spec, but for partition keys we just use the raw numeric value which
    /// is what Hive-style partition paths expect.
    fn primitive_to_string(prim: &PrimitiveLiteral) -> String {
        match prim {
            PrimitiveLiteral::Boolean(b) => b.to_string(),
            PrimitiveLiteral::Int(i) => i.to_string(),
            PrimitiveLiteral::Long(l) => l.to_string(),
            PrimitiveLiteral::Float(f) => f.0.to_string(),
            PrimitiveLiteral::Double(d) => d.0.to_string(),
            PrimitiveLiteral::String(s) => s.clone(),
            PrimitiveLiteral::Binary(b) => hex::encode(b),
            PrimitiveLiteral::Int128(i) => i.to_string(),
            PrimitiveLiteral::UInt128(u) => u.to_string(),
            PrimitiveLiteral::AboveMax => "ABOVE_MAX".to_string(),
            PrimitiveLiteral::BelowMin => "BELOW_MIN".to_string(),
        }
    }
}

#[async_trait]
impl TableSource for IcebergTable {
    fn format(&self) -> TableFormat {
        TableFormat::Iceberg
    }

    fn location(&self) -> &str {
        self.table.metadata().location()
    }

    async fn schema(&self) -> Result<SchemaRef> {
        let iceberg_schema = self.table.metadata().current_schema();
        let arrow_schema = schema_to_arrow_schema(iceberg_schema)
            .context("Failed to convert Iceberg schema to Arrow")?;
        Ok(Arc::new(arrow_schema))
    }

    async fn discover(&self) -> Result<PartitionFileMap> {
        let all_entries = self.discover_files().await?;

        // Group by partition key
        let mut map = PartitionFileMap::new();
        for (partition_key, file) in all_entries {
            map.entry(partition_key).or_default().push(file);
        }

        let total_files: usize = map.values().map(|v| v.len()).sum();
        info!(partitions = map.len(), total_files, "discovery complete");

        Ok(map)
    }
}

#[async_trait]
impl TableSink for IcebergTable {
    fn format(&self) -> TableFormat {
        TableFormat::Iceberg
    }

    async fn commit(
        &self,
        added_files: Vec<OutputFileInfo>,
        removed_files: Vec<String>,
    ) -> Result<CommitResult> {
        // TODO: Implement actual Iceberg commit using iceberg-rust
        // This will involve:
        // 1. Creating a new manifest with added_files
        // 2. Marking removed_files as deleted
        // 3. Creating a new snapshot
        // 4. Committing to the catalog
        warn!(
            added = added_files.len(),
            removed = removed_files.len(),
            "Iceberg commit not yet implemented - files written but not committed to catalog"
        );

        Ok(CommitResult {
            snapshot_id: None,
            files_added: added_files.len(),
            files_removed: removed_files.len(),
        })
    }
}
