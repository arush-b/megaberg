//! Iceberg table source implementation.
//!
//! Discovers files by reading Iceberg metadata and manifests.

use std::sync::Arc;

use anyhow::{Context, Result};
use apache_avro::types::Value;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use opendal::Operator;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::reader::{FileReader, SerializedFileReader};
use tracing::{debug, info, warn};

use super::source::TableSource;
use super::super::discovery::{FileInfo, PartitionFileMap, TableFormat, TableInfo};

/// Iceberg table source.
///
/// This provides a TableSource implementation for Iceberg tables,
/// reading manifest lists and manifests directly from storage.
pub struct IcebergSource {
    /// Table info from catalog (contains authoritative manifest_list path)
    table_info: TableInfo,
    /// OpenDAL operator for reading manifest files
    operator: Operator,
}

impl IcebergSource {
    /// Create from existing TableInfo (loaded via catalog).
    pub fn new(table_info: TableInfo, operator: Operator) -> Self {
        Self {
            table_info,
            operator,
        }
    }

    /// Get the underlying TableInfo.
    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }

    /// Extracts the path from s3://bucket/path URLs for OpenDAL.
    fn s3_url_path(url: &str) -> &str {
        let without_scheme = url.strip_prefix("s3://").unwrap_or(url);
        match without_scheme.find('/') {
            Some(idx) => &without_scheme[idx + 1..],
            None => "",
        }
    }

    /// Lists metadata/ directory and returns path to latest metadata.json.
    /// Files are named like: 00001-uuid.metadata.json (version-uuid.metadata.json)
    ///
    /// NOTE: This is a legacy fallback. Prefer using catalog-provided TableInfo.
    async fn find_latest_metadata_path(&self) -> Result<String> {
        let table_prefix = Self::s3_url_path(&self.table_info.location);
        let metadata_dir = format!("{}/metadata/", table_prefix);

        debug!(%metadata_dir, "Listing metadata directory");

        // Collect all entries in metadata/
        let entries = self
            .operator
            .list(&metadata_dir)
            .await
            .with_context(|| format!("Failed to list metadata directory: {}", metadata_dir))?;

        // Find file with highest version number
        let mut best: Option<(i32, String)> = None;
        for entry in entries {
            let name = entry.name();
            if !name.ends_with(".metadata.json") {
                continue;
            }
            // Parse "00001" from "00001-uuid.metadata.json"
            let version = name
                .split('-')
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(-1);
            if best.is_none() || version > best.as_ref().unwrap().0 {
                best = Some((version, entry.path().to_string()));
            }
        }

        let (version, path) = best.context("no metadata.json found")?;
        info!(version, %path, "found latest metadata.json");
        Ok(path)
    }

    /// Reads metadata.json from storage and parses it.
    async fn read_metadata(&self, path: &str) -> Result<TableMetadata> {
        let path = Self::s3_url_path(path);
        debug!(%path, "reading metadata.json");

        let bytes = self
            .operator
            .read(path)
            .await
            .with_context(|| format!("Failed to read metadata.json: {}", path))?
            .to_vec();
        let metadata: TableMetadata =
            serde_json::from_slice(&bytes).context("Failed to parse metadata.json")?;

        info!(
            snapshot_id = metadata.current_snapshot_id,
            snapshots = metadata.snapshots.len(),
            "parsed metadata.json"
        );
        Ok(metadata)
    }

    /// Returns the authoritative manifest-list URL (catalog or legacy fallback).
    async fn manifest_list_url(&self) -> Result<String> {
        if self.table_info.snapshot_id >= 0 && !self.table_info.manifest_list_path.is_empty() {
            info!(
                table = %self.table_info.location,
                snapshot_id = self.table_info.snapshot_id,
                manifest_list = %self.table_info.manifest_list_path,
                "using catalog-provided manifest list"
            );
            return Ok(self.table_info.manifest_list_path.clone());
        }

        // Legacy fallback: scan S3 for metadata.json (NOT RECOMMENDED)
        warn!(
            table = %self.table_info.location,
            "falling back to S3 metadata scan - this may select orphaned metadata!"
        );

        let metadata_path = self
            .find_latest_metadata_path()
            .await
            .context("Failed to find latest metadata.json")?;
        let metadata = self
            .read_metadata(&metadata_path)
            .await
            .context("Failed to read metadata.json")?;

        let snapshot = metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == metadata.current_snapshot_id)
            .context("current snapshot not found")?;

        info!(manifest_list = %snapshot.manifest_list, "found current snapshot (via S3 scan)");
        Ok(snapshot.manifest_list.clone())
    }

    /// Reads manifest-list avro and returns list of manifest paths.
    async fn read_manifest_list(&self, manifest_list_url: &str) -> Result<Vec<String>> {
        let path = Self::s3_url_path(manifest_list_url);
        debug!(%path, "reading manifest-list");

        let bytes = self
            .operator
            .read(path)
            .await
            .with_context(|| format!("Failed to read manifest-list: {}", path))?
            .to_vec();
        let reader = apache_avro::Reader::new(&bytes[..])
            .context("Failed to parse manifest-list avro")?;

        let mut manifest_paths = Vec::new();
        for value in reader {
            let record = value.context("Failed to read manifest-list record")?;
            let Value::Record(fields) = record else {
                continue;
            };

            if let Some(path) = avro_field(&fields, "manifest_path").and_then(avro_string) {
                manifest_paths.push(path.to_string());
            }
        }

        info!(count = manifest_paths.len(), "read manifest-list");
        Ok(manifest_paths)
    }

    /// Reads a manifest avro and extracts file info.
    async fn read_manifest(&self, manifest_url: &str) -> Result<Vec<(String, FileInfo)>> {
        let path = Self::s3_url_path(manifest_url);
        debug!(%path, "reading manifest");

        let bytes = self
            .operator
            .read(path)
            .await
            .with_context(|| format!("Failed to read manifest: {}", path))?
            .to_vec();
        let reader = apache_avro::Reader::new(&bytes[..])
            .context("Failed to parse manifest avro")?;

        let mut entries = Vec::new();
        let mut file_count = 0;
        for value in reader {
            let record = value.context("Failed to read manifest record")?;
            let Value::Record(entry_fields) = record else {
                continue;
            };

            // Status: 0=EXISTING, 1=ADDED, 2=DELETED - skip deleted entries
            let status = avro_field(&entry_fields, "status")
                .and_then(avro_int)
                .unwrap_or(0);
            if status == 2 {
                continue;
            }

            let Some(data_file) = avro_field(&entry_fields, "data_file").and_then(avro_record)
            else {
                continue;
            };

            let Some(file_path) = avro_field(data_file, "file_path").and_then(avro_string)
            else {
                continue;
            };

            let size_bytes = avro_field(data_file, "file_size_in_bytes")
                .and_then(avro_long)
                .unwrap_or(0) as u64;

            let record_count = avro_field(data_file, "record_count")
                .and_then(avro_long)
                .unwrap_or(0) as u64;

            let partition_key = Self::extract_partition_key(data_file);

            entries.push((
                partition_key,
                FileInfo {
                    path: file_path.to_string(),
                    size_bytes,
                    record_count,
                },
            ));
            file_count += 1;
        }

        debug!(file_count, "parsed manifest");
        Ok(entries)
    }

    /// Reads only the first data file entry from a manifest.
    async fn read_first_manifest_file(&self, manifest_url: &str) -> Result<Option<FileInfo>> {
        let path = Self::s3_url_path(manifest_url);
        debug!(%path, "reading manifest (first file)");

        let bytes = self
            .operator
            .read(path)
            .await
            .with_context(|| format!("Failed to read manifest: {}", path))?
            .to_vec();
        let reader = apache_avro::Reader::new(&bytes[..])
            .context("Failed to parse manifest avro")?;

        for value in reader {
            let record = value.context("Failed to read manifest record")?;
            let Value::Record(entry_fields) = record else {
                continue;
            };

            let status = avro_field(&entry_fields, "status")
                .and_then(avro_int)
                .unwrap_or(0);
            if status == 2 {
                continue;
            }

            let Some(data_file) = avro_field(&entry_fields, "data_file").and_then(avro_record)
            else {
                continue;
            };

            let Some(file_path) = avro_field(data_file, "file_path").and_then(avro_string)
            else {
                continue;
            };

            let size_bytes = avro_field(data_file, "file_size_in_bytes")
                .and_then(avro_long)
                .unwrap_or(0) as u64;

            let record_count = avro_field(data_file, "record_count")
                .and_then(avro_long)
                .unwrap_or(0) as u64;

            return Ok(Some(FileInfo {
                path: file_path.to_string(),
                size_bytes,
                record_count,
            }));
        }

        Ok(None)
    }

    /// Extracts partition key string from data_file record.
    /// Handles multi-column partitions: "year=2024/month=01/id_bucket=0"
    fn extract_partition_key(data_file: &[(String, Value)]) -> String {
        let Some(partition_fields) = avro_field(data_file, "partition").and_then(avro_record)
        else {
            return String::new();
        };

        let mut parts = Vec::new();
        for (name, val) in partition_fields {
            let Some(value_str) = avro_value_to_string(val) else {
                continue;
            };
            parts.push(format!("{}={}", name, value_str));
        }

        if parts.is_empty() {
            String::new()
        } else {
            parts.join("/")
        }
    }
}

#[async_trait]
impl TableSource for IcebergSource {
    fn format(&self) -> TableFormat {
        TableFormat::Iceberg
    }

    fn location(&self) -> &str {
        &self.table_info.location
    }

    async fn schema(&self) -> Result<SchemaRef> {
        let manifest_list_url = self.manifest_list_url().await?;
        let manifest_paths = self.read_manifest_list(&manifest_list_url).await?;
        let first_manifest = manifest_paths
            .first()
            .context("manifest list is empty")?;

        let first_file = self
            .read_first_manifest_file(first_manifest)
            .await?
            .context("no data files found in first manifest")?;

        let path = Self::s3_url_path(&first_file.path);
        let bytes = self
            .operator
            .read(path)
            .await
            .with_context(|| format!("Failed to read parquet file: {}", path))?
            .to_vec();

        let reader = SerializedFileReader::new(Bytes::from(bytes))
            .context("Failed to create parquet reader")?;
        let schema = parquet_to_arrow_schema(
            reader.metadata().file_metadata().schema_descr(),
            None,
        )
        .context("Failed to convert parquet schema to arrow")?;

        Ok(Arc::new(schema))
    }

    async fn discover(&self) -> Result<PartitionFileMap> {
        let manifest_list_url = self.manifest_list_url().await?;
        let manifest_paths = self.read_manifest_list(&manifest_list_url).await?;

        let mut map = PartitionFileMap::new();
        for manifest_url in &manifest_paths {
            for (partition_key, file) in self.read_manifest(manifest_url).await? {
                map.entry(partition_key).or_default().push(file);
            }
        }

        let total_files: usize = map.values().map(|v| v.len()).sum();
        info!(partitions = map.len(), total_files, "discovery complete");

        Ok(map)
    }
}

// ---- Iceberg metadata.json structures ----

/// Partial representation of Iceberg's metadata.json.
/// Only includes fields needed for discovery.
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct TableMetadata {
    current_snapshot_id: i64,
    snapshots: Vec<Snapshot>,
}

/// Snapshot entry from metadata.json
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Snapshot {
    snapshot_id: i64,
    manifest_list: String,
}

// ---- Avro helpers ----

fn avro_field<'a>(fields: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    fields.iter().find_map(|(n, v)| (n == name).then_some(v))
}

fn avro_unwrap_union<'a>(value: &'a Value) -> &'a Value {
    match value {
        Value::Union(_, inner) => inner.as_ref(),
        _ => value,
    }
}

fn avro_record<'a>(value: &'a Value) -> Option<&'a [(String, Value)]> {
    match avro_unwrap_union(value) {
        Value::Record(fields) => Some(fields.as_slice()),
        _ => None,
    }
}

fn avro_string(value: &Value) -> Option<&str> {
    match avro_unwrap_union(value) {
        Value::String(s) => Some(s.as_str()),
        _ => None,
    }
}

fn avro_int(value: &Value) -> Option<i32> {
    match avro_unwrap_union(value) {
        Value::Int(n) => Some(*n),
        _ => None,
    }
}

fn avro_long(value: &Value) -> Option<i64> {
    match avro_unwrap_union(value) {
        Value::Long(n) => Some(*n),
        _ => None,
    }
}

fn avro_value_to_string(value: &Value) -> Option<String> {
    avro_string(value)
        .map(|s| s.to_string())
        .or_else(|| avro_int(value).map(|n| n.to_string()))
        .or_else(|| avro_long(value).map(|n| n.to_string()))
}
