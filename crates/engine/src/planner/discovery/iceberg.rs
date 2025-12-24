use super::{Discovery, PartitionFileMap};
use object_store::ObjectStore;
use std::sync::Arc;
use tracing::info;

/// Discovers files in an Iceberg table by reading metadata and manifests.
pub struct IcebergDiscovery {
    /// S3 path to table root (e.g., "s3://warehouse/db/test_table")
    table_path: String,
    /// Object store for reading files
    store: Arc<dyn ObjectStore>,
}

impl IcebergDiscovery {
    pub fn new(table_path: impl Into<String>, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            table_path: table_path.into(),
            store,
        }
    }

    /// Extracts bucket and path from s3://bucket/path URL.
    fn parse_s3_url(url: &str) -> (&str, &str) {
        let without_scheme = url.strip_prefix("s3://").unwrap_or(url);
        match without_scheme.find('/') {
            Some(idx) => (&without_scheme[..idx], &without_scheme[idx + 1..]),
            None => (without_scheme, ""),
        }
    }

    /// Lists metadata/ directory and returns path to latest metadata.json.
    /// Files are named like: 00001-uuid.metadata.json (version-uuid.metadata.json)
    async fn find_latest_metadata_path(&self) -> anyhow::Result<object_store::path::Path> {
        use futures::TryStreamExt;

        let (_, table_prefix) = Self::parse_s3_url(&self.table_path);
        let metadata_dir = object_store::path::Path::from(format!("{}/metadata/", table_prefix));

        tracing::debug!(%metadata_dir, "Listing metadata directory");

        // Collect all entries in metadata/
        let entries: Vec<_> = self
            .store
            .list(Some(&metadata_dir))
            .try_collect()
            .await?;

        // Find file with highest version number
        let mut best: Option<(i32, object_store::path::Path)> = None;
        for entry in entries {
            let name = entry.location.filename().unwrap_or_default();
            if !name.ends_with(".metadata.json") {
                continue;
            }
            // Parse "00001" from "00001-uuid.metadata.json"
            let version = name.split('-').next().and_then(|s| s.parse().ok()).unwrap_or(-1);
            if best.is_none() || version > best.as_ref().unwrap().0 {
                best = Some((version, entry.location));
            }
        }

        let (version, path) = best.ok_or_else(|| anyhow::anyhow!("no metadata.json found"))?;
        info!(version, %path, "found latest metadata.json");
        Ok(path)
    }

    /// Reads metadata.json from object store and parses it.
    async fn read_metadata(
        &self,
        path: &object_store::path::Path,
    ) -> anyhow::Result<TableMetadata> {
        tracing::debug!(%path, "reading metadata.json");

        let bytes = self.store.get(path).await?.bytes().await?;
        let metadata: TableMetadata = serde_json::from_slice(&bytes)?;

        info!(
            snapshot_id = metadata.current_snapshot_id,
            snapshots = metadata.snapshots.len(),
            "parsed metadata.json"
        );
        Ok(metadata)
    }

    /// Converts s3://bucket/path/to/file -> path/to/file
    fn s3_url_to_path(url: &str) -> object_store::path::Path {
        let (_, path) = Self::parse_s3_url(url);
        object_store::path::Path::from(path)
    }

    /// Reads manifest-list avro and returns list of manifest paths.
    async fn read_manifest_list(&self, manifest_list_url: &str) -> anyhow::Result<Vec<String>> {
        let path = Self::s3_url_to_path(manifest_list_url);
        tracing::debug!(%path, "reading manifest-list");

        let bytes = self.store.get(&path).await?.bytes().await?;
        let reader = apache_avro::Reader::new(&bytes[..])?;

        let mut manifest_paths = Vec::new();
        for value in reader {
            let record = value?;
            // Extract manifest_path field from avro record
            if let apache_avro::types::Value::Record(fields) = record {
                for (name, val) in fields {
                    if name == "manifest_path" {
                        if let apache_avro::types::Value::String(path) = val {
                            manifest_paths.push(path);
                        }
                    }
                }
            }
        }

        info!(count = manifest_paths.len(), "read manifest-list");
        Ok(manifest_paths)
    }

    /// Extracts a field from an Avro record by name.
    fn get_avro_field<'a>(
        fields: &'a [(String, apache_avro::types::Value)],
        name: &str,
    ) -> Option<&'a apache_avro::types::Value> {
        fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    /// Reads a manifest avro and extracts file info into the partition map.
    async fn read_manifest(
        &self,
        manifest_url: &str,
        map: &mut PartitionFileMap,
    ) -> anyhow::Result<()> {
        use super::FileInfo;
        use apache_avro::types::Value;

        let path = Self::s3_url_to_path(manifest_url);
        tracing::debug!(%path, "reading manifest");

        let bytes = self.store.get(&path).await?.bytes().await?;
        let reader = apache_avro::Reader::new(&bytes[..])?;

        let mut file_count = 0;
        for value in reader {
            let record = value?;

            // Each record is a manifest_entry with status and data_file fields
            // Status: 0=EXISTING, 1=ADDED, 2=DELETED - skip deleted entries
            if let Value::Record(entry_fields) = record {
                let status = match Self::get_avro_field(&entry_fields, "status") {
                    Some(Value::Int(n)) => *n,
                    _ => 0,
                };
                if status == 2 {
                    continue;
                }

                if let Some(Value::Record(data_file)) = Self::get_avro_field(&entry_fields, "data_file") {
                    // Extract file info from data_file record
                    let file_path = match Self::get_avro_field(data_file, "file_path") {
                        Some(Value::String(s)) => s.clone(),
                        _ => continue,
                    };

                    let size_bytes = match Self::get_avro_field(data_file, "file_size_in_bytes") {
                        Some(Value::Long(n)) => *n as u64,
                        _ => 0,
                    };

                    let record_count = match Self::get_avro_field(data_file, "record_count") {
                        Some(Value::Long(n)) => *n as u64,
                        _ => 0,
                    };

                    // Extract partition key (e.g., "id_bucket=0")
                    let partition_key = Self::extract_partition_key(data_file);

                    map.entry(partition_key)
                        .or_default()
                        .push(FileInfo { path: file_path, size_bytes, record_count });

                    file_count += 1;
                }
            }
        }

        tracing::debug!(file_count, "parsed manifest");
        Ok(())
    }

    /// Extracts partition key string from data_file record.
    /// Handles multi-column partitions: "year=2024/month=01/id_bucket=0"
    fn extract_partition_key(data_file: &[(String, apache_avro::types::Value)]) -> String {
        use apache_avro::types::Value;

        if let Some(Value::Record(partition_fields)) = Self::get_avro_field(data_file, "partition") {
            let mut parts = Vec::new();
            for (name, val) in partition_fields {
                let value_str = match val {
                    Value::Int(n) => Some(n.to_string()),
                    Value::Long(n) => Some(n.to_string()),
                    Value::String(s) => Some(s.clone()),
                    Value::Union(_, inner) => match inner.as_ref() {
                        Value::Int(n) => Some(n.to_string()),
                        Value::Long(n) => Some(n.to_string()),
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    },
                    _ => None,
                };
                if let Some(v) = value_str {
                    parts.push(format!("{}={}", name, v));
                }
            }
            if !parts.is_empty() {
                return parts.join("/");
            }
        }
        "unknown".to_string()
    }
}

#[async_trait::async_trait]
impl Discovery for IcebergDiscovery {
    async fn discover(&self) -> anyhow::Result<PartitionFileMap> {
        info!(table = %self.table_path, "starting iceberg discovery");

        // Step 1: Find and read metadata.json
        let metadata_path = self.find_latest_metadata_path().await?;
        let metadata = self.read_metadata(&metadata_path).await?;

        // Get manifest-list path from current snapshot
        let snapshot = metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == metadata.current_snapshot_id)
            .ok_or_else(|| anyhow::anyhow!("current snapshot not found"))?;

        info!(manifest_list = %snapshot.manifest_list, "found current snapshot");

        // Step 2: Read manifest-list to get manifest paths
        let manifest_paths = self.read_manifest_list(&snapshot.manifest_list).await?;

        // Step 3: Read each manifest and build partition map
        let mut map = PartitionFileMap::new();
        for manifest_url in &manifest_paths {
            self.read_manifest(manifest_url, &mut map).await?;
        }

        // Log summary
        let total_files: usize = map.values().map(|v| v.len()).sum();
        info!(
            partitions = map.len(),
            total_files,
            "discovery complete"
        );

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
