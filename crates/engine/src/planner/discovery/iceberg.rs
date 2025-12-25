use super::{Discovery, PartitionFileMap, TableInfo};
use opendal::Operator;
use tracing::info;

/// Discovers files in an Iceberg table by reading metadata and manifests.
///
/// This struct requires a `TableInfo` from the catalog (REST or Glue) which
/// provides the authoritative manifest_list path. This avoids the pitfall of
/// reading metadata.json directly from S3 where orphaned files from failed
/// commits could be incorrectly selected.
pub struct IcebergDiscovery {
    /// Table info from catalog (contains authoritative manifest_list path)
    table_info: TableInfo,
    /// OpenDAL operator for reading manifest files
    operator: Operator,
}

impl IcebergDiscovery {
    /// Create a new IcebergDiscovery from catalog-provided TableInfo.
    ///
    /// The TableInfo should be obtained via `load_table_info()` from the catalog
    /// module, which ensures we use the authoritative metadata from REST/Glue.
    pub fn new(table_info: TableInfo, operator: Operator) -> Self {
        Self {
            table_info,
            operator,
        }
    }

    /// Create from table path directly (DEPRECATED - use catalog instead).
    ///
    /// This method reads metadata.json directly from S3 which is NOT recommended
    /// as it may select orphaned metadata from failed commits.
    #[deprecated(note = "Use new() with TableInfo from catalog instead")]
    pub fn from_path(table_path: impl Into<String>, operator: Operator) -> Self {
        // Create a placeholder TableInfo - discover() will fall back to S3 scan
        Self {
            table_info: TableInfo {
                location: table_path.into(),
                snapshot_id: -1, // Sentinel: triggers legacy S3 scan
                manifest_list_path: String::new(),
            },
            operator,
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
    ///
    /// NOTE: This is a legacy fallback. Prefer using catalog-provided TableInfo.
    async fn find_latest_metadata_path(&self) -> anyhow::Result<String> {
        let (_, table_prefix) = Self::parse_s3_url(&self.table_info.location);
        let metadata_dir = format!("{}/metadata/", table_prefix);

        tracing::debug!(%metadata_dir, "Listing metadata directory");

        // Collect all entries in metadata/
        let entries = self.operator.list(&metadata_dir).await?;

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

        let (version, path) = best.ok_or_else(|| anyhow::anyhow!("no metadata.json found"))?;
        info!(version, %path, "found latest metadata.json");
        Ok(path)
    }

    /// Reads metadata.json from storage and parses it.
    async fn read_metadata(&self, path: &str) -> anyhow::Result<TableMetadata> {
        tracing::debug!(%path, "reading metadata.json");

        let bytes = self.operator.read(path).await?.to_vec();
        let metadata: TableMetadata = serde_json::from_slice(&bytes)?;

        info!(
            snapshot_id = metadata.current_snapshot_id,
            snapshots = metadata.snapshots.len(),
            "parsed metadata.json"
        );
        Ok(metadata)
    }

    /// Converts s3://bucket/path/to/file -> path/to/file
    fn s3_url_to_path(url: &str) -> String {
        let (_, path) = Self::parse_s3_url(url);
        path.to_string()
    }

    /// Reads manifest-list avro and returns list of manifest paths.
    async fn read_manifest_list(&self, manifest_list_url: &str) -> anyhow::Result<Vec<String>> {
        let path = Self::s3_url_to_path(manifest_list_url);
        tracing::debug!(%path, "reading manifest-list");

        let bytes = self.operator.read(&path).await?.to_vec();
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

        let bytes = self.operator.read(&path).await?.to_vec();
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

                if let Some(Value::Record(data_file)) =
                    Self::get_avro_field(&entry_fields, "data_file")
                {
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

                    map.entry(partition_key).or_default().push(FileInfo {
                        path: file_path,
                        size_bytes,
                        record_count,
                    });

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

        if let Some(Value::Record(partition_fields)) = Self::get_avro_field(data_file, "partition")
        {
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
        String::new() // Unpartitioned table - empty string
    }
}

#[async_trait::async_trait]
impl Discovery for IcebergDiscovery {
    async fn discover(&self) -> anyhow::Result<PartitionFileMap> {
        // Check if we have valid catalog-provided TableInfo
        let manifest_list_url = if self.table_info.snapshot_id >= 0
            && !self.table_info.manifest_list_path.is_empty()
        {
            // Use authoritative manifest_list from catalog
            info!(
                table = %self.table_info.location,
                snapshot_id = self.table_info.snapshot_id,
                manifest_list = %self.table_info.manifest_list_path,
                "using catalog-provided manifest list"
            );
            self.table_info.manifest_list_path.clone()
        } else {
            // Legacy fallback: scan S3 for metadata.json (NOT RECOMMENDED)
            tracing::warn!(
                table = %self.table_info.location,
                "falling back to S3 metadata scan - this may select orphaned metadata!"
            );
            let metadata_path = self.find_latest_metadata_path().await?;
            let metadata = self.read_metadata(&metadata_path).await?;

            let snapshot = metadata
                .snapshots
                .iter()
                .find(|s| s.snapshot_id == metadata.current_snapshot_id)
                .ok_or_else(|| anyhow::anyhow!("current snapshot not found"))?;

            info!(manifest_list = %snapshot.manifest_list, "found current snapshot (via S3 scan)");
            snapshot.manifest_list.clone()
        };

        // Step 2: Read manifest-list to get manifest paths (uses existing Avro parsing)
        let manifest_paths = self.read_manifest_list(&manifest_list_url).await?;

        // Step 3: Read each manifest and build partition map (uses existing Avro parsing)
        let mut map = PartitionFileMap::new();
        for manifest_url in &manifest_paths {
            self.read_manifest(manifest_url, &mut map).await?;
        }

        // Log summary
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
