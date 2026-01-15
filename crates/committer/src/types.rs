//! Core types for the committer.

use std::collections::HashMap;

/// Table provider type - determines commit behavior.
pub enum TableProvider {
    /// Raw parquet files, no catalog metadata.
    RawParquet,
    /// Iceberg table with full catalog support.
    Iceberg,
}

/// Information about a data file to be added.
pub struct DataFileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
    pub partition_values: Option<HashMap<String, String>>,
}

/// Request to commit files to a table.
pub struct CommitRequest {
    pub provider: TableProvider,
    pub table_location: String,
    pub namespace: Option<String>,
    pub table_name: Option<String>,
    pub added_data_files: Vec<DataFileInfo>,
    pub deleted_data_files: Vec<String>, //provisioned for later, not being used yet
}

/// Output file produced by executor.
pub struct OutputFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

/// Result from a single compaction task.
pub struct TaskResult {
    pub input_files: Vec<String>,
    pub output_files: Vec<OutputFile>,
}

/// Internal context for a commit - IDs and paths generated before writing.
pub(crate) struct CommitContext {
    pub new_snapshot_id: i64,
    pub new_sequence_number: i64,
    pub parent_snapshot_id: Option<i64>,
    pub commit_uuid: uuid::Uuid,
    pub manifest_path: String,
    pub manifest_list_path: String,
}

impl CommitContext {
    pub fn new(table_location: &str, parent_snapshot_id: Option<i64>, current_seq: i64) -> Self {
        let new_snapshot_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let commit_uuid = uuid::Uuid::now_v7();

        Self {
            new_snapshot_id,
            new_sequence_number: current_seq + 1,
            parent_snapshot_id,
            commit_uuid,
            manifest_path: format!("{}/metadata/{}-m0.avro", table_location, commit_uuid),
            manifest_list_path: format!(
                "{}/metadata/snap-{}-0-{}.avro",
                table_location, new_snapshot_id, commit_uuid
            ),
        }
    }
}
