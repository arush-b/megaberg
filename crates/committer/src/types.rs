//! Core types for the committer.

use std::collections::HashMap;

/// Table provider type - determines commit behavior.
pub enum TableProvider {
    /// Raw parquet files, no catalog metadata.
    RawParquet,
    /// Iceberg table with full catalog support.
    Iceberg,
}

/// Information about a data file to be added or deleted.
pub struct DataFileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
    pub partition_values: Option<HashMap<String, String>>,
    /// Sequence number when file was added (required for delete entries)
    pub sequence_number: Option<i64>,
}

/// Request to commit files to a table.
pub struct CommitRequest {
    pub provider: TableProvider,
    pub table_location: String,
    pub namespace: Option<String>,
    pub table_name: Option<String>,
    pub added_data_files: Vec<DataFileInfo>,
    pub deleted_data_files: Vec<String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_context_basic_creation() {
        // Valid inputs produce non-empty paths, snapshot_id > 0
        let ctx = CommitContext::new("s3://bucket/table", None, 0);

        assert!(ctx.new_snapshot_id > 0);
        assert!(!ctx.manifest_path.is_empty());
        assert!(!ctx.manifest_list_path.is_empty());
        assert!(ctx.manifest_path.contains("/metadata/"));
        assert!(ctx.manifest_path.ends_with(".avro"));
        assert!(ctx.manifest_list_path.contains("/metadata/snap-"));
    }

    #[test]
    fn test_commit_context_sequence_number_increments() {
        // new_sequence_number should be current_seq + 1
        let ctx = CommitContext::new("s3://bucket/table", None, 5);
        assert_eq!(ctx.new_sequence_number, 6);

        let ctx2 = CommitContext::new("s3://bucket/table", None, 0);
        assert_eq!(ctx2.new_sequence_number, 1);

        let ctx3 = CommitContext::new("s3://bucket/table", Some(100), 99);
        assert_eq!(ctx3.new_sequence_number, 100);
    }

    #[test]
    fn test_commit_context_parent_snapshot_none() {
        // First commit (parent=None) sets parent_snapshot_id to None
        let ctx = CommitContext::new("s3://bucket/table", None, 0);
        assert_eq!(ctx.parent_snapshot_id, None);
    }

    #[test]
    fn test_commit_context_parent_snapshot_some() {
        // Subsequent commits store the provided parent_snapshot_id
        let parent_id = 1234567890123_i64;
        let ctx = CommitContext::new("s3://bucket/table", Some(parent_id), 5);
        assert_eq!(ctx.parent_snapshot_id, Some(parent_id));
    }
}
