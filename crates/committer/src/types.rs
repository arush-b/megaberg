//! Core types for the committer.

use std::collections::HashMap;

/// Output file produced by compaction.
#[derive(Debug, Clone)]
pub struct OutputFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
    /// Partition key (e.g., "year=2024/month=01").
    pub partition: String,
}

/// Result from a single compaction task.
#[derive(Debug, Clone)]
pub struct TaskResult {
    /// Input file paths that were compacted (to be removed).
    pub input_files: Vec<String>,
    /// Output files that were created.
    pub output_files: Vec<OutputFile>,
}

/// Information about a data file for catalog commit.
pub struct DataFileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
    pub partition_values: Option<HashMap<String, String>>,
    /// Sequence number when file was added (required for delete entries).
    pub sequence_number: Option<i64>,
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
