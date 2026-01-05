//! Common types for commit operations.

/// Result of a single compaction task.
#[derive(Debug, Clone)]
pub struct TaskResult {
    /// Input file paths that were compacted (to be deleted)
    pub input_files: Vec<String>,
    /// Output files that were created
    pub output_files: Vec<OutputFile>,
}

/// Information about an output file.
#[derive(Debug, Clone)]
pub struct OutputFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

/// Context for a compaction commit operation.
pub struct CommitContext {
    pub new_snapshot_id: i64,
    pub new_sequence_number: i64,
    pub parent_snapshot_id: Option<i64>,
    pub commit_uuid: uuid::Uuid,
    pub manifest_path: String,
    pub manifest_list_path: String,
}

impl CommitContext {
    /// Create a new commit context from table metadata.
    pub fn new(table_location: &str, parent_snapshot_id: Option<i64>, current_sequence_number: i64) -> Self {
        let new_snapshot_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let new_sequence_number = current_sequence_number + 1;
        let commit_uuid = uuid::Uuid::now_v7();

        let manifest_path = format!("{}/metadata/{}-m0.avro", table_location, commit_uuid);
        let manifest_list_path = format!(
            "{}/metadata/snap-{}-0-{}.avro",
            table_location, new_snapshot_id, commit_uuid
        );

        Self {
            new_snapshot_id,
            new_sequence_number,
            parent_snapshot_id,
            commit_uuid,
            manifest_path,
            manifest_list_path,
        }
    }
}
