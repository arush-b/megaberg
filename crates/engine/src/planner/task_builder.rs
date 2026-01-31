//! Builds CompactionTasks from FileGroups for executor consumption.

use super::discovery::FileGroup;
use uuid::Uuid;

/// A compaction task ready for execution.
#[derive(Debug, Clone)]
pub struct CompactionTask {
    /// Unique identifier for this task
    pub task_id: String,
    /// Partition key (e.g., "id_bucket=0")
    pub partition: String,
    /// Input file paths to compact
    pub input_files: Vec<String>,
    /// Output directory path (e.g., "db/test_table/data/id_bucket=0")
    pub output_prefix: String,
}

/// Builds CompactionTasks from FileGroups.
pub fn build_tasks(groups: Vec<FileGroup>, table_location: &str) -> Vec<CompactionTask> {
    groups
        .into_iter()
        .map(|group| build_task(group, table_location))
        .collect()
}

/// Builds a single CompactionTask from a FileGroup.
fn build_task(group: FileGroup, table_location: &str) -> CompactionTask {
    let task_id = Uuid::new_v4().to_string();

    // Extract paths from FileInfo
    let input_files: Vec<String> = group.files.into_iter().map(|f| f.path).collect();

    // Build output prefix: {table_location}/data/{partition}/
    // For unpartitioned tables (empty partition), just use {table_location}/data/
    //
    // Handle two cases:
    // 1. Full s3:// URL: "s3://bucket/path/to/table" -> "path/to/table"
    // 2. Relative path: "output_data/" -> "output_data"
    let table_path = if table_location.starts_with("s3://") {
        // Strip s3://bucket/ prefix
        table_location
            .trim_start_matches("s3://")
            .split('/')
            .skip(1) // skip bucket name
            .collect::<Vec<_>>()
            .join("/")
    } else {
        // Relative path - use as-is (trim trailing slash)
        table_location.trim_end_matches('/').to_string()
    };

    let output_prefix = if group.partition.is_empty() {
        format!("{table_path}/data")
    } else {
        format!("{}/data/{}", table_path, group.partition)
    };

    CompactionTask {
        task_id,
        partition: group.partition,
        input_files,
        output_prefix,
    }
}
