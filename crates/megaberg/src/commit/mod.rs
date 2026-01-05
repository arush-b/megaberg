//! Commit compaction results to Iceberg catalog.
//!
//! This module commits compaction results by removing old data files and adding
//! new compacted files in a single atomic transaction with operation type "replace".
//!
//! # Submodules
//!
//! - `types` - Common types (TaskResult, OutputFile, CommitContext)
//! - `manifest` - Manifest writing utilities
//! - `snapshot` - Snapshot building utilities
//! - `rest` - REST catalog commit
//! - `glue` - Glue catalog commit

mod glue;
mod manifest;
mod rest;
mod snapshot;
mod types;

use std::collections::HashSet;

pub use types::{OutputFile, TaskResult};

// Re-export commit functions
pub use glue::commit as commit_compaction_glue;
pub use rest::commit as commit_compaction;

/// Collect files from task results into deleted paths and added files.
fn collect_files(
    task_results: Vec<TaskResult>,
    s3_bucket: &str,
) -> (HashSet<String>, Vec<OutputFile>) {
    let mut deleted_paths: HashSet<String> = HashSet::new();
    let mut added_files: Vec<OutputFile> = Vec::new();

    for result in task_results {
        for path in result.input_files {
            let full_path = if path.starts_with("s3://") {
                path
            } else {
                format!("s3://{}/{}", s3_bucket, path)
            };
            deleted_paths.insert(full_path);
        }
        added_files.extend(result.output_files);
    }

    (deleted_paths, added_files)
}
