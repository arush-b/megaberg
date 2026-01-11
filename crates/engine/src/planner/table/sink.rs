//! TableSink trait for committing compaction results.

use anyhow::Result;
use async_trait::async_trait;

use super::super::discovery::TableFormat;

/// Result of committing compaction.
#[derive(Debug)]
pub struct CommitResult {
    /// Snapshot ID if applicable (Iceberg, Delta)
    pub snapshot_id: Option<i64>,
    /// Number of files added
    pub files_added: usize,
    /// Number of files removed
    pub files_removed: usize,
}

/// Output file info for commit.
#[derive(Debug, Clone)]
pub struct OutputFileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

/// Sink for committing compaction results.
///
/// This trait abstracts over different table formats' commit protocols:
/// - Iceberg: manifest + catalog commit
/// - Delta: transaction log commit
/// - Parquet: no metadata, optionally delete source files
#[async_trait]
pub trait TableSink: Send + Sync {
    /// What format is this sink?
    fn format(&self) -> TableFormat;

    /// Commit compaction results.
    ///
    /// - added_files: newly written compacted files
    /// - removed_files: original files that were compacted (to be removed from table)
    async fn commit(
        &self,
        added_files: Vec<OutputFileInfo>,
        removed_files: Vec<String>,
    ) -> Result<CommitResult>;
}
