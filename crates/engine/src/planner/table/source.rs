//! TableSource trait for discovering files from various table formats.

use anyhow::Result;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use super::super::discovery::{PartitionFileMap, TableFormat};

/// Source of files for compaction.
///
/// This trait abstracts over different table formats (Iceberg, Parquet, Delta)
/// providing a uniform interface for:
/// - Discovering files grouped by partition
/// - Reading schema for the executor
/// - Getting table location for output path construction
#[async_trait]
pub trait TableSource: Send + Sync {
    /// What format is this source?
    fn format(&self) -> TableFormat;

    /// Base location of the table.
    /// Used by task builder for constructing output paths.
    fn location(&self) -> &str;

    /// Arrow schema for reading/writing data.
    /// Executor needs this to correctly read parquet files.
    async fn schema(&self) -> Result<SchemaRef>;

    /// Discover files grouped by partition.
    /// Returns a map of partition_key -> files in that partition.
    async fn discover(&self) -> Result<PartitionFileMap>;
}
