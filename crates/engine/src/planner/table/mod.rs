//! Table abstraction layer for different table formats.
//!
//! This module provides traits and implementations for interacting with
//! different table formats (Iceberg, Parquet, Delta) in a uniform way.
//!
//! # Architecture
//!
//! ```text
//! TableSource (trait)          TableSink (trait)
//!      │                            │
//!      ├── IcebergSource            ├── IcebergSink (in megaberg crate)
//!      └── ParquetSource            └── ParquetSink (future)
//! ```
//!
//! The `TableSource` trait handles discovery (finding files to compact).
//! The `TableSink` trait handles commit (recording compaction results).
//!
//! # Usage
//!
//! ```ignore
//! // Create source from existing TableInfo
//! let source = IcebergSource::new(table_info, operator);
//!
//! // Discover files
//! let files = source.discover().await?;
//! let schema = source.schema().await?;
//!
//! // ... run compaction ...
//!
//! // Commit results via sink (IcebergSink in megaberg crate)
//! sink.commit(added_files, removed_files).await?;
//! ```

mod source;
mod sink;
mod iceberg;
mod parquet;

pub use source::TableSource;
pub use sink::{TableSink, CommitResult};
pub use iceberg::{IcebergTable, CatalogConfig};
pub use parquet::{
    ParquetSource,
    // Time window config types for observability data
    TimeWindowConfig, PartitionTimeSpec, PartitionTimeField,
    TimeComponent, PartitionGranularity, LookbackWindow,
};

// Re-export core types from discovery for convenience
pub use super::discovery::{TableFormat, FileInfo, PartitionFileMap, FileGroup};
