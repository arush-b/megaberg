mod iceberg;

pub use iceberg::IcebergDiscovery;

use std::collections::HashMap;

/// Supported table formats
#[derive(Debug, Clone, Copy)]
pub enum TableFormat {
    Iceberg,
}

/// Information about a single data file
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

/// Map of partition key to files in that partition
pub type PartitionFileMap = HashMap<String, Vec<FileInfo>>;

/// A group of files to be compacted together
#[derive(Debug, Clone)]
pub struct FileGroup {
    pub partition: String,
    pub files: Vec<FileInfo>,
    pub total_bytes: u64,
    pub total_records: u64,
}

/// Discovery trait for different table formats
#[async_trait::async_trait]
pub trait Discovery {
    async fn discover(&self) -> anyhow::Result<PartitionFileMap>;
}
