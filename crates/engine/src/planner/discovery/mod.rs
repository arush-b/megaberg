use std::collections::HashMap;

/// Supported table formats
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    Iceberg,
    Parquet,
}

/// Information about a single data file
#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
    pub sequence_number: Option<i64>,
    pub partition_values: HashMap<String, String>,
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
