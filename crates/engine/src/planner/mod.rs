pub mod discovery;
pub mod packer;
pub mod table;
pub mod task_builder;

pub use discovery::{
    load_table_info, CatalogConfig, FileGroup, FileInfo, PartitionFileMap, TableFormat, TableInfo,
};
pub use packer::{pack_files, PackerConfig};
pub use task_builder::{build_tasks, CompactionTask};

// Re-export table traits and implementations
pub use table::{
    TableSource, TableSink, CommitResult, OutputFileInfo,
    IcebergSource, ParquetSource,
};
