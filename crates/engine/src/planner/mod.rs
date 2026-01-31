pub mod discovery;
pub mod packer;
pub mod table;
pub mod task_builder;

pub use discovery::{FileGroup, FileInfo, PartitionFileMap, TableFormat};
pub use packer::{pack_files, PackerConfig};
pub use task_builder::{build_tasks, CompactionTask};

// Re-export table traits and implementations
pub use table::{
    CatalogConfig,
    CommitResult,
    IcebergTable,
    LookbackWindow,
    ParquetSource,
    PartitionGranularity,
    PartitionTimeField,
    PartitionTimeSpec,
    TableSink,
    TableSource,
    TimeComponent,
    // Time window types for observability compaction
    TimeWindowConfig,
};
