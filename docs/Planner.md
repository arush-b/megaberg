# Planner

The planner discovers files in a table and packs them into groups for compaction.

## Pipeline

```
Discovery → Packing → FileGroups
```

## Discovery

Builds `HashMap<Partition, Vec<FileInfo>>` from Iceberg metadata.

```rust
IcebergDiscovery::discover(table_path) -> Result<PartitionFileMap>
```

**Data Flow:**
```
metadata.json → manifest-list (Avro) → manifests (Avro) → PartitionFileMap
```

## Packing

Groups files into `FileGroup`s using bin-packing.

```rust
pack_files(map: PartitionFileMap, config: &PackerConfig) -> Vec<FileGroup>
```

**Config:**
- `target_file_size`: target bytes per output file
- `min_files_per_partition`: skip partitions with fewer files

## Module Structure

```
planner/
├── mod.rs              # Re-exports
├── packer.rs           # Bin-packing logic
└── discovery/
    ├── mod.rs          # Discovery trait, FileInfo, FileGroup, PartitionFileMap
    └── iceberg.rs      # IcebergDiscovery implementation
```

## Types

```rust
pub struct FileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

pub type PartitionFileMap = HashMap<String, Vec<FileInfo>>;

pub struct FileGroup {
    pub partition: String,
    pub files: Vec<FileInfo>,
    pub total_bytes: u64,
    pub total_records: u64,
}

pub struct PackerConfig {
    pub target_file_size: u64,
    pub min_files_per_partition: usize,
}
```
