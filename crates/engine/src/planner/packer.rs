//! Bin-packer: packs files into FileGroups for compaction.

use super::discovery::{FileGroup, FileInfo, PartitionFileMap};
use tracing::info;

/// Configuration for the packer
pub struct PackerConfig {
    /// Target size for each output file (bytes)
    pub target_file_size: u64,
    /// Minimum files per partition to trigger compaction
    pub min_files_per_partition: usize,
}

/// Packs files from partition map into FileGroups using bin-packing.
pub fn pack_files(map: PartitionFileMap, config: &PackerConfig) -> Vec<FileGroup> {
    let mut groups = Vec::new();

    // Sort partitions for deterministic output
    let mut partitions: Vec<_> = map.into_iter().collect();
    partitions.sort_by(|a, b| a.0.cmp(&b.0));

    for (partition, files) in partitions {
        // Skip partitions with too few files
        if files.len() < config.min_files_per_partition {
            continue;
        }

        // Pack files for this partition
        let partition_groups = bin_pack_partition(&partition, files, config.target_file_size);
        groups.extend(partition_groups);
    }

    info!(group_count = groups.len(), "packed files for compaction");

    groups
}

/// Bin-pack files in a single partition into groups.
fn bin_pack_partition(
    partition: &str,
    mut files: Vec<FileInfo>,
    target_size: u64,
) -> Vec<FileGroup> {
    // Sort by size descending (largest first for better packing)
    files.sort_by(|a, b| b.size_bytes.cmp(&a.size_bytes));

    let mut groups = Vec::new();
    let mut current_files = Vec::new();
    let mut current_bytes = 0u64;
    let mut current_records = 0u64;

    for file in files {
        // If adding this file would exceed target, start new group
        if current_bytes + file.size_bytes > target_size && !current_files.is_empty() {
            groups.push(FileGroup {
                partition: partition.to_string(),
                files: std::mem::take(&mut current_files),
                total_bytes: current_bytes,
                total_records: current_records,
            });
            current_bytes = 0;
            current_records = 0;
        }

        current_bytes += file.size_bytes;
        current_records += file.record_count;
        current_files.push(file);
    }

    // Don't forget the last group
    if !current_files.is_empty() {
        groups.push(FileGroup {
            partition: partition.to_string(),
            files: current_files,
            total_bytes: current_bytes,
            total_records: current_records,
        });
    }

    groups
}
