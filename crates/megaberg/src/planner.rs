//! Planning phase: discovery, packing, and task building.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use engine::planner::{build_tasks, pack_files, CompactionTask, PackerConfig, TableSource};
use tracing::info;

use crate::configs::AppConfig;

/// Result of the planning phase.
pub struct PlanResult {
    /// Compaction tasks to execute
    pub tasks: Vec<CompactionTask>,
    /// Arrow schema for the table
    pub schema: SchemaRef,
    /// Total bytes across all input files
    pub total_bytes: u64,
    /// Time spent in planning phase
    pub planning_time: Duration,
}

/// Run the planning phase: discover files, pack into groups, build tasks.
///
/// Returns `None` if there are no files to compact.
pub async fn plan(source: &dyn TableSource, config: &AppConfig) -> Result<Option<PlanResult>> {
    let planning_start = Instant::now();

    // Get schema from source (via trait)
    info!("Reading schema from source...");
    let schema = source.schema().await.context("Failed to get schema")?;
    info!(fields = schema.fields().len(), "Got schema");

    // Discover files (via trait)
    info!("Discovering files...");
    let partition_map = source.discover().await.context("Failed to discover files")?;

    let total_files: usize = partition_map.values().map(|v| v.len()).sum();
    let total_bytes: u64 = partition_map
        .values()
        .flat_map(|v| v.iter())
        .map(|f| f.size_bytes)
        .sum();

    info!(
        partitions = partition_map.len(),
        files = total_files,
        total_mb = total_bytes / 1024 / 1024,
        "Discovery complete"
    );

    if total_files == 0 {
        return Ok(None);
    }

    // Pack files into groups
    let packer_config = PackerConfig {
        target_file_size: config.compaction.target_file_size_mb * 1024 * 1024,
        min_files_per_partition: config.compaction.min_files_per_partition,
    };

    info!(
        target_file_size_mb = config.compaction.target_file_size_mb,
        min_files = config.compaction.min_files_per_partition,
        "Packing files into groups"
    );

    let file_groups = pack_files(partition_map, &packer_config);

    if file_groups.is_empty() {
        info!("No file groups to compact (partitions may have too few files)");
        return Ok(None);
    }

    info!(groups = file_groups.len(), "Created file groups");

    // Build tasks
    let tasks = build_tasks(file_groups, source.location());
    let planning_time = planning_start.elapsed();

    info!(
        tasks = tasks.len(),
        planning_time_ms = planning_time.as_millis(),
        "Built compaction tasks"
    );

    Ok(Some(PlanResult {
        tasks,
        schema,
        total_bytes,
        planning_time,
    }))
}
