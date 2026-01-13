//! Execution phase: executor lifecycle, task submission, result collection.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use engine::executor::{CompactionConfig, Executor, ExecutorConfig, WrittenFile};
use engine::planner::CompactionTask;
use parquet::basic::Compression;
use tracing::{info, warn};

use crate::configs::AppConfig;

/// Result of a single compaction task.
#[derive(Debug, Clone)]
pub struct TaskResult {
    /// Input file paths that were compacted (to be removed)
    pub input_files: Vec<String>,
    /// Output files that were created
    pub output_files: Vec<OutputFile>,
}

/// Information about an output file.
#[derive(Debug, Clone)]
pub struct OutputFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

/// Result of the execution phase.
pub struct ExecutionResult {
    /// Results for each completed task (for catalog commit)
    pub task_results: Vec<TaskResult>,
    /// Total input files processed
    pub total_input_files: usize,
    /// Total output files written
    pub total_output_files: usize,
    /// Total bytes written
    pub total_output_bytes: u64,
    /// Time spent in execution phase
    pub execution_time: Duration,
    /// Number of workers used
    pub worker_count: usize,
}

/// Run compaction tasks using the executor worker pool.
pub async fn run(
    tasks: Vec<CompactionTask>,
    schema: SchemaRef,
    config: &AppConfig,
) -> Result<ExecutionResult> {
    let compaction_config = CompactionConfig {
        target_file_size_bytes: config.compaction.target_file_size_mb * 1024 * 1024,
        row_group_size: 300_000,
        compression: Compression::UNCOMPRESSED,
        ..Default::default()
    };

    // Determine worker count (CPU count, capped at task count)
    let worker_count = std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
        .min(tasks.len());

    let executor_config = ExecutorConfig::default()
        .with_worker_count(worker_count)
        .with_s3(config.s3.clone())
        .with_compaction(compaction_config);

    info!(workers = worker_count, "Spawning executor worker pool...");
    let executor = Executor::new(executor_config, schema);
    let mut handle = executor.spawn().await.context("Failed to spawn executor")?;

    let execution_start = Instant::now();
    let task_count = tasks.len();

    info!(
        tasks = task_count,
        workers = handle.worker_count,
        "Submitting tasks to executor"
    );

    // Submit all tasks
    for task in &tasks {
        handle
            .submit((
                task.task_id.clone(),
                task.input_files.clone(),
                task.output_prefix.clone(),
            ))
            .await
            .context("Failed to submit task")?;
    }

    // Collect results
    let mut completed = 0usize;
    let mut total_input_files = 0usize;
    let mut total_output_files = 0usize;
    let mut total_output_bytes = 0u64;
    let mut task_results = Vec::new();

    while completed < task_count {
        if let Some((task_id, input_files, result)) = handle.recv().await {
            completed += 1;
            process_result(
                &task_id,
                input_files,
                result,
                completed,
                task_count,
                &mut task_results,
                &mut total_input_files,
                &mut total_output_files,
                &mut total_output_bytes,
            );

            // Log progress periodically
            if completed % 10 == 0 || completed == task_count {
                let metrics = handle.metrics().snapshot();
                info!(
                    completed = metrics.tasks_completed,
                    failed = metrics.tasks_failed,
                    in_progress = metrics.in_progress(),
                    avg_duration_secs = format!("{:.2}", metrics.avg_task_duration_secs()),
                    "Progress update"
                );
            }
        }
    }

    // Graceful shutdown
    let remaining = handle.shutdown().await;
    if !remaining.is_empty() {
        info!(
            additional_results = remaining.len(),
            "Collected remaining results during shutdown"
        );
    }

    Ok(ExecutionResult {
        task_results,
        total_input_files,
        total_output_files,
        total_output_bytes,
        execution_time: execution_start.elapsed(),
        worker_count,
    })
}

/// Process a single task result.
fn process_result(
    task_id: &str,
    input_files: Vec<String>,
    result: Result<Vec<WrittenFile>, String>,
    completed: usize,
    total: usize,
    task_results: &mut Vec<TaskResult>,
    total_input_files: &mut usize,
    total_output_files: &mut usize,
    total_output_bytes: &mut u64,
) {
    match result {
        Ok(written_files) => {
            let task_bytes: u64 = written_files.iter().map(|f| f.size_bytes).sum();
            let task_records: u64 = written_files.iter().map(|f| f.record_count).sum();

            info!(
                progress = format!("{}/{}", completed, total),
                %task_id,
                output_files = written_files.len(),
                output_mb = task_bytes / 1024 / 1024,
                records = task_records,
                "Task completed"
            );

            task_results.push(TaskResult {
                input_files: input_files.clone(),
                output_files: written_files
                    .iter()
                    .map(|f| OutputFile {
                        path: f.path.clone(),
                        size_bytes: f.size_bytes,
                        record_count: f.record_count,
                    })
                    .collect(),
            });

            *total_input_files += input_files.len();
            *total_output_files += written_files.len();
            *total_output_bytes += task_bytes;
        }
        Err(e) => {
            warn!(
                progress = format!("{}/{}", completed, total),
                %task_id,
                error = %e,
                "Task failed"
            );
        }
    }
}
