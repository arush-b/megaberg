//! Megaberg: Iceberg table compaction CLI
//!
//! Usage: megaberg --config config.json
//!
//! See configs.rs for configuration format examples.

mod configs;
mod planner;
mod runner;
mod source;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use engine::planner::OutputFileInfo;
use tracing::{info, warn};

use crate::configs::{AppConfig, ConfigFile};
use crate::runner::TaskResult;

/// Megaberg: Iceberg table compaction tool
#[derive(Parser, Debug)]
#[command(name = "megaberg")]
#[command(about = "Iceberg table compaction tool")]
struct Args {
    /// Path to config JSON file
    #[arg(short, long)]
    config: PathBuf,
}

impl Args {
    /// Load and validate configuration from the specified config file.
    fn load_config(&self) -> Result<AppConfig> {
        let config_str = std::fs::read_to_string(&self.config)
            .with_context(|| format!("Failed to read config file: {}", self.config.display()))?;

        let config_file: ConfigFile =
            serde_json::from_str(&config_str).with_context(|| "Failed to parse config JSON")?;

        config_file.into_app_config()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("megaberg=info".parse().unwrap())
                .add_directive("engine=info".parse().unwrap()),
        )
        .init();

    // Parse CLI and load config
    let args = Args::parse();
    info!(config_path = %args.config.display(), "Loading configuration");
    let config = args.load_config()?;

    // Build S3 operator
    let operator = config.build_operator()?;
    info!(
        bucket = %config.s3.bucket,
        endpoint = %config.s3.endpoint,
        "Built S3 operator"
    );

    // Create source and sink (shared instance when same table)
    let (source, sink) = source::create_source_and_sink(&config, operator).await?;

    info!(
        source_format = ?source.format(),
        source_location = %source.location(),
        sink_format = ?sink.format(),
        "Created source and sink"
    );

    // Plan: discover files from source, pack into groups, build tasks
    let plan_result = match planner::plan(source.as_ref(), &config).await? {
        Some(result) => result,
        None => {
            warn!("No files to compact");
            return Ok(());
        }
    };

    // Execute compaction
    let exec_result = runner::run(plan_result.tasks, plan_result.schema, &config).await?;

    // Print summary
    print_summary(
        plan_result.planning_time,
        exec_result.execution_time,
        exec_result.worker_count,
        plan_result.total_bytes,
        exec_result.total_input_files,
        exec_result.total_output_files,
        exec_result.total_output_bytes,
    );

    // Commit to sink via trait
    if !exec_result.task_results.is_empty() {
        commit_results(sink.as_ref(), exec_result.task_results).await?;
    }

    Ok(())
}

/// Print compaction summary to console and logs.
fn print_summary(
    planning_time: Duration,
    execution_time: Duration,
    worker_count: usize,
    total_input_bytes: u64,
    total_input_files: usize,
    total_output_files: usize,
    total_output_bytes: u64,
) {
    let input_throughput =
        (total_input_bytes as f64 / 1024.0 / 1024.0) / execution_time.as_secs_f64();
    let avg_task_duration_secs = if total_input_files > 0 {
        execution_time.as_secs_f64() / total_input_files as f64 * worker_count as f64
    } else {
        0.0
    };

    info!(
        planning_time_ms = planning_time.as_millis(),
        avg_task_time_secs = avg_task_duration_secs,
        total_compaction_time_secs = execution_time.as_secs_f64(),
        workers = worker_count,
        input_files = total_input_files,
        input_mb = total_input_bytes / 1024 / 1024,
        output_files = total_output_files,
        output_mb = total_output_bytes / 1024 / 1024,
        throughput_mb_s = input_throughput,
        "Compaction complete"
    );

    println!("\n========== COMPACTION SUMMARY ==========");
    println!("Workers:                 {:>10}", worker_count);
    println!(
        "Planning time:           {:>10.2} ms",
        planning_time.as_secs_f64() * 1000.0
    );
    println!("Avg task compaction:     {:>10.2} s", avg_task_duration_secs);
    println!(
        "Total compaction time:   {:>10.2} s",
        execution_time.as_secs_f64()
    );
    println!("-----------------------------------------");
    println!("Input files:             {:>10}", total_input_files);
    println!(
        "Input size:              {:>10} MB",
        total_input_bytes / 1024 / 1024
    );
    println!("Output files:            {:>10}", total_output_files);
    println!(
        "Output size:             {:>10} MB",
        total_output_bytes / 1024 / 1024
    );
    println!("Throughput (input):      {:>10.2} MB/s", input_throughput);
    println!("=========================================\n");
}

/// Commit compaction results using the TableSink trait.
async fn commit_results(
    sink: &dyn engine::planner::TableSink,
    task_results: Vec<TaskResult>,
) -> Result<()> {
    let mut added_files = Vec::new();
    let mut removed_files = Vec::new();

    for result in task_results {
        removed_files.extend(result.input_files);
        added_files.extend(result.output_files.into_iter().map(|f| OutputFileInfo {
            path: f.path,
            size_bytes: f.size_bytes,
            record_count: f.record_count,
        }));
    }

    info!(
        added = added_files.len(),
        removed = removed_files.len(),
        format = ?sink.format(),
        "Committing compaction results"
    );

    let result = sink.commit(added_files, removed_files).await?;

    info!(
        snapshot_id = ?result.snapshot_id,
        files_added = result.files_added,
        files_removed = result.files_removed,
        "Commit complete"
    );

    Ok(())
}
