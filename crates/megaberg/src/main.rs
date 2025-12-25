//! Megaberg: Iceberg table compaction CLI
//!
//! Usage: megaberg --config config.json
//!
//! Config JSON format (with explicit credentials):
//! ```json
//! {
//!   "s3": {
//!     "endpoint": "http://localhost:9000",
//!     "bucket": "warehouse",
//!     "region": "us-east-1",
//!     "access_key": "admin",
//!     "secret_key": "password",
//!     "disable_config_load": true
//!   },
//!   "catalog": {
//!     "type": "rest",
//!     "uri": "http://localhost:8181",
//!     "warehouse": "s3://warehouse"
//!   },
//!   "table": {
//!     "namespace": "db",
//!     "name": "my_table"
//!   },
//!   "compaction": {
//!     "target_file_size_mb": 256,
//!     "min_files_per_partition": 2
//!   }
//! }
//! ```
//!
//! Config JSON format (with env credentials - AWS_ACCESS_KEY_ID, etc.):
//! ```json
//! {
//!   "s3": {
//!     "endpoint": "https://s3.us-east-1.amazonaws.com",
//!     "bucket": "my-bucket",
//!     "region": "us-east-1"
//!   },
//!   "catalog": {
//!     "type": "glue",
//!     "warehouse": "s3://my-bucket/warehouse",
//!     "region": "us-east-1"
//!   },
//!   "table": {
//!     "namespace": "my_database",
//!     "name": "my_table"
//!   }
//! }
//! ```

mod commit;
mod configs;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use clap::Parser;
use engine::executor::{CompactionConfig, CompactionEngine, S3Config};
use engine::planner::{
    build_tasks, load_table_info, pack_files, CatalogConfig, Discovery,
    IcebergDiscovery, PackerConfig,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use opendal::Operator;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::basic::Compression;
use parquet::file::reader::{FileReader, SerializedFileReader};
use tracing::{info, warn};

use crate::commit::{commit_compaction, commit_compaction_glue, OutputFile, TaskResult};
use crate::configs::Config;

/// Read Arrow schema from a parquet file
async fn read_schema_from_parquet(operator: &Operator, path: &str) -> Result<SchemaRef> {
    // Read just enough bytes to get the footer (last 8 bytes has footer size)
    let file_bytes = operator.read(path).await
        .with_context(|| format!("Failed to read parquet file: {}", path))?;

    let reader = SerializedFileReader::new(Bytes::from(file_bytes.to_vec()))
        .context("Failed to create parquet reader")?;

    let parquet_schema = reader.metadata().file_metadata().schema_descr();
    let arrow_schema = parquet_to_arrow_schema(parquet_schema, None)
        .context("Failed to convert parquet schema to arrow")?;

    Ok(Arc::new(arrow_schema))
}

#[derive(Parser, Debug)]
#[command(name = "megaberg")]
#[command(about = "Iceberg table compaction tool")]
struct Args {
    /// Path to config JSON file
    #[arg(short, long)]
    config: PathBuf,
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

    let args = Args::parse();

    // Load config
    info!(config_path = %args.config.display(), "Loading configuration");
    let config_str = std::fs::read_to_string(&args.config)
        .with_context(|| format!("Failed to read config file: {}", args.config.display()))?;
    let config: Config = serde_json::from_str(&config_str)
        .with_context(|| "Failed to parse config JSON")?;

    info!(
        namespace = %config.table.namespace,
        table = %config.table.name,
        "Starting compaction"
    );

    // Start planning timer
    let planning_start = std::time::Instant::now();

    // Build OpenDAL operator for discovery
    let operator = config.s3.build_operator()?;
    info!(
        bucket = %config.s3.bucket,
        region = %config.s3.region,
        endpoint = %config.s3.endpoint,
        using_env_creds = config.s3.access_key.is_none(),
        "Built S3 operator"
    );

    // Step 1: Load table info from catalog
    info!("Loading table info from catalog...");
    let catalog_config: CatalogConfig = (&config.catalog).into();
    let table_info = load_table_info(
        &catalog_config,
        &config.table.namespace,
        &config.table.name,
    )
    .await
    .context("Failed to load table info from catalog")?;

    info!(
        location = %table_info.location,
        snapshot_id = table_info.snapshot_id,
        manifest_list = %table_info.manifest_list_path,
        "Loaded table info"
    );

    // Step 2: Discover files
    info!("Discovering files from manifests...");
    let discovery = IcebergDiscovery::new(table_info.clone(), operator.clone());
    let partition_map = discovery.discover().await
        .context("Failed to discover files")?;

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
        warn!("No files found to compact");
        return Ok(());
    }

    // Step 3: Pack files into groups
    let compaction_cfg = config.compaction.clone().unwrap_or_default();

    let packer_config = PackerConfig {
        target_file_size: compaction_cfg.target_file_size_mb * 1024 * 1024,
        min_files_per_partition: compaction_cfg.min_files_per_partition,
    };

    info!(
        target_file_size_mb = compaction_cfg.target_file_size_mb,
        min_files = compaction_cfg.min_files_per_partition,
        "Packing files into groups"
    );

    let file_groups = pack_files(partition_map, &packer_config);

    if file_groups.is_empty() {
        info!("No file groups to compact (partitions may have too few files)");
        return Ok(());
    }

    info!(groups = file_groups.len(), "Created file groups");

    // Step 4: Build compaction tasks
    let tasks = build_tasks(file_groups, &table_info.location);
    let planning_time = planning_start.elapsed();
    info!(
        tasks = tasks.len(),
        planning_time_ms = planning_time.as_millis(),
        "Built compaction tasks"
    );

    // Step 5: Read schema from first input file
    let first_file = tasks
        .first()
        .and_then(|t| t.input_files.first())
        .context("No input files found")?;

    // Strip s3://bucket/ prefix from the path
    let s3_prefix = format!("s3://{}/", config.s3.bucket);
    let first_file_path = first_file.strip_prefix(&s3_prefix).unwrap_or(first_file);

    info!(file = %first_file_path, "Reading schema from first parquet file");
    let schema = read_schema_from_parquet(&operator, first_file_path).await
        .context("Failed to read schema from parquet file")?;
    info!(schema = ?schema, "Read schema with {} fields", schema.fields().len());

    // Step 6: Create single compaction engine and reuse for all tasks
    let s3_config: S3Config = (&config.s3).into();
    let compaction_config = CompactionConfig {
        target_file_size_bytes: compaction_cfg.target_file_size_mb * 1024 * 1024,
        row_group_size: 300_000, // Match test config for better throughput
        compression: Compression::UNCOMPRESSED,
        ..Default::default()
    };

    info!("Creating compaction engine...");
    let engine = CompactionEngine::with_s3(schema, &s3_config, compaction_config)
        .await
        .context("Failed to create compaction engine")?;

    // Step 7: Execute tasks sequentially
    let compaction_start = std::time::Instant::now();
    let mut total_input_files = 0usize;
    let mut total_output_files = 0usize;
    let mut total_output_bytes = 0u64;
    let mut task_durations: Vec<std::time::Duration> = Vec::new();
    let mut task_results: Vec<TaskResult> = Vec::new();

    for (i, task) in tasks.iter().enumerate() {
        info!(
            task = i + 1,
            total = tasks.len(),
            partition = %task.partition,
            input_files = task.input_files.len(),
            "Executing compaction task"
        );

        let task_start = std::time::Instant::now();
        let result = engine
            .compact(&task.task_id, task.input_files.clone(), &task.output_prefix)
            .await;

        match result {
            Ok(written_files) => {
                let task_duration = task_start.elapsed();
                task_durations.push(task_duration);
                let task_bytes: u64 = written_files.iter().map(|f| f.size_bytes).sum();
                let task_records: u64 = written_files.iter().map(|f| f.record_count).sum();

                info!(
                    task = i + 1,
                    output_files = written_files.len(),
                    output_mb = task_bytes / 1024 / 1024,
                    records = task_records,
                    duration_secs = task_duration.as_secs_f64(),
                    throughput_mb_s = (task_bytes as f64 / 1024.0 / 1024.0) / task_duration.as_secs_f64(),
                    "Task completed"
                );

                // Track task result for commit
                task_results.push(TaskResult {
                    input_files: task.input_files.clone(),
                    output_files: written_files
                        .iter()
                        .map(|f| OutputFile {
                            path: f.path.clone(),
                            size_bytes: f.size_bytes,
                            record_count: f.record_count,
                        })
                        .collect(),
                });

                total_input_files += task.input_files.len();
                total_output_files += written_files.len();
                total_output_bytes += task_bytes;
            }
            Err(e) => {
                warn!(
                    task = i + 1,
                    partition = %task.partition,
                    error = %e,
                    "Task failed, continuing with next task"
                );
            }
        }
    }

    let total_compaction_time = compaction_start.elapsed();

    // Compute average task duration
    let avg_task_duration = if !task_durations.is_empty() {
        let total_task_time: std::time::Duration = task_durations.iter().sum();
        total_task_time / task_durations.len() as u32
    } else {
        std::time::Duration::ZERO
    };

    // Use input bytes (from discovery) for throughput - matches test methodology
    let input_throughput = (total_bytes as f64 / 1024.0 / 1024.0) / total_compaction_time.as_secs_f64();

    info!(
        planning_time_ms = planning_time.as_millis(),
        avg_task_time_secs = avg_task_duration.as_secs_f64(),
        total_compaction_time_secs = total_compaction_time.as_secs_f64(),
        input_files = total_input_files,
        input_mb = total_bytes / 1024 / 1024,
        output_files = total_output_files,
        output_mb = total_output_bytes / 1024 / 1024,
        throughput_mb_s = input_throughput,
        "Compaction complete"
    );

    // Print summary table
    println!("\n========== COMPACTION SUMMARY ==========");
    println!("Planning time:           {:>10.2} ms", planning_time.as_secs_f64() * 1000.0);
    println!("Avg task compaction:     {:>10.2} s", avg_task_duration.as_secs_f64());
    println!("Total compaction time:   {:>10.2} s", total_compaction_time.as_secs_f64());
    println!("-----------------------------------------");
    println!("Input files:             {:>10}", total_input_files);
    println!("Input size:              {:>10} MB", total_bytes / 1024 / 1024);
    println!("Output files:            {:>10}", total_output_files);
    println!("Output size:             {:>10} MB", total_output_bytes / 1024 / 1024);
    println!("Throughput (input):      {:>10.2} MB/s", input_throughput);
    println!("=========================================\n");

    // Step 8: Commit compaction results to catalog
    if !task_results.is_empty() {
        info!("Committing compaction results to catalog...");

        let table_ident = TableIdent::new(
            NamespaceIdent::new(config.table.namespace.clone()),
            config.table.name.clone(),
        );

        match &config.catalog {
            crate::configs::CatalogConfigJson::Rest { uri, warehouse } => {
                // Build REST catalog
                let mut props = HashMap::new();
                props.insert("uri".to_string(), uri.clone());
                if let Some(wh) = warehouse {
                    props.insert("warehouse".to_string(), wh.clone());
                }
                props.insert("s3.endpoint".to_string(), config.s3.endpoint.clone());
                props.insert("s3.region".to_string(), config.s3.region.clone());
                if let Some(ref ak) = config.s3.access_key {
                    props.insert("s3.access-key-id".to_string(), ak.clone());
                }
                if let Some(ref sk) = config.s3.secret_key {
                    props.insert("s3.secret-access-key".to_string(), sk.clone());
                }
                props.insert("s3.allow-http".to_string(), "true".to_string());
                props.insert("s3.path-style-access".to_string(), "true".to_string());

                let catalog = RestCatalogBuilder::default()
                    .load("rest", props)
                    .await
                    .context("Failed to build REST catalog")?;

                let table = catalog
                    .load_table(&table_ident)
                    .await
                    .context("Failed to load table for commit")?;

                commit_compaction(
                    &table,
                    task_results,
                    &config.s3.bucket,
                    uri,
                    &config.table.namespace,
                    &config.table.name,
                )
                .await
                .context("Failed to commit compaction")?;
            }
            crate::configs::CatalogConfigJson::Glue { warehouse, region, .. } => {
                // Build Glue catalog
                let mut props = HashMap::new();
                props.insert("warehouse".to_string(), warehouse.clone());
                if let Some(r) = region {
                    props.insert("region".to_string(), r.clone());
                }

                let catalog = GlueCatalogBuilder::default()
                    .load("glue", props)
                    .await
                    .context("Failed to build Glue catalog")?;

                let table = catalog
                    .load_table(&table_ident)
                    .await
                    .context("Failed to load table for commit")?;

                commit_compaction_glue(
                    &catalog,
                    &table,
                    task_results,
                    &config.s3.bucket,
                )
                .await
                .context("Failed to commit compaction to Glue")?;
            }
        }

        println!("Compaction committed to catalog!");
    }

    Ok(())
}
