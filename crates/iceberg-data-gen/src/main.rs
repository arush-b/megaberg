use anyhow::{Context, Result};
use clap::Parser;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::TableCreation;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use opendal::Operator;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;

mod config;
mod generator;
mod schema;
mod writer;

use config::{AppConfig, CatalogConfig, S3Config};
use generator::Generator;
use writer::write_parquet_file;

#[derive(Parser, Debug)]
#[command(name = "iceberg-data-gen")]
struct Args {
    /// Path to config JSON file
    #[arg(short, long)]
    config: PathBuf,
}

impl S3Config {
    fn build_operator(&self) -> Result<Operator> {
        let mut builder = opendal::services::S3::default()
            .bucket(&self.bucket)
            .region(&self.region)
            .endpoint(&self.endpoint);

        if let Some(ref access_key) = self.access_key {
            builder = builder.access_key_id(access_key);
        }
        if let Some(ref secret_key) = self.secret_key {
            builder = builder.secret_access_key(secret_key);
        }
        if self.disable_config_load {
            builder = builder.disable_config_load();
        }

        let op = Operator::new(builder)
            .context("Failed to create OpenDAL S3 operator")?
            .finish();
        Ok(op)
    }
}

// Enum removed, using Box<dyn Catalog>
async fn build_catalog(config: &AppConfig) -> Result<Box<dyn Catalog>> {
    match &config.catalog {
        CatalogConfig::Rest { uri, warehouse } => {
            let mut props = HashMap::new();
            props.insert("uri".to_string(), uri.to_string());
            if let Some(wh) = warehouse {
                props.insert("warehouse".to_string(), wh.to_string());
            }
            // Pass S3 credentials for FileIO used during commits
            props.insert("s3.endpoint".to_string(), config.s3.endpoint.clone());
            props.insert("s3.region".to_string(), config.s3.region.clone());
            if let Some(ref ak) = config.s3.access_key {
                props.insert("s3.access-key-id".to_string(), ak.clone());
            }
            if let Some(ref sk) = config.s3.secret_key {
                props.insert("s3.secret-access-key".to_string(), sk.clone());
            }
            // Allow HTTP for local MinIO
            props.insert("s3.allow-http".to_string(), "true".to_string());
            // Path-style access for MinIO
            props.insert("s3.path-style-access".to_string(), "true".to_string());
            // Disable credential chain loading
            props.insert("s3.disable-config-load".to_string(), "true".to_string());
            props.insert("s3.disable-ec2-metadata".to_string(), "true".to_string());

            let catalog = RestCatalogBuilder::default()
                .load("rest", props)
                .await
                .context("Failed to create REST catalog")?;
            Ok(Box::new(catalog))
        }
        CatalogConfig::Glue {
            warehouse,
            catalog_id,
            region,
        } => {
            let mut props = HashMap::new();
            props.insert("warehouse".to_string(), warehouse.to_string());
            if let Some(id) = catalog_id {
                props.insert("catalog_id".to_string(), id.to_string());
            }
            if let Some(r) = region {
                props.insert("region_name".to_string(), r.to_string());
            }
            let catalog = GlueCatalogBuilder::default()
                .load("glue", props)
                .await
                .context("Failed to create Glue catalog")?;
            Ok(Box::new(catalog))
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config_content =
        std::fs::read_to_string(&args.config).context("Failed to read config file")?;
    let config: AppConfig =
        serde_json::from_str(&config_content).context("Failed to parse config config")?;

    let operator = config.s3.build_operator()?;
    let catalog = build_catalog(&config).await?;

    let table_ident = TableIdent::new(
        NamespaceIdent::new(config.table.namespace.clone()),
        config.table.name.clone(),
    );

    // Load or Create Table
    // Try loading first to avoid HEAD request issues with some REST catalogs
    let table = match catalog.load_table(&table_ident).await {
        Ok(t) => {
            println!("Table {} loaded.", table_ident);
            t
        }
        Err(_) => {
            println!("Table {} does not exist, creating...", table_ident);
            let schema = schema::get_iceberg_schema();
            let partition_spec = schema::get_partition_spec(&schema);

            // Construct explicit table location to avoid double-slash issues
            let table_location = format!(
                "s3://{}/warehouse/{}/{}",
                config.s3.bucket, config.table.namespace, config.table.name
            );

            let table_creation = TableCreation::builder()
                .name(config.table.name.clone())
                .schema(schema)
                .partition_spec(partition_spec)
                .location(table_location)
                .build();

            catalog
                .create_table(
                    &NamespaceIdent::new(config.table.namespace.clone()),
                    table_creation,
                )
                .await?
        }
    };

    let table_location = table.metadata().location().to_string();
    let partition_spec = table.metadata().default_partition_spec().clone();

    // Generator Setup
    let generator = Arc::new(Generator::new(&config.generate.partitions));
    let operator = Arc::new(operator);

    // Calculate iterations
    let file_size_mb = config.generate.file_size_kb as f64 / 1024.0;
    let total_files = (config.generate.total_size_mb as f64 / file_size_mb).ceil() as usize;

    // Print configuration summary
    println!("=== Iceberg Data Generator ===");
    println!("Table: {}.{}", config.table.namespace, config.table.name);
    println!("Location: {}", table_location);
    println!("Bucket: {}", config.s3.bucket);
    println!("---");
    println!("Total size: {} MB", config.generate.total_size_mb);
    println!("File size: {} KB", config.generate.file_size_kb);
    println!("Total files: {}", total_files);
    println!("Parallelism: {}", config.generate.parallelism);
    println!("Compression: {}", config.generate.compression);
    println!("---");
    println!(
        "Partitions: {} aliases x {} workspaces x {} clusters = {} unique",
        config.generate.partitions.num_aliases,
        config.generate.partitions.workspaces_per_alias,
        config.generate.partitions.clusters_per_workspace,
        generator.num_partitions()
    );
    println!("==============================");
    println!();

    let start_time = std::time::Instant::now();
    let config = Arc::new(config);

    let semaphore = Arc::new(Semaphore::new(config.generate.parallelism));
    let mut tasks = Vec::new();

    // Data generation loop
    for _ in 0..total_files {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let generator = generator.clone();
        let operator = operator.clone();
        let table_location = table_location.clone();
        let partition_spec = partition_spec.clone();
        let bucket = config.s3.bucket.clone();
        let compression_str = config.generate.compression.clone();
        let file_size_kb = config.generate.file_size_kb;

        let task = tokio::spawn(async move {
            let row_size_est = 200; // estimated bytes per row
            let rows = (file_size_kb * 1024) / row_size_est;

            let batch = generator
                .generate_batch(rows)
                .expect("Failed to generate batch");

            // Map string to Compression
            let compression = match compression_str.as_str() {
                "zstd" => parquet::basic::Compression::ZSTD(Default::default()),
                "snappy" => parquet::basic::Compression::SNAPPY,
                _ => parquet::basic::Compression::UNCOMPRESSED,
            };

            let data_file = write_parquet_file(
                &operator,
                &table_location,
                &bucket,
                batch,
                &partition_spec,
                compression,
            )
            .await;

            drop(permit);
            data_file
        });
        tasks.push(task);
    }

    let mut data_files = Vec::new();
    let mut completed = 0;
    let progress_interval = (total_files / 10).max(1); // Report every 10%

    for task in tasks {
        match task.await.unwrap() {
            Ok(df) => {
                data_files.push(df);
                completed += 1;
                if completed % progress_interval == 0 || completed == total_files {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let rate = completed as f64 / elapsed;
                    let eta = if rate > 0.0 { (total_files - completed) as f64 / rate } else { 0.0 };
                    println!(
                        "Progress: {}/{} files ({:.1}%) - {:.1} files/sec - ETA: {:.0}s",
                        completed, total_files,
                        (completed as f64 / total_files as f64) * 100.0,
                        rate, eta
                    );
                }
            }
            Err(e) => eprintln!("Task failed: {:?}", e),
        }
    }

    let generation_time = start_time.elapsed();
    println!();
    println!("Generated {} files in {:.1}s. Committing...", data_files.len(), generation_time.as_secs_f64());

    if !data_files.is_empty() {
        let tx = Transaction::new(&table);
        let action = tx.fast_append().add_data_files(data_files);
        // Apply action returns Result<Transaction>.
        let tx = action.apply(tx)?;
        // Pass &*catalog (reference to dyn Catalog).
        // Hopefully Box<dyn Catalog> implements Catalog or &dyn Catalog implements it
        tx.commit(&*catalog).await?;
        println!("Commit successful!");
    } else {
        println!("No files generated.");
    }

    Ok(())
}
