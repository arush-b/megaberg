//! Generate test parquet files and commit to Iceberg table.
//!
//! Creates 20 parquet files (~30MB each) and commits them to an Iceberg table
//! via REST or Glue catalog.
//!
//! Usage: cargo run --example generate_data -- --config config.json
//!
//! Config JSON format:
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
//!     "name": "test_table"
//!   },
//!   "generate": {
//!     "num_files": 20,
//!     "rows_per_file": 500000
//!   }
//! }
//! ```

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use clap::Parser;
use futures::FutureExt;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use opendal::Operator;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use serde::Deserialize;
use uuid::Uuid;

const NUM_FILES: usize = 20;
const ROWS_PER_FILE: usize = 500_000; // ~30MB per file uncompressed

#[derive(Parser, Debug)]
#[command(name = "generate_data")]
#[command(about = "Generate test data for Iceberg table")]
struct Args {
    /// Path to config JSON file
    #[arg(short, long)]
    config: PathBuf,
}

/// S3 configuration
#[derive(Debug, Deserialize)]
struct S3Config {
    endpoint: String,
    #[serde(default)]
    access_key: Option<String>,
    #[serde(default)]
    secret_key: Option<String>,
    bucket: String,
    region: String,
    #[serde(default)]
    disable_config_load: bool,
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

/// Catalog configuration
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum CatalogConfigJson {
    Rest {
        uri: String,
        #[serde(default)]
        warehouse: Option<String>,
    },
    Glue {
        warehouse: String,
        #[serde(default)]
        catalog_id: Option<String>,
        #[serde(default)]
        region: Option<String>,
    },
}

/// Table configuration
#[derive(Debug, Deserialize)]
struct TableConfig {
    namespace: String,
    name: String,
}

/// Generate configuration
#[derive(Debug, Deserialize)]
struct GenerateConfig {
    #[serde(default = "default_num_files")]
    num_files: usize,
    #[serde(default = "default_rows_per_file")]
    rows_per_file: usize,
    #[serde(default = "default_compression")]
    compression: String,
}

fn default_num_files() -> usize {
    NUM_FILES
}
fn default_rows_per_file() -> usize {
    ROWS_PER_FILE
}
fn default_compression() -> String {
    "snappy".to_string()
}

impl Default for GenerateConfig {
    fn default() -> Self {
        Self {
            num_files: NUM_FILES,
            rows_per_file: ROWS_PER_FILE,
            compression: default_compression(),
        }
    }
}

impl GenerateConfig {
    fn get_compression(&self) -> Compression {
        match self.compression.to_lowercase().as_str() {
            "none" | "uncompressed" => Compression::UNCOMPRESSED,
            "snappy" => Compression::SNAPPY,
            "gzip" => Compression::GZIP(Default::default()),
            "zstd" => Compression::ZSTD(Default::default()),
            "lz4" => Compression::LZ4,
            _ => Compression::SNAPPY,
        }
    }
}

/// Top-level config
#[derive(Debug, Deserialize)]
struct Config {
    s3: S3Config,
    catalog: CatalogConfigJson,
    table: TableConfig,
    #[serde(default)]
    generate: Option<GenerateConfig>,
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]))
}

fn create_batch(schema: &SchemaRef, start_id: i64, num_rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (start_id..start_id + num_rows as i64).collect();
    let timestamps: Vec<i64> = ids.iter().map(|i| 1700000000000 + i * 1000).collect();
    let values: Vec<f64> = ids.iter().map(|i| (*i as f64) * 0.123).collect();
    let categories: Vec<String> = ids.iter().map(|i| format!("cat_{:03}", i % 100)).collect();
    let descriptions: Vec<String> = ids
        .iter()
        .map(|i| format!("Description for record {}", i))
        .collect();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(categories)),
            Arc::new(StringArray::from(descriptions)),
        ],
    )
    .unwrap()
}

struct OpendalWriter(opendal::Writer);

impl AsyncFileWriter for OpendalWriter {
    fn write(&mut self, bs: Bytes) -> futures::future::BoxFuture<'_, parquet::errors::Result<()>> {
        async move {
            self.0
                .write(bs)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
        }
        .boxed()
    }

    fn complete(&mut self) -> futures::future::BoxFuture<'_, parquet::errors::Result<()>> {
        async move {
            self.0
                .close()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))
        }
        .boxed()
    }
}

/// Write a test parquet file and return (path, file_size, record_count)
async fn write_test_file(
    operator: &Operator,
    path: &str,
    schema: &SchemaRef,
    start_id: i64,
    rows: usize,
    compression: Compression,
) -> Result<(String, u64, u64)> {
    let writer = operator
        .writer(path)
        .await
        .context("Failed to create writer")?;

    let props = WriterProperties::builder()
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_1_0)
        .set_compression(compression)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
        .build();

    let mut parquet_writer =
        AsyncArrowWriter::try_new(OpendalWriter(writer), schema.clone(), Some(props))
            .context("Failed to create parquet writer")?;

    let batch_size = 10_000;
    let mut written = 0;
    while written < rows {
        let batch = create_batch(
            schema,
            start_id + written as i64,
            batch_size.min(rows - written),
        );
        parquet_writer
            .write(&batch)
            .await
            .context("Failed to write batch")?;
        written += batch_size;
    }

    parquet_writer
        .close()
        .await
        .context("Failed to close parquet writer")?;

    // Get actual file size from storage (includes header, footer, and magic bytes)
    let stat = operator
        .stat(path)
        .await
        .context("Failed to stat written file")?;
    let file_size = stat.content_length();

    Ok((path.to_string(), file_size, rows as u64))
}

async fn build_rest_catalog(
    uri: &str,
    warehouse: Option<&str>,
    s3_config: &S3Config,
) -> Result<impl Catalog> {
    let mut props = HashMap::new();
    props.insert("uri".to_string(), uri.to_string());
    if let Some(wh) = warehouse {
        props.insert("warehouse".to_string(), wh.to_string());
    }

    // Pass S3 configuration for file I/O (iceberg uses uppercase property names)
    props.insert("s3.endpoint".to_string(), s3_config.endpoint.clone());
    props.insert("s3.region".to_string(), s3_config.region.clone());
    if let Some(ref ak) = s3_config.access_key {
        props.insert("s3.access-key-id".to_string(), ak.clone());
    }
    if let Some(ref sk) = s3_config.secret_key {
        props.insert("s3.secret-access-key".to_string(), sk.clone());
    }
    // Allow HTTP and path-style for local MinIO
    props.insert("s3.allow-http".to_string(), "true".to_string());
    props.insert("s3.path-style-access".to_string(), "true".to_string());

    // Disable credential chain loading to use only explicit credentials
    props.insert("s3.disable-config-load".to_string(), "true".to_string());

    RestCatalogBuilder::default()
        .load("rest", props)
        .await
        .context("Failed to create REST catalog")
}

async fn build_glue_catalog(
    warehouse: &str,
    catalog_id: Option<&str>,
    region: Option<&str>,
) -> Result<impl Catalog> {
    let mut props = HashMap::new();
    props.insert("warehouse".to_string(), warehouse.to_string());
    if let Some(id) = catalog_id {
        props.insert("catalog_id".to_string(), id.to_string());
    }
    if let Some(r) = region {
        props.insert("region_name".to_string(), r.to_string());
    }

    GlueCatalogBuilder::default()
        .load("glue", props)
        .await
        .context("Failed to create Glue catalog")
}

async fn commit_files<C: Catalog>(
    catalog: &C,
    table: &Table,
    files: Vec<(String, u64, u64)>,
    bucket: &str,
) -> Result<()> {
    // Build data files
    let mut data_files = Vec::new();
    for (path, file_size, record_count) in files {
        let full_path = format!("s3://{}/{}", bucket, path);

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(full_path)
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(file_size)
            .record_count(record_count)
            .partition(Struct::empty())
            .partition_spec_id(0) // Default unpartitioned spec
            .build()
            .context("Failed to build data file")?;

        data_files.push(data_file);
    }

    // Create transaction and append action
    let tx = Transaction::new(table);
    let append = tx.fast_append().add_data_files(data_files);

    // Apply the action to get back the transaction, then commit
    let tx = append.apply(tx)?;
    tx.commit(catalog)
        .await
        .context("Failed to commit transaction")?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("generate_data=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    // Load config
    println!("Loading config from: {}", args.config.display());
    let config_str = std::fs::read_to_string(&args.config)
        .with_context(|| format!("Failed to read config file: {}", args.config.display()))?;
    let config: Config =
        serde_json::from_str(&config_str).with_context(|| "Failed to parse config JSON")?;

    let generate_cfg = config.generate.unwrap_or_default();
    let num_files = generate_cfg.num_files;
    let rows_per_file = generate_cfg.rows_per_file;
    let compression = generate_cfg.get_compression();

    println!(
        "Generating {} files with {} rows each (~{:.0} MB total), compression: {:?}",
        num_files,
        rows_per_file,
        (num_files * rows_per_file * 50) as f64 / 1024.0 / 1024.0, // rough estimate
        compression
    );

    // Build S3 operator
    let operator = config.s3.build_operator()?;
    println!(
        "Connected to S3: {} (bucket: {})",
        config.s3.endpoint, config.s3.bucket
    );

    // Get table location from catalog
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(config.table.namespace.clone()),
        config.table.name.clone(),
    );

    let table_location: String;
    let schema = test_schema();

    // Write files based on catalog type
    match &config.catalog {
        CatalogConfigJson::Rest { uri, warehouse } => {
            println!("Using REST catalog: {}", uri);
            let catalog = build_rest_catalog(uri, warehouse.as_deref(), &config.s3).await?;
            let table = catalog.load_table(&table_ident).await?;
            table_location = table.metadata().location().to_string();
            println!("Table location: {}", table_location);

            // Generate and write files
            let mut files = Vec::new();
            let data_dir = format!(
                "{}/data",
                table_location.trim_start_matches(&format!("s3://{}/", config.s3.bucket)),
            );
            let file_uuid = Uuid::new_v4();

            for i in 0..num_files {
                let path = format!("{}/{}-data_{:04}.parquet", data_dir, file_uuid, i);
                let start_id = (i * rows_per_file) as i64;

                print!("  Writing file {}/{}: {}...", i + 1, num_files, path);
                let (written_path, size, count) =
                    write_test_file(&operator, &path, &schema, start_id, rows_per_file, compression).await?;
                println!(" {:.2} MB, {} rows", size as f64 / 1024.0 / 1024.0, count);

                files.push((written_path, size, count));
            }

            // Commit to catalog
            println!("\nCommitting {} files to catalog...", files.len());
            commit_files(&catalog, &table, files, &config.s3.bucket).await?;
            println!("Committed successfully!");
        }
        CatalogConfigJson::Glue {
            warehouse,
            catalog_id,
            region,
        } => {
            println!("Using Glue catalog (warehouse: {})", warehouse);
            let catalog =
                build_glue_catalog(warehouse, catalog_id.as_deref(), region.as_deref()).await?;

            // Try to load the table, create if it doesn't exist
            let table = match catalog.load_table(&table_ident).await {
                Ok(t) => t,
                Err(_) => {
                    println!("Table doesn't exist, creating...");
                    let iceberg_schema = iceberg::spec::Schema::builder()
                        .with_fields(vec![
                            iceberg::spec::NestedField::required(1, "id", iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long)).into(),
                            iceberg::spec::NestedField::required(2, "timestamp", iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long)).into(),
                            iceberg::spec::NestedField::required(3, "value", iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Double)).into(),
                            iceberg::spec::NestedField::required(4, "category", iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String)).into(),
                            iceberg::spec::NestedField::required(5, "description", iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::String)).into(),
                        ])
                        .build()
                        .context("Failed to build iceberg schema")?;

                    let table_creation = iceberg::TableCreation::builder()
                        .name(config.table.name.clone())
                        .schema(iceberg_schema)
                        .location(format!("{}/{}/{}", warehouse, config.table.namespace, config.table.name))
                        .build();

                    catalog
                        .create_table(&iceberg::NamespaceIdent::new(config.table.namespace.clone()), table_creation)
                        .await
                        .context("Failed to create table")?
                }
            };
            table_location = table.metadata().location().to_string();
            println!("Table location: {}", table_location);

            // Generate and write files
            let mut files = Vec::new();
            let data_dir = format!(
                "{}/data",
                table_location.trim_start_matches(&format!("s3://{}/", config.s3.bucket)),
            );
            let file_uuid = Uuid::new_v4();

            for i in 0..num_files {
                let path = format!("{}/{}-data_{:04}.parquet", data_dir, file_uuid, i);
                let start_id = (i * rows_per_file) as i64;

                print!("  Writing file {}/{}: {}...", i + 1, num_files, path);
                let (written_path, size, count) =
                    write_test_file(&operator, &path, &schema, start_id, rows_per_file, compression).await?;
                println!(" {:.2} MB, {} rows", size as f64 / 1024.0 / 1024.0, count);

                files.push((written_path, size, count));
            }

            // Commit to catalog
            println!("\nCommitting {} files to catalog...", files.len());
            commit_files(&catalog, &table, files, &config.s3.bucket).await?;
            println!("Committed successfully!");
        }
    }

    println!(
        "\nDone! Generated {} files for table {}.{}",
        num_files, config.table.namespace, config.table.name
    );

    Ok(())
}
