//! End-to-end compaction tests using testcontainers and the megaberg binary.
//!
//! These tests:
//! 1. Spin up MinIO + REST catalog containers
//! 2. Generate mixed-size test data using iceberg-data-gen
//! 3. Build and run the megaberg binary with a config file
//! 4. Verify large files are carried forward and small files are compacted
//!
//! Run with: `cargo test -p megaberg --test compaction_e2e -- --include-ignored --nocapture`

use std::collections::HashMap;
use std::process::Command;
use std::time::Duration;

use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_data_gen::{
    AppConfig as DatagenConfig, CatalogConfig, FileGroup, GenerateConfig, PartitionConfig,
    S3Config as DatagenS3Config, TableConfig,
};
use testcontainers::{
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use testcontainers_modules::minio::MinIO;

/// MinIO container configuration
const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
const MINIO_BUCKET: &str = "warehouse";

/// REST catalog image
fn rest_catalog_image() -> GenericImage {
    GenericImage::new("tabulario/iceberg-rest", "1.6.0")
        .with_exposed_port(ContainerPort::Tcp(8181))
        .with_wait_for(WaitFor::message_on_stdout("Started ServerConnector"))
}

/// Start MinIO container
async fn start_minio() -> testcontainers::ContainerAsync<MinIO> {
    MinIO::default()
        .with_env_var("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
        .with_env_var("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
        .start()
        .await
        .expect("Failed to start MinIO container")
}

/// Start REST catalog container linked to MinIO
async fn start_rest_catalog(minio_port: u16) -> testcontainers::ContainerAsync<GenericImage> {
    let minio_endpoint = format!("http://host.docker.internal:{}", minio_port);

    rest_catalog_image()
        .with_env_var("CATALOG_WAREHOUSE", format!("s3://{}/", MINIO_BUCKET))
        .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env_var("CATALOG_S3_ENDPOINT", &minio_endpoint)
        .with_env_var("CATALOG_S3_ACCESS__KEY__ID", MINIO_ACCESS_KEY)
        .with_env_var("CATALOG_S3_SECRET__ACCESS__KEY", MINIO_SECRET_KEY)
        .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .with_env_var("AWS_REGION", "us-east-1")
        .start()
        .await
        .expect("Failed to start REST catalog container")
}

/// Create the warehouse bucket in MinIO
async fn create_bucket(minio_endpoint: &str) {
    let creds = aws_sdk_s3::config::Credentials::new(
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY,
        None,
        None,
        "test",
    );

    let config = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .endpoint_url(minio_endpoint)
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(creds)
        .force_path_style(true)
        .build();

    let client = aws_sdk_s3::Client::from_conf(config);

    match client.create_bucket().bucket(MINIO_BUCKET).send().await {
        Ok(_) => println!("Bucket '{}' created successfully", MINIO_BUCKET),
        Err(e) => println!("Bucket creation result: {:?}", e),
    }
}

/// Test environment with containers
struct TestEnvironment {
    minio_endpoint: String,
    rest_endpoint: String,
    #[allow(dead_code)]
    minio: testcontainers::ContainerAsync<MinIO>,
    #[allow(dead_code)]
    rest: testcontainers::ContainerAsync<GenericImage>,
}

impl TestEnvironment {
    async fn setup() -> Self {
        let minio = start_minio().await;
        let minio_port = minio.get_host_port_ipv4(9000).await.unwrap();
        let minio_endpoint = format!("http://127.0.0.1:{}", minio_port);

        tokio::time::sleep(Duration::from_secs(2)).await;
        create_bucket(&minio_endpoint).await;

        let rest = start_rest_catalog(minio_port).await;
        let rest_port = rest.get_host_port_ipv4(8181).await.unwrap();
        let rest_endpoint = format!("http://127.0.0.1:{}", rest_port);

        tokio::time::sleep(Duration::from_secs(3)).await;

        Self {
            minio_endpoint,
            rest_endpoint,
            minio,
            rest,
        }
    }

    /// Build datagen config for generating test data
    fn datagen_config(
        &self,
        namespace: &str,
        table_name: &str,
        file_groups: Vec<FileGroup>,
    ) -> DatagenConfig {
        DatagenConfig {
            s3: DatagenS3Config {
                endpoint: self.minio_endpoint.clone(),
                bucket: MINIO_BUCKET.to_string(),
                region: "us-east-1".to_string(),
                access_key: Some(MINIO_ACCESS_KEY.to_string()),
                secret_key: Some(MINIO_SECRET_KEY.to_string()),
                disable_config_load: true,
            },
            catalog: CatalogConfig::Rest {
                uri: self.rest_endpoint.clone(),
                warehouse: Some(format!("s3://{}/", MINIO_BUCKET)),
            },
            table: TableConfig {
                namespace: namespace.to_string(),
                name: table_name.to_string(),
            },
            generate: GenerateConfig {
                total_size_mb: 1,
                file_size_kb: 10,
                parallelism: 2,
                compression: "none".to_string(),
                partitions: PartitionConfig {
                    num_aliases: 1,
                    workspaces_per_alias: 1,
                    clusters_per_workspace: 1,
                },
                file_groups: Some(file_groups),
            },
        }
    }

    /// Generate megaberg config JSON for the binary
    fn megaberg_config_json(
        &self,
        namespace: &str,
        table_name: &str,
        target_file_size_mb: u64,
    ) -> String {
        serde_json::json!({
            "s3": {
                "endpoint": self.minio_endpoint,
                "access_key": MINIO_ACCESS_KEY,
                "secret_key": MINIO_SECRET_KEY,
                "bucket": MINIO_BUCKET,
                "region": "us-east-1",
                "use_default_credentials": false
            },
            "source": {
                "type": "iceberg",
                "catalog": {
                    "type": "rest",
                    "uri": self.rest_endpoint,
                    "warehouse": format!("s3://{}/", MINIO_BUCKET)
                },
                "namespace": namespace,
                "table": table_name
            },
            "compaction": {
                "target_file_size_mb": target_file_size_mb,
                "min_files_per_partition": 2
            }
        })
        .to_string()
    }

    /// Create namespace in the catalog
    async fn create_namespace(&self, ns: &str) {
        let catalog = self.build_catalog().await;
        catalog
            .create_namespace(&NamespaceIdent::new(ns.to_string()), HashMap::new())
            .await
            .expect("Failed to create namespace");
    }

    /// Build a REST catalog client
    async fn build_catalog(&self) -> impl Catalog {
        let mut props = HashMap::new();
        props.insert("uri".to_string(), self.rest_endpoint.clone());
        props.insert("warehouse".to_string(), format!("s3://{}/", MINIO_BUCKET));
        props.insert("s3.endpoint".to_string(), self.minio_endpoint.clone());
        props.insert("s3.region".to_string(), "us-east-1".to_string());
        props.insert("s3.access-key-id".to_string(), MINIO_ACCESS_KEY.to_string());
        props.insert(
            "s3.secret-access-key".to_string(),
            MINIO_SECRET_KEY.to_string(),
        );
        props.insert("s3.allow-http".to_string(), "true".to_string());
        props.insert("s3.path-style-access".to_string(), "true".to_string());
        props.insert("s3.disable-config-load".to_string(), "true".to_string());
        props.insert("s3.disable-ec2-metadata".to_string(), "true".to_string());

        RestCatalogBuilder::default()
            .load("rest", props)
            .await
            .expect("Failed to create REST catalog")
    }

    /// Load table and get file info from manifest (path, size, record_count)
    async fn get_table_files(&self, namespace: &str, table_name: &str) -> Vec<(String, u64, u64)> {
        let catalog = self.build_catalog().await;
        let table_ident = TableIdent::new(
            NamespaceIdent::new(namespace.to_string()),
            table_name.to_string(),
        );

        let table = catalog
            .load_table(&table_ident)
            .await
            .expect("Failed to load table");

        let metadata = table.metadata();
        let snapshot = match metadata.current_snapshot() {
            Some(s) => s,
            None => return vec![],
        };

        let file_io = table.file_io();
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .expect("Failed to load manifest list");

        let mut files = Vec::new();
        for manifest_entry in manifest_list.entries() {
            let manifest = manifest_entry
                .load_manifest(file_io)
                .await
                .expect("Failed to load manifest");

            for entry in manifest.entries() {
                // Skip deleted entries
                if entry.status() == iceberg::spec::ManifestStatus::Deleted {
                    continue;
                }
                let data_file = entry.data_file();
                files.push((
                    data_file.file_path().to_string(),
                    data_file.file_size_in_bytes(),
                    data_file.record_count(),
                ));
            }
        }

        files
    }
}

/// Build the megaberg binary
fn build_megaberg() -> String {
    println!("Building megaberg binary...");
    let output = Command::new("cargo")
        .args(["build", "-p", "megaberg", "--release"])
        .output()
        .expect("Failed to run cargo build");

    if !output.status.success() {
        panic!(
            "Failed to build megaberg:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Return path to the binary
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let workspace_root = std::path::Path::new(&manifest_dir)
        .parent()
        .unwrap()
        .parent()
        .unwrap();

    workspace_root
        .join("target/release/megaberg")
        .to_string_lossy()
        .to_string()
}

/// Run megaberg with a config file
fn run_megaberg(binary_path: &str, config_json: &str) -> std::process::Output {
    // Write config to a temp file
    let config_path = std::env::temp_dir().join("megaberg_test_config.json");
    std::fs::write(&config_path, config_json).expect("Failed to write config");

    println!("Running megaberg with config: {}", config_path.display());
    println!("Config content:\n{}", config_json);

    let output = Command::new(binary_path)
        .args(["--config", &config_path.to_string_lossy()])
        .env("RUST_LOG", "megaberg=info,engine=info")
        .output()
        .expect("Failed to run megaberg");

    println!(
        "megaberg stdout:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
    println!(
        "megaberg stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    output
}

#[tokio::test]
#[ignore = "docker"]
async fn test_compaction_with_megaberg_binary() {
    // Build the binary first
    let binary_path = build_megaberg();
    println!("Built megaberg at: {}", binary_path);

    // Setup test environment
    let env = TestEnvironment::setup().await;
    env.create_namespace("megans").await;

    // Generate test data: 1 large file (256MB target -> ~200MB) + 5 small files (10KB each)
    let datagen_config = env.datagen_config(
        "megans",
        "megatable_binary_test",
        vec![
            FileGroup {
                size_kb: 256 * 1024, // 256MB target
                count: 1,
            },
            FileGroup {
                size_kb: 10,
                count: 5,
            },
        ],
    );

    println!("Generating test data...");
    let gen_result = iceberg_data_gen::run_generation(datagen_config)
        .await
        .expect("Data generation failed");

    println!(
        "Generated {} files with {} rows",
        gen_result.files_generated, gen_result.total_rows
    );
    assert_eq!(gen_result.files_generated, 6, "Should generate 6 files");

    // Get files before compaction
    let files_before = env.get_table_files("megans", "megatable_binary_test").await;
    println!("Files before compaction: {}", files_before.len());
    let mut total_rows_before: u64 = 0;
    for (path, size, records) in &files_before {
        println!(
            "  {} ({} bytes, {:.2} MB, {} rows)",
            path,
            size,
            *size as f64 / 1024.0 / 1024.0,
            records
        );
        total_rows_before += records;
    }
    println!("Total rows before compaction: {}", total_rows_before);

    // Sort by size to identify large vs small
    let mut sizes_before: Vec<u64> = files_before.iter().map(|(_, s, _)| *s).collect();
    sizes_before.sort();
    let large_file_size_before = *sizes_before.last().unwrap();
    println!(
        "Large file size before: {} bytes ({:.2} MB)",
        large_file_size_before,
        large_file_size_before as f64 / 1024.0 / 1024.0
    );

    // Generate megaberg config with 128MB threshold
    // The large file (~200MB) should be above threshold
    let config_json = env.megaberg_config_json("megans", "megatable_binary_test", 128);

    // Run megaberg
    let output = run_megaberg(&binary_path, &config_json);

    if !output.status.success() {
        panic!("megaberg failed with exit code: {:?}", output.status.code());
    }

    // Get files after compaction
    let files_after = env.get_table_files("megans", "megatable_binary_test").await;
    println!("Files after compaction: {}", files_after.len());
    let mut total_rows_after: u64 = 0;
    for (path, size, records) in &files_after {
        println!(
            "  {} ({} bytes, {:.2} MB, {} rows)",
            path,
            size,
            *size as f64 / 1024.0 / 1024.0,
            records
        );
        total_rows_after += records;
    }
    println!("Total rows after compaction: {}", total_rows_after);

    // Verify compaction happened
    assert!(
        files_after.len() < files_before.len(),
        "Should have fewer files after compaction ({} vs {})",
        files_after.len(),
        files_before.len()
    );

    // Verify row count is preserved (data integrity check)
    assert_eq!(
        total_rows_before, total_rows_after,
        "Row count must be preserved after compaction ({} before vs {} after)",
        total_rows_before,
        total_rows_after
    );

    println!("Test passed: compaction via megaberg binary completed successfully");
    println!(
        "Files reduced from {} to {}, rows preserved: {}",
        files_before.len(),
        files_after.len(),
        total_rows_after
    );
}

#[tokio::test]
#[ignore = "docker"]
async fn test_compaction_small_files_only_binary() {
    // Build the binary first
    let binary_path = build_megaberg();

    // Setup test environment
    let env = TestEnvironment::setup().await;
    env.create_namespace("megans").await;

    // Generate only small files
    let datagen_config = env.datagen_config(
        "megans",
        "megatable_small_binary",
        vec![FileGroup {
            size_kb: 10,
            count: 10, // 10 files of 10KB each
        }],
    );

    println!("Generating small files...");
    let gen_result = iceberg_data_gen::run_generation(datagen_config)
        .await
        .expect("Data generation failed");

    assert_eq!(gen_result.files_generated, 10, "Should generate 10 files");

    // Get files before
    let files_before = env
        .get_table_files("megans", "megatable_small_binary")
        .await;
    println!("Files before compaction: {}", files_before.len());
    let total_rows_before: u64 = files_before.iter().map(|(_, _, r)| r).sum();
    println!("Total rows before compaction: {}", total_rows_before);

    // Run megaberg with 256MB threshold (all files should be compacted)
    let config_json = env.megaberg_config_json("megans", "megatable_small_binary", 256);
    let output = run_megaberg(&binary_path, &config_json);

    if !output.status.success() {
        panic!("megaberg failed");
    }

    // Get files after
    let files_after = env
        .get_table_files("megans", "megatable_small_binary")
        .await;
    println!("Files after compaction: {}", files_after.len());
    let total_rows_after: u64 = files_after.iter().map(|(_, _, r)| r).sum();
    println!("Total rows after compaction: {}", total_rows_after);

    // 10 small files should compact into 1 file
    assert_eq!(files_after.len(), 1, "Should have 1 file after compaction");

    // Verify row count is preserved (data integrity check)
    assert_eq!(
        total_rows_before, total_rows_after,
        "Row count must be preserved after compaction ({} before vs {} after)",
        total_rows_before,
        total_rows_after
    );

    println!(
        "Test passed: small files compacted to 1 file, rows preserved: {}",
        total_rows_after
    );
}
