//! Integration tests for data generation and compaction using testcontainers.
//!
//! These tests spin up MinIO + REST catalog containers, generate data with mixed file sizes,
//! and verify the resulting manifest structure.
//!
//! Run these tests with: `cargo test -p iceberg-data-gen --test rest_compactor -- --ignored`

use std::time::Duration;

use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_data_gen::{
    AppConfig, CatalogConfig, FileGroup, GenerateConfig, PartitionConfig, S3Config, TableConfig,
};
use std::collections::HashMap;
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

/// REST catalog image (Apache Iceberg REST catalog)
fn rest_catalog_image() -> GenericImage {
    GenericImage::new("tabulario/iceberg-rest", "1.6.0")
        .with_exposed_port(ContainerPort::Tcp(8181))
        .with_wait_for(WaitFor::message_on_stdout("Started ServerConnector"))
}

/// Helper to create MinIO container with proper configuration
async fn start_minio() -> testcontainers::ContainerAsync<MinIO> {
    MinIO::default()
        .with_env_var("MINIO_ROOT_USER", MINIO_ACCESS_KEY)
        .with_env_var("MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY)
        .start()
        .await
        .expect("Failed to start MinIO container")
}

/// Helper to create REST catalog container linked to MinIO
/// Uses host.docker.internal to allow container-to-host communication
async fn start_rest_catalog(minio_port: u16) -> testcontainers::ContainerAsync<GenericImage> {
    // Use host.docker.internal for container to reach host's MinIO
    let minio_endpoint_for_container = format!("http://host.docker.internal:{}", minio_port);

    rest_catalog_image()
        .with_env_var("CATALOG_WAREHOUSE", format!("s3://{}/", MINIO_BUCKET))
        .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env_var("CATALOG_S3_ENDPOINT", &minio_endpoint_for_container)
        .with_env_var("CATALOG_S3_ACCESS__KEY__ID", MINIO_ACCESS_KEY)
        .with_env_var("CATALOG_S3_SECRET__ACCESS__KEY", MINIO_SECRET_KEY)
        .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .with_env_var("AWS_REGION", "us-east-1")
        .start()
        .await
        .expect("Failed to start REST catalog container")
}

/// Create the warehouse bucket in MinIO using AWS SDK
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
        Err(e) => {
            // Bucket might already exist, that's OK
            println!("Bucket creation result: {:?}", e);
        }
    }
}

/// Build a REST catalog for verification
async fn build_rest_catalog(rest_endpoint: &str, minio_endpoint: &str) -> Box<dyn Catalog> {
    let mut props = HashMap::new();
    props.insert("uri".to_string(), rest_endpoint.to_string());
    props.insert("warehouse".to_string(), format!("s3://{}/", MINIO_BUCKET));
    props.insert("s3.endpoint".to_string(), minio_endpoint.to_string());
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

    let catalog = RestCatalogBuilder::default()
        .load("rest", props)
        .await
        .expect("Failed to create REST catalog");
    Box::new(catalog)
}

/// Builder for test configuration
struct TestConfigBuilder {
    minio_endpoint: String,
    rest_endpoint: String,
    namespace: String,
    table_name: String,
    file_groups: Option<Vec<FileGroup>>,
    total_size_mb: usize,
    file_size_kb: usize,
    parallelism: usize,
    compression: String,
    partitions: PartitionConfig,
}

impl TestConfigBuilder {
    fn new(minio_endpoint: &str, rest_endpoint: &str) -> Self {
        Self {
            minio_endpoint: minio_endpoint.to_string(),
            rest_endpoint: rest_endpoint.to_string(),
            namespace: "megans".to_string(),
            table_name: "megatable".to_string(),
            file_groups: None,
            total_size_mb: 1,
            file_size_kb: 10,
            parallelism: 2,
            compression: "snappy".to_string(),
            partitions: PartitionConfig {
                num_aliases: 1,
                workspaces_per_alias: 1,
                clusters_per_workspace: 1,
            },
        }
    }

    fn namespace(mut self, ns: &str) -> Self {
        self.namespace = ns.to_string();
        self
    }

    fn table_name(mut self, name: &str) -> Self {
        self.table_name = name.to_string();
        self
    }

    fn file_groups(mut self, groups: Vec<FileGroup>) -> Self {
        self.file_groups = Some(groups);
        self
    }

    fn total_size_mb(mut self, size: usize) -> Self {
        self.total_size_mb = size;
        self
    }

    fn file_size_kb(mut self, size: usize) -> Self {
        self.file_size_kb = size;
        self
    }

    #[allow(dead_code)]
    fn parallelism(mut self, p: usize) -> Self {
        self.parallelism = p;
        self
    }

    #[allow(dead_code)]
    fn compression(mut self, c: &str) -> Self {
        self.compression = c.to_string();
        self
    }

    fn partitions(mut self, p: PartitionConfig) -> Self {
        self.partitions = p;
        self
    }

    fn build(self) -> AppConfig {
        AppConfig {
            s3: S3Config {
                endpoint: self.minio_endpoint,
                bucket: MINIO_BUCKET.to_string(),
                region: "us-east-1".to_string(),
                access_key: Some(MINIO_ACCESS_KEY.to_string()),
                secret_key: Some(MINIO_SECRET_KEY.to_string()),
                disable_config_load: true,
            },
            catalog: CatalogConfig::Rest {
                uri: self.rest_endpoint,
                warehouse: Some(format!("s3://{}/", MINIO_BUCKET)),
            },
            table: TableConfig {
                namespace: self.namespace,
                name: self.table_name,
            },
            generate: GenerateConfig {
                total_size_mb: self.total_size_mb,
                file_size_kb: self.file_size_kb,
                parallelism: self.parallelism,
                compression: self.compression,
                partitions: self.partitions,
                file_groups: self.file_groups,
            },
        }
    }
}

/// Helper struct holding container endpoints
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

    fn config_builder(&self) -> TestConfigBuilder {
        TestConfigBuilder::new(&self.minio_endpoint, &self.rest_endpoint)
    }

    async fn catalog(&self) -> Box<dyn Catalog> {
        build_rest_catalog(&self.rest_endpoint, &self.minio_endpoint).await
    }

    async fn create_namespace(&self, ns: &str) {
        let catalog = self.catalog().await;
        catalog
            .create_namespace(&NamespaceIdent::new(ns.to_string()), HashMap::new())
            .await
            .expect("Failed to create namespace");
    }
}

#[tokio::test]
#[ignore = "docker"]
async fn test_datagen_uniform_files() {
    let env = TestEnvironment::setup().await;
    env.create_namespace("megans").await;

    // Generate uniform files (legacy mode)
    let config = env
        .config_builder()
        .namespace("megans")
        .table_name("megatable_uniform")
        .total_size_mb(1)
        .file_size_kb(10)
        .build();

    let result = iceberg_data_gen::run_generation(config)
        .await
        .expect("Data generation failed");

    assert!(result.files_generated > 0, "Should have generated files");
    println!(
        "Generated {} files with {} total rows",
        result.files_generated, result.total_rows
    );

    // Verify table exists and has data
    let catalog = env.catalog().await;
    let table_ident = TableIdent::new(
        NamespaceIdent::new("megans".to_string()),
        "megatable_uniform".to_string(),
    );
    let table = catalog
        .load_table(&table_ident)
        .await
        .expect("Table should exist");

    let metadata = table.metadata();
    assert!(
        metadata.current_snapshot().is_some(),
        "Table should have a snapshot"
    );

    println!("Test passed: uniform file generation works");
}

#[tokio::test]
#[ignore = "docker"]
async fn test_datagen_mixed_file_sizes() {
    let env = TestEnvironment::setup().await;
    env.create_namespace("megans").await;

    // Generate mixed file sizes: 1 large file (100KB) + 5 small files (10KB)
    let config = env
        .config_builder()
        .namespace("megans")
        .table_name("megatable_mixed")
        .file_groups(vec![
            FileGroup {
                size_kb: 100,
                count: 1,
            },
            FileGroup {
                size_kb: 10,
                count: 5,
            },
        ])
        .build();

    let result = iceberg_data_gen::run_generation(config)
        .await
        .expect("Data generation failed");

    // Should have 6 files total (1 + 5)
    assert_eq!(
        result.files_generated, 6,
        "Should have generated 6 files (1 large + 5 small)"
    );
    println!(
        "Generated {} files with {} total rows",
        result.files_generated, result.total_rows
    );

    // Verify table and snapshot
    let catalog = env.catalog().await;
    let table_ident = TableIdent::new(
        NamespaceIdent::new("megans".to_string()),
        "megatable_mixed".to_string(),
    );
    let table = catalog
        .load_table(&table_ident)
        .await
        .expect("Table should exist");

    let metadata = table.metadata();
    let snapshot = metadata
        .current_snapshot()
        .expect("Table should have a snapshot");

    // Verify manifest list path exists
    let manifest_list = snapshot.manifest_list();
    println!("Manifest list path: {}", manifest_list);

    println!("Test passed: mixed file size generation works");
}

#[tokio::test]
#[ignore = "docker"]
async fn test_manifest_contains_mixed_sizes() {
    let env = TestEnvironment::setup().await;
    env.create_namespace("megans").await;

    // Generate with realistic size groups for compaction testing:
    // 1 large file at 256MB target (lands ~200MB uncompressed, above 128MB threshold)
    // 5 small files at 10KB each (candidates for compaction)
    // When engine is configured with target_file_size_mb=128, the large file
    // should be skipped/carried forward while small files get compacted.
    let config = env
        .config_builder()
        .namespace("megans")
        .table_name("megatable_manifest")
        .compression("none")
        .file_groups(vec![
            FileGroup {
                size_kb: 256 * 1024, // 256MB target
                count: 1,
            },
            FileGroup {
                size_kb: 10,
                count: 5,
            },
        ])
        .build();

    let result = iceberg_data_gen::run_generation(config)
        .await
        .expect("Data generation failed");

    assert_eq!(
        result.files_generated, 6,
        "Should have 6 files (1 large + 5 small)"
    );

    // Load table and inspect manifest
    let catalog = env.catalog().await;
    let table_ident = TableIdent::new(
        NamespaceIdent::new("megans".to_string()),
        "megatable_manifest".to_string(),
    );
    let table = catalog
        .load_table(&table_ident)
        .await
        .expect("Table should exist");

    let metadata = table.metadata();
    let snapshot = metadata.current_snapshot().expect("Should have snapshot");

    // Get manifests from the snapshot
    let file_io = table.file_io();
    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .expect("Failed to load manifest list");

    println!("Number of manifests: {}", manifest_list.entries().len());

    // Should be exactly 1 manifest (single commit)
    assert_eq!(
        manifest_list.entries().len(),
        1,
        "Single commit should produce single manifest"
    );

    // Load the manifest to inspect data files
    let manifest_entry = &manifest_list.entries()[0];
    let manifest = manifest_entry
        .load_manifest(file_io)
        .await
        .expect("Failed to load manifest");

    let entries = manifest.entries();
    println!("Number of data files in manifest: {}", entries.len());
    assert_eq!(entries.len(), 6, "Manifest should contain 6 data files");

    // Check file sizes - should have a mix
    let mut file_sizes: Vec<u64> = entries
        .iter()
        .map(|e| e.data_file().file_size_in_bytes())
        .collect();
    file_sizes.sort();

    println!("File sizes: {:?}", file_sizes);

    // The large file (128MB) should be much bigger than small files (10KB)
    // Expected ratio is ~13,000x, so checking for at least 1000x difference
    let largest = file_sizes.last().unwrap();
    let smallest = file_sizes.first().unwrap();

    println!(
        "Largest file: {} bytes ({:.2} MB), Smallest file: {} bytes ({:.2} KB)",
        largest,
        *largest as f64 / (1024.0 * 1024.0),
        smallest,
        *smallest as f64 / 1024.0
    );

    assert!(
        *largest > *smallest * 1000,
        "Large file should be at least 1000x bigger than small files (largest: {}, smallest: {})",
        largest,
        smallest
    );

    println!("Test passed: manifest contains 128MB file + 10KB files");
}

#[tokio::test]
#[ignore = "docker"]
async fn test_multiple_partitions() {
    let env = TestEnvironment::setup().await;
    env.create_namespace("megans").await;

    // Generate data across multiple partitions
    let config = env
        .config_builder()
        .namespace("megans")
        .table_name("megatable_partitions")
        .partitions(PartitionConfig {
            num_aliases: 2,
            workspaces_per_alias: 2,
            clusters_per_workspace: 1,
        })
        .file_groups(vec![FileGroup {
            size_kb: 10,
            count: 8,
        }])
        .build();

    let result = iceberg_data_gen::run_generation(config)
        .await
        .expect("Data generation failed");

    assert_eq!(result.files_generated, 8, "Should have 8 files");

    // Verify table
    let catalog = env.catalog().await;
    let table_ident = TableIdent::new(
        NamespaceIdent::new("megans".to_string()),
        "megatable_partitions".to_string(),
    );
    let table = catalog
        .load_table(&table_ident)
        .await
        .expect("Table should exist");

    assert!(
        table.metadata().current_snapshot().is_some(),
        "Should have snapshot"
    );

    println!("Test passed: multi-partition generation works");
}
