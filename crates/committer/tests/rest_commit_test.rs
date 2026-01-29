//! Integration tests for REST catalog committer using testcontainers.
//!
//! These tests spin up MinIO + REST catalog containers and test the full commit flow.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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
        .with_wait_for(WaitFor::message_on_stderr("Started ServerConnector"))
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
async fn start_rest_catalog(
    minio_endpoint: &str,
) -> testcontainers::ContainerAsync<GenericImage> {
    rest_catalog_image()
        .with_env_var("CATALOG_WAREHOUSE", format!("s3://{}/", MINIO_BUCKET))
        .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env_var("CATALOG_S3_ENDPOINT", minio_endpoint)
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
    let client = reqwest::Client::new();

    // MinIO admin API to create bucket
    // In practice, use aws-sdk-s3 or similar
    let _ = client
        .put(&format!("{}/{}", minio_endpoint, MINIO_BUCKET))
        .send()
        .await;
}

#[tokio::test]
async fn test_containers_start() {
    // Basic smoke test - just verify containers can start
    let minio = start_minio().await;
    let minio_port = minio.get_host_port_ipv4(9000).await.unwrap();
    let minio_endpoint = format!("http://127.0.0.1:{}", minio_port);

    println!("MinIO started at: {}", minio_endpoint);

    // Give MinIO time to fully initialize
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Create bucket
    create_bucket(&minio_endpoint).await;

    // Start REST catalog
    let rest = start_rest_catalog(&minio_endpoint).await;
    let rest_port = rest.get_host_port_ipv4(8181).await.unwrap();
    let rest_endpoint = format!("http://127.0.0.1:{}", rest_port);

    println!("REST catalog started at: {}", rest_endpoint);

    // Give REST catalog time to initialize
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify REST catalog is responding
    let client = reqwest::Client::new();
    let response = client
        .get(&format!("{}/v1/config", rest_endpoint))
        .send()
        .await;

    assert!(response.is_ok(), "REST catalog should be reachable");

    println!("Containers started successfully!");
}

// TODO: Add full commit flow tests once containers are verified working
// These will:
// 1. Create a table via REST catalog API
// 2. Write test parquet files to MinIO
// 3. Use RestCommitter to commit the files
// 4. Verify the table metadata was updated correctly

#[tokio::test]
#[ignore = "requires docker and takes time to run"]
async fn test_rest_commit_fresh_table() {
    // TODO: Implement full commit test
    // 1. Start containers
    // 2. Create namespace and table
    // 3. Write test data files
    // 4. Commit with RestCommitter
    // 5. Verify snapshot created
}

#[tokio::test]
#[ignore = "requires docker and takes time to run"]
async fn test_rest_commit_incremental() {
    // TODO: Implement incremental commit test
    // 1. Start containers
    // 2. Create table with initial data
    // 3. Add more files via commit
    // 4. Verify both snapshots exist
}

#[tokio::test]
#[ignore = "requires docker and takes time to run"]
async fn test_rest_commit_partial_compaction() {
    // TODO: Implement partial compaction test
    // 1. Start containers
    // 2. Create table with multiple files across partitions
    // 3. Compact only some partitions
    // 4. Verify uncompacted files are still in table
}
