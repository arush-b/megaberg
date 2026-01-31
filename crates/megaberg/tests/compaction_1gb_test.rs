//! Large-scale compaction test using existing config files.
//!
//! This test:
//! 1. Uses iceberg-data-gen with local_config.json to generate test data
//! 2. Uses megaberg with config_glue.json to compact the data
//! 3. Verifies row count is preserved after compaction
//!
//! Config files:
//! - crates/iceberg-data-gen/local_config.json (datagen config)
//! - config_glue.json (megaberg config for Glue catalog)
//!
//! Prerequisites:
//! - AWS credentials configured (for Glue catalog)
//! - Or docker-compose up for local testing
//!
//! Run with:
//!   cargo test -p megaberg --test compaction_1gb_test -- --include-ignored --nocapture

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;

use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;

/// Get workspace root directory
fn workspace_root() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(&manifest_dir)
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Get path to iceberg-data-gen local_config.json
fn datagen_config_path() -> PathBuf {
    workspace_root().join("crates/iceberg-data-gen/local_config.json")
}

/// Get path to megaberg config_glue.json
fn megaberg_glue_config_path() -> PathBuf {
    workspace_root().join("config_glue.json")
}

/// Get path to megaberg config_local.json (for local docker-compose)
fn megaberg_local_config_path() -> PathBuf {
    workspace_root().join("config_local.json")
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

    workspace_root()
        .join("target/release/megaberg")
        .to_string_lossy()
        .to_string()
}

/// Run iceberg-data-gen with the specified config file
fn run_datagen(config_path: &PathBuf) -> std::process::Output {
    println!("Running iceberg-data-gen with config: {}", config_path.display());

    let output = Command::new("cargo")
        .args([
            "run",
            "-p",
            "iceberg-data-gen",
            "--release",
            "--",
            "--config",
            &config_path.to_string_lossy(),
        ])
        .env("RUST_LOG", "iceberg_data_gen=info")
        .output()
        .expect("Failed to run iceberg-data-gen");

    println!(
        "datagen stdout:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
    println!(
        "datagen stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    output
}

/// Run megaberg with the specified config file
fn run_megaberg(binary_path: &str, config_path: &PathBuf) -> std::process::Output {
    println!("Running megaberg with config: {}", config_path.display());

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

/// Run a Spark SQL query via docker exec and return the row count
fn spark_query_count(namespace: &str, table_name: &str) -> Option<u64> {
    println!(
        "Running Spark SQL: SELECT COUNT(*) FROM {}.{}",
        namespace, table_name
    );

    let query = format!("SELECT COUNT(*) FROM {}.{}", namespace, table_name);
    let output = Command::new("docker")
        .args([
            "exec",
            "spark-iceberg",
            "spark-sql",
            "--conf",
            "spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog",
            "--conf",
            "spark.sql.catalog.rest.type=rest",
            "--conf",
            "spark.sql.catalog.rest.uri=http://rest:8181",
            "--conf",
            "spark.sql.catalog.rest.warehouse=s3://lakehouse/",
            "--conf",
            "spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
            "--conf",
            "spark.sql.catalog.rest.s3.endpoint=http://minio:9000",
            "--conf",
            "spark.sql.defaultCatalog=rest",
            "-e",
            &query,
        ])
        .output()
        .expect("Failed to run spark-sql");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("Spark SQL stdout:\n{}", stdout);
    if !stderr.is_empty() && !output.status.success() {
        println!("Spark SQL stderr:\n{}", stderr);
    }

    if !output.status.success() {
        println!("Spark SQL query failed");
        return None;
    }

    // Parse the count from output - typically the last non-empty line contains the count
    for line in stdout.lines().rev() {
        let trimmed = line.trim();
        if let Ok(count) = trimmed.parse::<u64>() {
            return Some(count);
        }
    }

    None
}

/// Check if Spark container is running
fn is_spark_available() -> bool {
    let output = Command::new("docker")
        .args(["ps", "--filter", "name=spark-iceberg", "--format", "{{.Names}}"])
        .output()
        .ok();

    match output {
        Some(o) => String::from_utf8_lossy(&o.stdout).contains("spark-iceberg"),
        None => false,
    }
}

/// Build Glue catalog client
async fn build_glue_catalog(region: &str, warehouse: &str) -> impl Catalog {
    let mut props = HashMap::new();
    props.insert("warehouse".to_string(), warehouse.to_string());
    props.insert("region".to_string(), region.to_string());

    GlueCatalogBuilder::default()
        .load("glue", props)
        .await
        .expect("Failed to create Glue catalog")
}

/// Build REST catalog client for local testing
async fn build_rest_catalog(uri: &str, warehouse: &str, s3_endpoint: &str) -> impl Catalog {
    let mut props = HashMap::new();
    props.insert("uri".to_string(), uri.to_string());
    props.insert("warehouse".to_string(), warehouse.to_string());
    props.insert("s3.endpoint".to_string(), s3_endpoint.to_string());
    props.insert("s3.region".to_string(), "us-east-1".to_string());
    props.insert("s3.access-key-id".to_string(), "admin".to_string());
    props.insert("s3.secret-access-key".to_string(), "password".to_string());
    props.insert("s3.allow-http".to_string(), "true".to_string());
    props.insert("s3.path-style-access".to_string(), "true".to_string());
    props.insert("s3.disable-config-load".to_string(), "true".to_string());
    props.insert("s3.disable-ec2-metadata".to_string(), "true".to_string());

    RestCatalogBuilder::default()
        .load("rest", props)
        .await
        .expect("Failed to create REST catalog")
}

/// Get file info from table manifest using Glue catalog
async fn get_table_files_glue(
    region: &str,
    warehouse: &str,
    namespace: &str,
    table_name: &str,
) -> Vec<(String, u64, u64)> {
    let catalog = build_glue_catalog(region, warehouse).await;
    get_table_files_from_catalog(&catalog, namespace, table_name).await
}

/// Get file info from table manifest using REST catalog
async fn get_table_files_rest(
    uri: &str,
    warehouse: &str,
    s3_endpoint: &str,
    namespace: &str,
    table_name: &str,
) -> Vec<(String, u64, u64)> {
    let catalog = build_rest_catalog(uri, warehouse, s3_endpoint).await;
    get_table_files_from_catalog(&catalog, namespace, table_name).await
}

/// Get file info from table manifest (path, size, record_count)
async fn get_table_files_from_catalog(
    catalog: &impl Catalog,
    namespace: &str,
    table_name: &str,
) -> Vec<(String, u64, u64)> {
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

/// Test using Glue catalog with config_glue.json
/// Requires AWS credentials and the table/namespace to exist
#[tokio::test]
#[ignore = "requires AWS credentials and Glue catalog"]
async fn test_compaction_glue_catalog() {
    println!("=== Glue Catalog Compaction Test ===");

    let datagen_config = datagen_config_path();
    let megaberg_config = megaberg_glue_config_path();

    println!("Datagen config: {}", datagen_config.display());
    println!("Megaberg config: {}", megaberg_config.display());

    // Values from config_glue.json
    let region = "us-east-1";
    let warehouse = "s3://laminar-dev/warehouse";
    let namespace = "laminar_iceberg";
    let table_name = "megaberg_1GB_test";

    // Build megaberg binary
    let binary_path = build_megaberg();
    println!("Built megaberg at: {}", binary_path);

    // Note: For Glue test, we assume data is already generated
    // or use a separate datagen config pointing to Glue
    println!("\n=== Checking existing table state ===");

    let files_before = get_table_files_glue(region, warehouse, namespace, table_name).await;
    println!("Files before compaction: {}", files_before.len());

    if files_before.is_empty() {
        println!("No files found. Please generate data first using iceberg-data-gen.");
        println!("Skipping compaction test.");
        return;
    }

    let total_rows_before: u64 = files_before.iter().map(|(_, _, r)| *r).sum();
    let total_size_before: u64 = files_before.iter().map(|(_, s, _)| *s).sum();
    println!(
        "Total size: {:.2} MB, Total rows: {}",
        total_size_before as f64 / 1024.0 / 1024.0,
        total_rows_before
    );

    // Run compaction
    println!("\n=== Running compaction ===");
    let output = run_megaberg(&binary_path, &megaberg_config);

    if !output.status.success() {
        panic!("megaberg failed with exit code: {:?}", output.status.code());
    }

    // Get files after compaction
    let files_after = get_table_files_glue(region, warehouse, namespace, table_name).await;
    println!("\nFiles after compaction: {}", files_after.len());

    let total_rows_after: u64 = files_after.iter().map(|(_, _, r)| *r).sum();
    let total_size_after: u64 = files_after.iter().map(|(_, s, _)| *s).sum();
    println!(
        "Total size: {:.2} MB, Total rows: {}",
        total_size_after as f64 / 1024.0 / 1024.0,
        total_rows_after
    );

    // Verify row count preserved
    assert_eq!(
        total_rows_before, total_rows_after,
        "Row count must be preserved ({} before vs {} after)",
        total_rows_before,
        total_rows_after
    );

    println!("\n=== Test Passed ===");
    println!(
        "Files: {} -> {}, Rows preserved: {}",
        files_before.len(),
        files_after.len(),
        total_rows_after
    );
}

/// Test using local docker-compose with local_config.json
/// Requires: docker-compose up -d minio mc rest spark-iceberg
#[tokio::test]
#[ignore = "requires docker-compose up"]
async fn test_compaction_local_with_configs() {
    println!("=== Local Compaction Test (using config files) ===");

    let datagen_config = datagen_config_path();
    println!("Datagen config: {}", datagen_config.display());

    // Check docker-compose is running
    let minio_ok = reqwest::Client::new()
        .get("http://localhost:9000/minio/health/live")
        .send()
        .await
        .is_ok();

    let rest_ok = reqwest::Client::new()
        .get("http://localhost:8181/v1/config")
        .send()
        .await
        .is_ok();

    if !minio_ok || !rest_ok {
        panic!(
            "Docker-compose services not running!\n\
             Please run: docker-compose up -d minio mc rest spark-iceberg\n\
             Then re-run this test."
        );
    }

    let spark_available = is_spark_available();
    if spark_available {
        println!("Spark container detected - will verify data via Spark SQL");
    } else {
        println!("Spark container not running - skipping Spark SQL verification");
    }

    // Values from local_config.json
    let rest_uri = "http://localhost:8181";
    let warehouse = "s3://lakehouse/warehouse";
    let s3_endpoint = "http://localhost:9000";
    let namespace = "default";
    let table_name = "megaberg_1gb"; // from local_config.json

    // Build megaberg
    let binary_path = build_megaberg();

    // Run datagen with the config file
    println!("\n=== Generating test data ===");
    let datagen_output = run_datagen(&datagen_config);

    if !datagen_output.status.success() {
        panic!("iceberg-data-gen failed");
    }

    // Get files before compaction (from manifest)
    let files_before =
        get_table_files_rest(rest_uri, warehouse, s3_endpoint, namespace, table_name).await;
    println!("\nFiles before compaction: {}", files_before.len());

    let total_rows_before: u64 = files_before.iter().map(|(_, _, r)| *r).sum();
    let total_size_before: u64 = files_before.iter().map(|(_, s, _)| *s).sum();
    println!(
        "Total size: {:.2} MB, Total rows (manifest): {}",
        total_size_before as f64 / 1024.0 / 1024.0,
        total_rows_before
    );

    // Query row count via Spark BEFORE compaction
    let spark_count_before = if spark_available {
        println!("\n=== Spark SQL verification BEFORE compaction ===");
        spark_query_count(namespace, table_name)
    } else {
        None
    };

    if let Some(count) = spark_count_before {
        println!("Spark row count BEFORE compaction: {}", count);
        assert_eq!(
            total_rows_before, count,
            "Spark count should match manifest count before compaction"
        );
    }

    // Use existing config_local.json
    let megaberg_config = megaberg_local_config_path();
    println!("Megaberg config: {}", megaberg_config.display());

    // Run compaction
    println!("\n=== Running compaction ===");
    let output = run_megaberg(&binary_path, &megaberg_config);

    if !output.status.success() {
        panic!("megaberg failed");
    }

    // Get files after compaction (from manifest)
    let files_after =
        get_table_files_rest(rest_uri, warehouse, s3_endpoint, namespace, table_name).await;
    println!("\nFiles after compaction: {}", files_after.len());

    let total_rows_after: u64 = files_after.iter().map(|(_, _, r)| *r).sum();
    println!("Total rows after (manifest): {}", total_rows_after);

    // Query row count via Spark AFTER compaction
    let spark_count_after = if spark_available {
        println!("\n=== Spark SQL verification AFTER compaction ===");
        spark_query_count(namespace, table_name)
    } else {
        None
    };

    if let Some(count) = spark_count_after {
        println!("Spark row count AFTER compaction: {}", count);
        assert_eq!(
            total_rows_after, count,
            "Spark count should match manifest count after compaction"
        );
    }

    // Verify manifest row count preserved
    assert_eq!(
        total_rows_before, total_rows_after,
        "Row count must be preserved (manifest: {} before vs {} after)",
        total_rows_before,
        total_rows_after
    );

    // Verify Spark counts match if available
    if let (Some(before), Some(after)) = (spark_count_before, spark_count_after) {
        assert_eq!(
            before, after,
            "Spark row count must be preserved ({} before vs {} after)",
            before,
            after
        );
        println!(
            "\n=== Spark SQL Verification Passed ===\nRows verified via Spark: {} -> {}",
            before, after
        );
    }

    println!("\n=== Test Passed ===");
    println!(
        "Files: {} -> {}, Rows: {}",
        files_before.len(),
        files_after.len(),
        total_rows_after
    );
}

/// Partition spec info for a single data file
#[derive(Debug, Clone)]
struct PartitionInfo {
    /// Partition spec ID from the manifest file
    manifest_partition_spec_id: i32,
    /// Number of partition fields in the data file's partition struct
    num_partition_fields: usize,
    /// Whether any partition values are null
    has_null_partition_values: bool,
    /// Partition values as strings for comparison (e.g., "1,2,3" for alias_id=1, workspace_id=2, cluster_id=3)
    partition_values: String,
    /// File path for debugging
    file_path: String,
}

/// Get partition spec info from table manifest using REST catalog
/// Returns: (table_default_partition_spec_id, expected_num_partition_fields, vec of partition info per file)
async fn get_partition_info_rest(
    uri: &str,
    warehouse: &str,
    s3_endpoint: &str,
    namespace: &str,
    table_name: &str,
) -> (i32, usize, Vec<PartitionInfo>) {
    let catalog = build_rest_catalog(uri, warehouse, s3_endpoint).await;

    let table_ident = TableIdent::new(
        NamespaceIdent::new(namespace.to_string()),
        table_name.to_string(),
    );

    let table = catalog
        .load_table(&table_ident)
        .await
        .expect("Failed to load table");

    let metadata = table.metadata();
    let table_partition_spec_id = metadata.default_partition_spec_id();
    let expected_partition_fields = metadata.default_partition_spec().fields().len();

    let snapshot = match metadata.current_snapshot() {
        Some(s) => s,
        None => return (table_partition_spec_id, expected_partition_fields, vec![]),
    };

    let file_io = table.file_io();
    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .expect("Failed to load manifest list");

    let mut partition_infos = Vec::new();
    for manifest_entry in manifest_list.entries() {
        let manifest_partition_spec_id = manifest_entry.partition_spec_id;

        let manifest = manifest_entry
            .load_manifest(file_io)
            .await
            .expect("Failed to load manifest");

        for entry in manifest.entries() {
            if entry.status() == iceberg::spec::ManifestStatus::Deleted {
                continue;
            }
            let data_file = entry.data_file();
            let partition = data_file.partition();
            let fields = partition.fields();

            // Check if any partition field value is None (null)
            let has_null_values = fields.iter().any(|f| f.is_none());

            // Convert partition values to string for comparison
            let partition_values: String = fields
                .iter()
                .map(|f| match f {
                    Some(lit) => format!("{:?}", lit),
                    None => "NULL".to_string(),
                })
                .collect::<Vec<_>>()
                .join(",");

            partition_infos.push(PartitionInfo {
                manifest_partition_spec_id,
                num_partition_fields: fields.len(),
                has_null_partition_values: has_null_values,
                partition_values,
                file_path: data_file.file_path().to_string(),
            });
        }
    }

    (table_partition_spec_id, expected_partition_fields, partition_infos)
}

/// Get unique partition values from partition infos
fn get_unique_partitions(partition_infos: &[PartitionInfo]) -> std::collections::HashSet<String> {
    partition_infos
        .iter()
        .map(|info| info.partition_values.clone())
        .collect()
}

/// Verify partition info is valid
fn verify_partition_info(
    table_partition_spec_id: i32,
    expected_partition_fields: usize,
    partition_infos: &[PartitionInfo],
    stage: &str,
) {
    println!("\n=== Partition Spec Verification ({}) ===", stage);
    println!("Table default partition_spec_id: {}", table_partition_spec_id);
    println!("Expected partition fields: {}", expected_partition_fields);
    println!("Files to verify: {}", partition_infos.len());

    // Show unique partitions
    let unique_partitions = get_unique_partitions(partition_infos);
    println!("Unique partitions: {}", unique_partitions.len());
    for p in &unique_partitions {
        println!("  - {}", p);
    }

    let mut errors = Vec::new();

    for info in partition_infos {
        // Check 1: Manifest partition_spec_id matches table's default
        if info.manifest_partition_spec_id != table_partition_spec_id {
            errors.push(format!(
                "File {} has wrong partition_spec_id: {} (expected {})",
                info.file_path, info.manifest_partition_spec_id, table_partition_spec_id
            ));
        }

        // Check 2: Number of partition fields matches expected
        if info.num_partition_fields != expected_partition_fields {
            errors.push(format!(
                "File {} has wrong number of partition fields: {} (expected {})",
                info.file_path, info.num_partition_fields, expected_partition_fields
            ));
        }

        // Check 3: No null partition values
        if info.has_null_partition_values {
            errors.push(format!(
                "File {} has null partition values",
                info.file_path
            ));
        }
    }

    if errors.is_empty() {
        println!("All {} files have valid partition specs", partition_infos.len());
    } else {
        for error in &errors {
            println!("ERROR: {}", error);
        }
        panic!(
            "Partition spec verification failed ({}) with {} errors:\n{}",
            stage,
            errors.len(),
            errors.join("\n")
        );
    }
}

/// Test that compaction preserves partition specs correctly
/// This catches bugs where manifests are written with null partition specs or values
/// Requires: docker-compose up -d minio mc rest
#[tokio::test]
#[ignore = "requires docker-compose up"]
async fn test_compaction_preserves_partition_spec() {
    println!("=== Partition Spec Preservation Test ===");

    let datagen_config = datagen_config_path();
    println!("Datagen config: {}", datagen_config.display());

    // Check docker-compose is running
    let minio_ok = reqwest::Client::new()
        .get("http://localhost:9000/minio/health/live")
        .send()
        .await
        .is_ok();

    let rest_ok = reqwest::Client::new()
        .get("http://localhost:8181/v1/config")
        .send()
        .await
        .is_ok();

    if !minio_ok || !rest_ok {
        panic!(
            "Docker-compose services not running!\n\
             Please run: docker-compose up -d minio mc rest\n\
             Then re-run this test."
        );
    }

    // Values from local_config.json
    let rest_uri = "http://localhost:8181";
    let warehouse = "s3://lakehouse/warehouse";
    let s3_endpoint = "http://localhost:9000";
    let namespace = "default";
    let table_name = "megaberg_1gb";

    // Build megaberg
    let binary_path = build_megaberg();

    // Run datagen with the config file
    println!("\n=== Generating test data ===");
    let datagen_output = run_datagen(&datagen_config);

    if !datagen_output.status.success() {
        panic!("iceberg-data-gen failed");
    }

    // Get partition info BEFORE compaction
    let (spec_id_before, expected_fields, partition_infos_before) =
        get_partition_info_rest(rest_uri, warehouse, s3_endpoint, namespace, table_name).await;

    println!("\nFiles before compaction: {}", partition_infos_before.len());

    // Verify partition specs are valid before compaction
    verify_partition_info(
        spec_id_before,
        expected_fields,
        &partition_infos_before,
        "BEFORE compaction",
    );

    // Use existing config_local.json
    let megaberg_config = megaberg_local_config_path();
    println!("Megaberg config: {}", megaberg_config.display());

    // Run compaction
    println!("\n=== Running compaction ===");
    let output = run_megaberg(&binary_path, &megaberg_config);

    if !output.status.success() {
        panic!("megaberg failed");
    }

    // Get partition info AFTER compaction
    let (spec_id_after, expected_fields_after, partition_infos_after) =
        get_partition_info_rest(rest_uri, warehouse, s3_endpoint, namespace, table_name).await;

    println!("\nFiles after compaction: {}", partition_infos_after.len());

    // Verify table's partition_spec_id hasn't changed
    assert_eq!(
        spec_id_before, spec_id_after,
        "Table partition_spec_id changed after compaction ({} -> {})",
        spec_id_before, spec_id_after
    );

    // Verify expected partition fields count hasn't changed
    assert_eq!(
        expected_fields, expected_fields_after,
        "Expected partition fields count changed after compaction ({} -> {})",
        expected_fields, expected_fields_after
    );

    // Verify partition specs are valid after compaction
    verify_partition_info(
        spec_id_after,
        expected_fields_after,
        &partition_infos_after,
        "AFTER compaction",
    );

    // Verify all partitions are preserved after compaction
    println!("\n=== Verifying Partition Groups Preserved ===");
    let partitions_before = get_unique_partitions(&partition_infos_before);
    let partitions_after = get_unique_partitions(&partition_infos_after);

    // Check no partitions were lost
    let missing: Vec<_> = partitions_before
        .difference(&partitions_after)
        .collect();
    if !missing.is_empty() {
        panic!(
            "Partitions missing after compaction: {:?}",
            missing
        );
    }

    // Check no unexpected new partitions appeared
    let unexpected: Vec<_> = partitions_after
        .difference(&partitions_before)
        .collect();
    if !unexpected.is_empty() {
        panic!(
            "Unexpected new partitions after compaction: {:?}",
            unexpected
        );
    }

    // Verify same number of unique partitions
    assert_eq!(
        partitions_before.len(),
        partitions_after.len(),
        "Number of unique partitions changed after compaction ({} -> {})",
        partitions_before.len(),
        partitions_after.len()
    );

    println!(
        "All {} partition groups preserved after compaction",
        partitions_after.len()
    );

    println!("\n=== Partition Spec Preservation Test Passed ===");
    println!(
        "Verified {} files before and {} files after compaction",
        partition_infos_before.len(),
        partition_infos_after.len()
    );
    println!("partition_spec_id: {}", spec_id_after);
    println!("partition fields: {}", expected_fields_after);
    println!("partition groups preserved: {}", partitions_after.len());
}
