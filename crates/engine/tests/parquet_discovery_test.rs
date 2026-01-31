//! Integration test for ParquetSource discovery against MinIO.
//! Tests: create parquet files -> discovery -> packing -> task building.

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use engine::planner::table::ParquetSource;
use engine::planner::table::TableSource;
use engine::planner::{build_tasks, pack_files, PackerConfig};
use opendal::{services::S3, Operator};
use parquet::arrow::ArrowWriter;

/// Creates an OpenDAL operator configured for local MinIO.
fn create_minio_operator() -> Operator {
    let builder = S3::default()
        .endpoint("http://localhost:9000")
        .access_key_id("admin")
        .secret_access_key("password")
        .bucket("lakehouse")
        .region("us-east-1")
        .disable_config_load();

    Operator::new(builder)
        .expect("Failed to create S3 operator")
        .finish()
}

/// Creates a sample parquet file as bytes.
fn create_parquet_bytes(partition_id: i32, file_id: i32) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("partition_id", DataType::Int32, false),
        Field::new("file_id", DataType::Int32, false),
        Field::new("data", DataType::Utf8, false),
    ]));

    // Create 100 rows per file
    let ids: Vec<i32> = (0..100).collect();
    let partition_ids: Vec<i32> = vec![partition_id; 100];
    let file_ids: Vec<i32> = vec![file_id; 100];
    let data: Vec<String> = (0..100)
        .map(|i| format!("row_{}_{}_{}_{}", partition_id, file_id, i, "x".repeat(50)))
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(partition_ids)),
            Arc::new(Int32Array::from(file_ids)),
            Arc::new(StringArray::from(data)),
        ],
    )
    .expect("Failed to create record batch");

    let mut buf = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, schema, None).expect("Failed to create parquet writer");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");

    buf
}

/// Creates test parquet files in MinIO: 10 partitions × 100 files each.
async fn create_test_files(operator: &Operator) {
    println!("Creating 10 partitions × 100 files = 1000 parquet files...");

    for partition_id in 0..10 {
        let partition_path = format!(
            "test_parquet/data/year=2024/month=01/day={:02}",
            partition_id + 1
        );

        for file_id in 0..100 {
            let file_path = format!("{}/file_{:04}.parquet", partition_path, file_id);
            let parquet_bytes = create_parquet_bytes(partition_id, file_id);

            operator
                .write(&file_path, Bytes::from(parquet_bytes))
                .await
                .unwrap_or_else(|_| panic!("Failed to write {}", file_path));
        }

        println!(
            "  Created partition {}/10: {}",
            partition_id + 1,
            partition_path
        );
    }

    println!("Done creating files!");
}

#[tokio::test]
#[ignore = "requires local MinIO"]
async fn test_parquet_discovery_and_planning() {
    // Initialize tracing for debug output
    let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();

    // Create operator pointing to MinIO
    let operator = create_minio_operator();

    // Step 1: Create test parquet files in MinIO
    println!("\n=== Step 1: Creating Test Files in MinIO ===");
    create_test_files(&operator).await;

    // Step 2: Discover files from MinIO
    println!("\n=== Step 2: Discovery ===");
    let source = ParquetSource::new(
        operator,
        "test_parquet/data/".to_string(),
        "lakehouse".to_string(),
    );

    let partition_map = source
        .discover()
        .await
        .expect("Failed to discover parquet files");

    let partition_count = partition_map.len();
    let total_files: usize = partition_map.values().map(|v| v.len()).sum();
    println!(
        "Discovered: {} partitions, {} total files",
        partition_count, total_files
    );

    // Step 3: Pack files into groups
    println!("\n=== Step 3: Packing ===");
    let packer_config = PackerConfig {
        target_file_size: 256 * 1024 * 1024, // 256MB target
        min_files_per_partition: 2,
    };

    let file_groups = pack_files(partition_map, &packer_config);
    println!("Created {} file groups", file_groups.len());

    for (i, group) in file_groups.iter().enumerate() {
        println!(
            "  Group {}: partition='{}', {} files, {} bytes",
            i,
            group.partition,
            group.files.len(),
            group.total_bytes
        );
    }

    // Step 4: Build compaction tasks
    println!("\n=== Step 4: Task Building ===");
    let table_location = "s3://lakehouse/test_parquet";
    let tasks = build_tasks(file_groups, table_location);
    println!("Created {} compaction tasks", tasks.len());

    for task in &tasks {
        println!(
            "  Task {}: partition='{}', {} input files, output_prefix='{}'",
            &task.task_id[..8],
            task.partition,
            task.input_files.len(),
            task.output_prefix
        );
    }

    // Assertions
    assert_eq!(partition_count, 10, "Expected 10 partitions");
    assert_eq!(total_files, 1000, "Expected 1000 total files");
    assert_eq!(tasks.len(), 10, "Expected 10 compaction tasks");

    for task in &tasks {
        assert_eq!(
            task.input_files.len(),
            100,
            "Expected 100 input files per task"
        );
    }

    println!("\n=== Test Passed ===");
}
