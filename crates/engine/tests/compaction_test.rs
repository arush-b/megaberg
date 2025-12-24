//! Integration test for compaction executor.
//!
//! Requires docker-compose up (minio on localhost:9000)
//!
//! Run with: cargo test --package engine --test compaction_test --release -- --nocapture
//!
//! EKS Resource Limits (1 worker):
//!   cpu: 1 core, memory: 2Gi

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use engine::executor::{
    add_field_ids_to_schema, CompactionConfig, CompactionEngine, S3Config, PARQUET_FIELD_ID_META_KEY,
};
use futures::FutureExt;
use opendal::Operator;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::FileReader;

const S3_ENDPOINT: &str = "http://localhost:9000";
const S3_ACCESS_KEY: &str = "admin";
const S3_SECRET_KEY: &str = "password";
const S3_BUCKET: &str = "warehouse";
const S3_REGION: &str = "us-east-1";

const NUM_FILES: usize = 6;
const ROWS_PER_FILE: usize = 500_000; // ~30MB per file uncompressed

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

fn build_operator() -> Operator {
    let builder = opendal::services::S3::default()
        .bucket(S3_BUCKET)
        .region(S3_REGION)
        .access_key_id(S3_ACCESS_KEY)
        .secret_access_key(S3_SECRET_KEY)
        .endpoint(S3_ENDPOINT)
        .disable_config_load()
        .disable_ec2_metadata();

    Operator::new(builder).unwrap().finish()
}

async fn write_test_file(
    operator: &Operator,
    path: &str,
    schema: &SchemaRef,
    start_id: i64,
    rows: usize,
) -> u64 {
    let writer = operator
        .writer_with(path)
        .chunk(8 * 1024 * 1024)
        .await
        .unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();

    let mut parquet_writer =
        AsyncArrowWriter::try_new(OpendalWriter(writer), schema.clone(), Some(props)).unwrap();

    let batch_size = 10_000;
    let mut written = 0;
    while written < rows {
        let batch = create_batch(
            schema,
            start_id + written as i64,
            batch_size.min(rows - written),
        );
        parquet_writer.write(&batch).await.unwrap();
        written += batch_size;
    }

    let metadata = parquet_writer.close().await.unwrap();
    metadata
        .row_groups
        .iter()
        .map(|rg| rg.total_byte_size as u64)
        .sum()
}

async fn cleanup(operator: &Operator, prefix: &str) {
    use futures::TryStreamExt;
    let mut lister = match operator.lister(prefix).await {
        Ok(l) => l,
        Err(_) => return,
    };
    while let Ok(Some(entry)) = lister.try_next().await {
        if entry.metadata().is_file() {
            let _ = operator.delete(entry.path()).await;
        }
    }
}

#[tokio::test]
async fn test_compaction() {
    let schema = test_schema();
    let operator = build_operator();

    let input_prefix = "test/compaction/input";
    let output_prefix = "test/compaction/output";

    // Cleanup
    println!("Cleaning up previous test data...");
    cleanup(&operator, "test/compaction/").await;

    // Write test files
    println!(
        "Writing {} test files ({} rows each)...",
        NUM_FILES, ROWS_PER_FILE
    );
    let mut input_files = Vec::new();
    let mut total_input_bytes = 0u64;

    for i in 0..NUM_FILES {
        let path = format!("{}/data_{:03}.parquet", input_prefix, i);
        let start_id = (i * ROWS_PER_FILE) as i64;
        let bytes = write_test_file(&operator, &path, &schema, start_id, ROWS_PER_FILE).await;
        println!(
            "  Written: {} ({:.2} MB)",
            path,
            bytes as f64 / 1024.0 / 1024.0
        );
        input_files.push(format!("s3://{}/{}", S3_BUCKET, path));
        total_input_bytes += bytes;
    }

    println!(
        "Total input: {:.2} MB across {} files",
        total_input_bytes as f64 / 1024.0 / 1024.0,
        NUM_FILES
    );

    // Setup compaction engine directly
    let s3_config = S3Config {
        endpoint: S3_ENDPOINT.to_string(),
        access_key: S3_ACCESS_KEY.to_string(),
        secret_key: S3_SECRET_KEY.to_string(),
        bucket: S3_BUCKET.to_string(),
        region: S3_REGION.to_string(),
    };
    let compaction_config = CompactionConfig {
        target_file_size_bytes: 256 * 1024 * 1024,
        row_group_size: 300_000,
        compression: Compression::UNCOMPRESSED,
        ..Default::default()
    };

    println!("\nCreating compaction engine...");
    let engine = CompactionEngine::with_s3(schema.clone(), &s3_config, compaction_config)
        .await
        .unwrap();

    // Run compaction task 1
    println!("Running compaction task 1...");
    let start1 = std::time::Instant::now();
    let result1 = engine
        .compact("test-task-001", input_files.clone(), output_prefix)
        .await;
    let duration1 = start1.elapsed();
    println!(
        "  Task 1: {:.2}s ({:.2} MB/s)",
        duration1.as_secs_f64(),
        total_input_bytes as f64 / 1024.0 / 1024.0 / duration1.as_secs_f64()
    );

    // Run compaction task 2 (same files, different output)
    println!("Running compaction task 2...");
    let start2 = std::time::Instant::now();
    let _result2 = engine
        .compact(
            "test-task-002",
            input_files.clone(),
            &format!("{}_2", output_prefix),
        )
        .await;
    let duration2 = start2.elapsed();
    println!(
        "  Task 2: {:.2}s ({:.2} MB/s)",
        duration2.as_secs_f64(),
        total_input_bytes as f64 / 1024.0 / 1024.0 / duration2.as_secs_f64()
    );

    let compaction_duration = duration1 + duration2;
    let result = result1;

    match result {
        Ok(written_files) => {
            println!("\nCompaction succeeded!");
            println!("Output files:");
            let mut total_output_bytes = 0u64;
            let mut total_output_rows = 0u64;
            for f in &written_files {
                println!(
                    "  {} ({:.2} MB, {} rows)",
                    f.path,
                    f.size_bytes as f64 / 1024.0 / 1024.0,
                    f.record_count
                );
                total_output_bytes += f.size_bytes;
                total_output_rows += f.record_count;
            }

            println!("\nSummary:");
            println!(
                "  Input:  {} files, {:.2} MB",
                NUM_FILES,
                total_input_bytes as f64 / 1024.0 / 1024.0
            );
            println!(
                "  Output: {} files, {:.2} MB",
                written_files.len(),
                total_output_bytes as f64 / 1024.0 / 1024.0
            );
            println!(
                "  Rows:   {} (expected {})",
                total_output_rows,
                NUM_FILES * ROWS_PER_FILE
            );
            println!("  Time:   {:.2}s", compaction_duration.as_secs_f64());
            println!(
                "  Throughput: {:.2} MB/s",
                total_input_bytes as f64 / 1024.0 / 1024.0 / compaction_duration.as_secs_f64()
            );

            // Verify row count
            assert_eq!(
                total_output_rows as usize,
                NUM_FILES * ROWS_PER_FILE,
                "Row count mismatch"
            );

            // Verify files exist in S3
            for f in &written_files {
                let exists = operator.exists(&f.path).await.unwrap();
                assert!(exists, "Output file not found: {}", f.path);
            }
            println!(
                "\nVerified {} output files exist in S3",
                written_files.len()
            );
        }
        Err(e) => {
            panic!("Compaction failed: {}", e);
        }
    }

    // Cleanup
    println!("\nCleaning up...");
    cleanup(&operator, "test/compaction/").await;
    println!("Done!");
}

/// Test that validates Iceberg field IDs and metadata extraction.
#[tokio::test]
async fn test_field_ids_and_metadata() {
    // Create schema with field IDs (Iceberg style)
    let base_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]);

    let (schema_with_ids, next_id) = add_field_ids_to_schema(&base_schema, 1);
    let schema: SchemaRef = Arc::new(schema_with_ids);

    println!("Schema with field IDs (next_id={}):", next_id);
    for (i, field) in schema.fields().iter().enumerate() {
        let field_id = field.metadata().get(PARQUET_FIELD_ID_META_KEY);
        println!("  Field {}: {} -> field_id={:?}", i, field.name(), field_id);
    }
    assert_eq!(next_id, 6, "Expected 5 fields with IDs 1-5, next_id should be 6");

    let operator = build_operator();

    let input_prefix = "test/metadata/input";
    let output_prefix = "test/metadata/output";

    // Cleanup
    println!("\nCleaning up previous test data...");
    cleanup(&operator, "test/metadata/").await;

    // Same dataset as test_compaction: 6 files × 500k rows (~30MB each)
    let num_files = 6;
    let rows_per_file = 500_000;

    println!(
        "Writing {} test files ({} rows each)...",
        num_files, rows_per_file
    );
    let mut input_files = Vec::new();

    for i in 0..num_files {
        let path = format!("{}/data_{:03}.parquet", input_prefix, i);
        let start_id = (i * rows_per_file) as i64;
        let bytes = write_test_file(&operator, &path, &schema, start_id, rows_per_file).await;
        println!(
            "  Written: {} ({:.2} MB)",
            path,
            bytes as f64 / 1024.0 / 1024.0
        );
        input_files.push(format!("s3://{}/{}", S3_BUCKET, path));
    }

    // Setup compaction engine
    let s3_config = S3Config {
        endpoint: S3_ENDPOINT.to_string(),
        access_key: S3_ACCESS_KEY.to_string(),
        secret_key: S3_SECRET_KEY.to_string(),
        bucket: S3_BUCKET.to_string(),
        region: S3_REGION.to_string(),
    };
    let compaction_config = CompactionConfig {
        target_file_size_bytes: 256 * 1024 * 1024,
        row_group_size: 300_000, // Same as test_compaction
        compression: Compression::UNCOMPRESSED,
        ..Default::default()
    };

    println!("\nCreating compaction engine...");
    let engine = CompactionEngine::with_s3(schema.clone(), &s3_config, compaction_config)
        .await
        .unwrap();

    // Run compaction task 1
    println!("Running compaction task 1...");
    let start1 = std::time::Instant::now();
    let result = engine
        .compact("test-metadata-001", input_files.clone(), output_prefix)
        .await;
    let duration1 = start1.elapsed();

    // Run compaction task 2 (same files, different output)
    println!("Running compaction task 2...");
    let start2 = std::time::Instant::now();
    let _result2 = engine
        .compact("test-metadata-002", input_files.clone(), &format!("{}_2", output_prefix))
        .await;
    let duration2 = start2.elapsed();

    match result {
        Ok(written_files) => {
            println!("\nCompaction succeeded!");
            assert!(!written_files.is_empty(), "Expected at least one output file");

            for (file_idx, f) in written_files.iter().enumerate() {
                println!("\n=== Output file {} ===", file_idx);
                println!("  Path: {}", f.path);
                println!("  Size: {:.2} MB", f.size_bytes as f64 / 1024.0 / 1024.0);
                println!("  Records: {}", f.record_count);

                // Validate split offsets
                println!("\n  Split offsets ({} row groups):", f.split_offsets.len());
                for (i, offset) in f.split_offsets.iter().enumerate() {
                    println!("    Row group {}: offset {}", i, offset);
                }
                // With 300k rows and 50k row group size, we expect multiple row groups
                assert!(
                    !f.split_offsets.is_empty(),
                    "Expected split offsets for multiple row groups"
                );

                // Validate column stats
                println!("\n  Column statistics:");

                // Check column_sizes (field IDs 1-5)
                println!("    column_sizes: {:?}", f.column_stats.column_sizes);
                for field_id in 1..=5 {
                    assert!(
                        f.column_stats.column_sizes.contains_key(&field_id),
                        "Missing column_size for field_id {}",
                        field_id
                    );
                    let size = f.column_stats.column_sizes.get(&field_id).unwrap();
                    println!("      field_id {}: {} bytes", field_id, size);
                }

                // Check value_counts
                println!("    value_counts: {:?}", f.column_stats.value_counts);
                for field_id in 1..=5 {
                    assert!(
                        f.column_stats.value_counts.contains_key(&field_id),
                        "Missing value_count for field_id {}",
                        field_id
                    );
                }

                // Check null_value_counts
                println!("    null_value_counts: {:?}", f.column_stats.null_value_counts);

                // Check lower_bounds
                println!("    lower_bounds (field_ids): {:?}", f.column_stats.lower_bounds.keys().collect::<Vec<_>>());
                assert!(
                    !f.column_stats.lower_bounds.is_empty(),
                    "Expected lower bounds to be populated"
                );

                // Check upper_bounds
                println!("    upper_bounds (field_ids): {:?}", f.column_stats.upper_bounds.keys().collect::<Vec<_>>());
                assert!(
                    !f.column_stats.upper_bounds.is_empty(),
                    "Expected upper bounds to be populated"
                );

                // Verify value counts match record count
                let id_value_count = f.column_stats.value_counts.get(&1).unwrap();
                assert_eq!(
                    *id_value_count, f.record_count,
                    "Value count for 'id' field should match record count"
                );
            }

            // Verify total rows
            let total_rows: u64 = written_files.iter().map(|f| f.record_count).sum();
            assert_eq!(
                total_rows as usize,
                num_files * rows_per_file,
                "Total row count mismatch"
            );
            // Calculate total size for throughput
            let total_bytes: u64 = written_files.iter().map(|f| f.size_bytes).sum();

            println!(
                "\nTotal rows: {} (expected {})",
                total_rows,
                num_files * rows_per_file
            );
            println!(
                "Task 1: {:.2}s ({:.2} MB/s)",
                duration1.as_secs_f64(),
                total_bytes as f64 / 1024.0 / 1024.0 / duration1.as_secs_f64()
            );
            println!(
                "Task 2: {:.2}s ({:.2} MB/s)",
                duration2.as_secs_f64(),
                total_bytes as f64 / 1024.0 / 1024.0 / duration2.as_secs_f64()
            );

            // Verify field IDs are actually written to Parquet file
            println!("\n--- Verifying Parquet file has field IDs ---");
            let first_file = &written_files[0];
            let file_bytes = operator.read(&first_file.path).await.unwrap();
            let parquet_reader = parquet::file::reader::SerializedFileReader::new(
                bytes::Bytes::from(file_bytes.to_vec()),
            )
            .unwrap();
            let parquet_schema = parquet_reader.metadata().file_metadata().schema_descr();

            println!("Parquet schema fields:");
            for (i, col) in parquet_schema.columns().iter().enumerate() {
                let basic_info = col.self_type().get_basic_info();
                let has_id = basic_info.has_id();
                println!(
                    "  Column {}: {} -> has_field_id={}, field_id={}",
                    i,
                    col.name(),
                    has_id,
                    if has_id { basic_info.id() } else { -1 }
                );
                assert!(
                    has_id,
                    "Field ID not written to Parquet for column {}",
                    col.name()
                );
                // Verify field IDs match expected (1-5)
                assert_eq!(
                    basic_info.id(),
                    (i + 1) as i32,
                    "Field ID mismatch for column {}",
                    col.name()
                );
            }

            println!("\n✓ All field ID and metadata validations passed!");
        }
        Err(e) => {
            panic!("Compaction failed: {}", e);
        }
    }

    // Cleanup
    println!("\nCleaning up...");
    cleanup(&operator, "test/metadata/").await;
    println!("Done!");
}
