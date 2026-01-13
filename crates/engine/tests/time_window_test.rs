//! Integration test for time-windowed partition discovery.
//!
//! Tests that the time window filtering correctly:
//! - Skips old partitions (before lookback window)
//! - Skips recent partitions (current hour based on skip_recent)
//! - Includes partitions within the window

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{Duration, Timelike, Utc};
use engine::planner::{
    LookbackWindow, ParquetSource, PartitionGranularity, PartitionTimeField, PartitionTimeSpec,
    TableSource, TimeComponent, TimeWindowConfig,
};
use opendal::Operator;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

fn create_minio_operator() -> Operator {
    let builder = opendal::services::S3::default()
        .bucket("lakehouse")
        .region("us-east-1")
        .endpoint("http://localhost:9000")
        .access_key_id("admin")
        .secret_access_key("password")
        .disable_config_load();

    Operator::new(builder).unwrap().finish()
}

fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
    ]))
}

fn create_test_parquet() -> Bytes {
    let schema = create_test_schema();

    let ids = Int64Array::from(vec![1, 2, 3]);
    let data = StringArray::from(vec!["a", "b", "c"]);

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(data)]).unwrap();

    let mut buf = Vec::new();
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    Bytes::from(buf)
}

#[tokio::test]
async fn test_time_window_filtering_with_minio() {
    // Setup
    let operator = create_minio_operator();
    let base_path = "time_window_test/";
    let now = Utc::now();

    // Clean up any existing test data
    let _ = operator.remove_all(base_path).await;

    // Create test parquet data
    let parquet_data = create_test_parquet();

    // Create partitions at different time offsets
    // We'll create partitions from 10 hours ago to current hour
    let offsets = vec![-10, -8, -6, -4, -3, -2, -1, 0];
    let mut created_partitions = Vec::new();

    println!("\n=== Creating test partitions ===");
    println!("Current time: {}", now.format("%Y-%m-%d %H:%M UTC"));

    for offset in &offsets {
        let ts = now + Duration::hours(*offset);
        let dt = ts.format("%Y-%m-%d").to_string();
        let hour = ts.hour();

        let partition_path = format!("{}dt={}/hour={:02}/", base_path, dt, hour);
        let file_path = format!("{}test.parquet", partition_path);

        operator.write(&file_path, parquet_data.clone()).await.unwrap();
        created_partitions.push((partition_path.clone(), *offset));
        println!("  Created: {} (offset: {}h)", partition_path, offset);
    }

    // Configure time window:
    // - Lookback: 5 hours (should skip -10, -8, -6)
    // - Skip recent: 1 (should skip hour 0 / current)
    // - Expected included: -4, -3, -2, -1
    let time_window_config = TimeWindowConfig {
        partition_spec: PartitionTimeSpec {
            fields: vec![
                PartitionTimeField {
                    name: "dt".to_string(),
                    format: "%Y-%m-%d".to_string(),
                    component: TimeComponent::Date,
                },
                PartitionTimeField {
                    name: "hour".to_string(),
                    format: "%H".to_string(),
                    component: TimeComponent::Hour,
                },
            ],
            timezone: "UTC".to_string(),
            granularity: PartitionGranularity::Hour,
        },
        lookback: LookbackWindow::Duration { hours: 5, minutes: 0 },
        skip_recent: 1,
    };

    // Create source with time window
    let source = ParquetSource::new(
        operator.clone(),
        base_path.to_string(),
        "lakehouse".to_string(),
    )
    .with_time_window(time_window_config);

    // Discover files with time filtering
    println!("\n=== Running discovery with time window ===");
    println!("  Lookback: 5 hours");
    println!("  Skip recent: 1 partition");

    let partition_map = source.discover().await.unwrap();

    println!("\n=== Results ===");
    println!("Discovered {} partitions:", partition_map.len());
    for (key, files) in &partition_map {
        println!("  {} -> {} files", key, files.len());
    }

    // Verify results
    println!("\n=== Verification ===");

    // With skip_recent=1 and hourly granularity:
    // - skip_after = truncate(now) - 1 hour
    // - So at 17:33, skip_after = 16:00
    // - Partitions from hour=16 onwards are "too recent"
    //
    // Expected:
    // - -10h, -8h, -6h: too old (before 5h lookback)
    // - -4h, -3h, -2h: included (within window)
    // - -1h: too recent (hour=16 >= skip_after=16:00)
    // - 0h: too recent (current hour)

    for (partition_path, offset) in &created_partitions {
        let key = partition_path
            .strip_prefix(base_path)
            .unwrap()
            .trim_end_matches('/');

        if *offset <= -6 {
            // Should be skipped (too old - before 5h lookback)
            assert!(
                !partition_map.contains_key(key),
                "Partition at offset {}h should be SKIPPED (too old): {}",
                offset,
                key
            );
            println!("  [CORRECT] {} (offset {}h) was skipped (too old)", key, offset);
        } else if *offset >= -1 {
            // Should be skipped (too recent - within skip_recent window)
            // -1h means hour=16 which is at 16:00, >= skip_after (16:00)
            assert!(
                !partition_map.contains_key(key),
                "Partition at offset {}h should be SKIPPED (too recent): {}",
                offset,
                key
            );
            println!("  [CORRECT] {} (offset {}h) was skipped (too recent)", key, offset);
        } else {
            // Should be included (-4, -3, -2)
            assert!(
                partition_map.contains_key(key),
                "Partition at offset {}h should be INCLUDED: {}",
                offset,
                key
            );
            println!("  [CORRECT] {} (offset {}h) was included", key, offset);
        }
    }

    // Cleanup
    let _ = operator.remove_all(base_path).await;

    println!("\n=== Test passed! ===");
}

#[tokio::test]
async fn test_parquet_source_without_time_window() {
    // Verify that without time window, all partitions are discovered
    let operator = create_minio_operator();
    let base_path = "no_time_window_test/";
    let now = Utc::now();

    // Clean up
    let _ = operator.remove_all(base_path).await;

    // Create test data
    let parquet_data = create_test_parquet();

    // Create a few partitions
    let offsets = vec![-5, -3, -1];
    for offset in &offsets {
        let ts = now + Duration::hours(*offset);
        let dt = ts.format("%Y-%m-%d").to_string();
        let hour = ts.hour();

        let file_path = format!("{}dt={}/hour={:02}/test.parquet", base_path, dt, hour);
        operator.write(&file_path, parquet_data.clone()).await.unwrap();
    }

    // Create source WITHOUT time window
    let source = ParquetSource::new(
        operator.clone(),
        base_path.to_string(),
        "lakehouse".to_string(),
    );

    let partition_map = source.discover().await.unwrap();

    // Should find all 3 partitions
    assert_eq!(
        partition_map.len(),
        3,
        "Without time window, should discover all {} partitions, found {}",
        offsets.len(),
        partition_map.len()
    );

    println!("Without time window: found {} partitions as expected", partition_map.len());

    // Cleanup
    let _ = operator.remove_all(base_path).await;
}
