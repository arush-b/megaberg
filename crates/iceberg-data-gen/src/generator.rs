use anyhow::Result;
use arrow::array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use chrono::Utc;
use rand::distributions::{Alphanumeric, DistString};
use rand::Rng;
use std::sync::Arc;

use crate::config::PartitionConfig;

/// Pre-generated partition values for consistent cardinality
#[derive(Clone)]
pub struct PartitionValues {
    /// List of (alias, workspace_id, cluster_name) tuples
    pub combinations: Vec<(String, String, String)>,
}

impl PartitionValues {
    /// Generate all partition combinations based on config
    pub fn new(config: &PartitionConfig) -> Self {
        let mut rng = rand::thread_rng();
        let mut combinations = Vec::new();

        // Generate aliases
        let aliases: Vec<String> = (0..config.num_aliases)
            .map(|_| Alphanumeric.sample_string(&mut rng, 6))
            .collect();

        // For each alias, generate workspaces
        for alias in &aliases {
            let workspaces: Vec<String> = (0..config.workspaces_per_alias)
                .map(|_| Alphanumeric.sample_string(&mut rng, 8))
                .collect();

            // For each workspace, generate clusters
            for workspace in &workspaces {
                let clusters: Vec<String> = (0..config.clusters_per_workspace)
                    .map(|_| Alphanumeric.sample_string(&mut rng, 8))
                    .collect();

                for cluster in &clusters {
                    combinations.push((alias.clone(), workspace.clone(), cluster.clone()));
                }
            }
        }

        Self { combinations }
    }

    /// Get a random partition combination
    pub fn random_partition(&self) -> &(String, String, String) {
        let mut rng = rand::thread_rng();
        let idx = rng.gen_range(0..self.combinations.len());
        &self.combinations[idx]
    }

    /// Total number of unique partitions
    pub fn num_partitions(&self) -> usize {
        self.combinations.len()
    }
}

pub struct Generator {
    partition_values: PartitionValues,
}

impl Generator {
    pub fn new(partition_config: &PartitionConfig) -> Self {
        Self {
            partition_values: PartitionValues::new(partition_config),
        }
    }

    /// Get partition info for logging
    pub fn num_partitions(&self) -> usize {
        self.partition_values.num_partitions()
    }

    pub fn generate_batch(&self, rows: usize) -> Result<RecordBatch> {
        let mut rng = rand::thread_rng();

        // Get partition values for this batch
        let (alias, workspace_id, cluster_name) = self.partition_values.random_partition();

        // 1. Cluster Name (String) - from partition
        let cluster_arr = vec![cluster_name.clone(); rows];

        // 2. Query ID (String)
        let query_ids: Vec<String> = (0..rows)
            .map(|_| Alphanumeric.sample_string(&mut rng, 16))
            .collect();

        // 3. Catalog (String - Optional)
        let catalogs: Vec<Option<String>> = (0..rows)
            .map(|_| {
                let cats = ["hive", "iceberg", "delta", "glue"];
                Some(cats[rng.gen_range(0..cats.len())].to_string())
            })
            .collect();

        // 4. Query (String - Optional)
        let queries: Vec<Option<String>> = (0..rows)
            .map(|_| {
                let queries = [
                    "SELECT * FROM users WHERE id = 1",
                    "SELECT count(*) FROM orders",
                    "INSERT INTO logs VALUES (1, 'test')",
                    "UPDATE users SET name = 'John'",
                    "DELETE FROM temp_table",
                    "SELECT a.id, b.name FROM a JOIN b ON a.id = b.id",
                ];
                Some(queries[rng.gen_range(0..queries.len())].to_string())
            })
            .collect();

        // 5. End Time (Long - Optional)
        let end_times: Vec<Option<i64>> = (0..rows)
            .map(|_| Some(Utc::now().timestamp_millis()))
            .collect();

        // 6. Is Cached (Boolean - Optional)
        let is_cached: Vec<Option<bool>> = (0..rows).map(|_| Some(rng.gen_bool(0.5))).collect();

        // 7. Explain Analyse Output (String - Optional)
        let explains: Vec<Option<String>> = (0..rows)
            .map(|_| {
                Some(format!(
                    "Seq Scan on table (cost=0.00..{:.2} rows={} width=100)",
                    rng.gen_range(10.0..1000.0),
                    rng.gen_range(100..10000)
                ))
            })
            .collect();

        // 8. Execution Time (Double - Optional)
        let exec_times: Vec<Option<f64>> =
            (0..rows).map(|_| Some(rng.gen_range(0.1..100.0))).collect();

        // 9. Workspace ID (String) - from partition
        let ws_arr = vec![workspace_id.clone(); rows];

        // 10. Query Hash (String - Optional)
        let query_hashes: Vec<Option<String>> = (0..rows)
            .map(|_| Some(Alphanumeric.sample_string(&mut rng, 32)))
            .collect();

        // 11. Start Time (Timestamptz - Optional)
        // Use current time in microseconds for Iceberg timestamptz
        let now = Utc::now().timestamp_micros();
        let start_times: Vec<Option<i64>> = (0..rows).map(|_| Some(now)).collect();

        // 17. Alias (String) - from partition
        let alias_arr = vec![alias.clone(); rows];

        let schema = crate::schema::get_arrow_schema();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(cluster_arr)), // cluster_name (1)
                Arc::new(StringArray::from(query_ids)),   // query_id (2)
                Arc::new(StringArray::from(catalogs)),    // catalog (3)
                Arc::new(StringArray::from(queries)),     // query (4)
                Arc::new(Int64Array::from(end_times)),    // end_time (5)
                Arc::new(BooleanArray::from(is_cached)),  // is_cached (6)
                Arc::new(StringArray::from(explains)),    // explain_analyse_output (7)
                Arc::new(Float64Array::from(exec_times)), // execution_time (8)
                Arc::new(StringArray::from(ws_arr)),      // workspace_id (9)
                Arc::new(StringArray::from(query_hashes)), // query_hash (10)
                Arc::new(TimestampMicrosecondArray::from(start_times).with_timezone("UTC")), // start_time (11)
                Arc::new(StringArray::from(vec![
                    Some("default_db".to_string());
                    rows
                ])), // database (12)
                Arc::new(Float64Array::from(
                    (0..rows)
                        .map(|_| Some(rng.gen_range(10.0..500.0)))
                        .collect::<Vec<_>>(),
                )), // client_perceived_time (13)
                Arc::new(Float64Array::from(
                    (0..rows)
                        .map(|_| Some(rng.gen_range(0.1..5.0)))
                        .collect::<Vec<_>>(),
                )), // parsing_time (14)
                Arc::new(StringArray::from(vec![
                    Some(
                        Alphanumeric.sample_string(&mut rng, 32)
                    );
                    rows
                ])), // cluster_uuid (15)
                Arc::new(Float64Array::from(
                    (0..rows)
                        .map(|_| Some(rng.gen_range(0.0..10.0)))
                        .collect::<Vec<_>>(),
                )), // queueing_time (16)
                Arc::new(StringArray::from(alias_arr)), // alias (17)
                Arc::new(StringArray::from(
                    (0..rows)
                        .map(|_| Some(format!("user{}@example.com", rng.gen_range(1..100))))
                        .collect::<Vec<_>>(),
                )), // email (18)
                Arc::new(StringArray::from(
                    (0..rows)
                        .map(|_| {
                            let statuses = ["COMPLETED", "FAILED", "RUNNING", "QUEUED"];
                            Some(statuses[rng.gen_range(0..statuses.len())].to_string())
                        })
                        .collect::<Vec<_>>(),
                )), // status (19)
            ],
        )?;

        Ok(batch)
    }
}
