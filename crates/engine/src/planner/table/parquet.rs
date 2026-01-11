//! Parquet table source implementation.
//!
//! Discovers parquet files from S3 using prefix listing.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use opendal::Operator;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::reader::{FileReader, SerializedFileReader};
use tracing::{debug, info};

use super::source::TableSource;
use super::super::discovery::{FileInfo, PartitionFileMap, TableFormat};

/// Parquet table source - discovers files via S3 listing.
///
/// This provides a TableSource implementation for raw parquet files
/// stored in S3 with Hive-style partitioning (e.g., `year=2024/month=01/`).
pub struct ParquetSource {
    operator: Operator,
    base_path: String,
    bucket: String,
}

impl ParquetSource {
    /// Create a new ParquetSource.
    ///
    /// - `operator`: OpenDAL operator configured for the S3 bucket
    /// - `base_path`: Path prefix within the bucket (e.g., "data/events/")
    /// - `bucket`: S3 bucket name (for constructing full s3:// URLs)
    pub fn new(operator: Operator, base_path: String, bucket: String) -> Self {
        // Ensure base_path ends with /
        let base_path = if base_path.ends_with('/') {
            base_path
        } else {
            format!("{}/", base_path)
        };

        Self {
            operator,
            base_path,
            bucket,
        }
    }

    /// List partition prefixes using delimiter-based listing.
    /// Returns paths like "data/events/year=2024/month=01/"
    async fn list_partitions(&self) -> Result<Vec<String>> {
        let mut partitions = Vec::new();

        let entries: Vec<_> = self
            .operator
            .lister_with(&self.base_path)
            .recursive(false)
            .await
            .context("Failed to list partitions")?
            .try_collect()
            .await?;

        for entry in entries {
            let path = entry.path();
            // Only include directories (paths ending with /)
            if path.ends_with('/') && path != self.base_path {
                partitions.push(path.to_string());
            }
        }

        debug!(base = %self.base_path, count = partitions.len(), "Listed partitions");
        Ok(partitions)
    }

    /// Recursively list all partition prefixes (for nested partitions).
    /// E.g., year=2024/ -> year=2024/month=01/ -> year=2024/month=01/day=15/
    async fn list_leaf_partitions(&self) -> Result<Vec<String>> {
        let mut to_explore = vec![self.base_path.clone()];
        let mut leaf_partitions = Vec::new();

        while let Some(current) = to_explore.pop() {
            let entries: Vec<_> = self
                .operator
                .lister_with(&current)
                .recursive(false)
                .await?
                .try_collect()
                .await?;

            let mut has_files = false;

            for entry in entries {
                let path = entry.path();
                if path.ends_with('/') && path != current {
                    // It's a subdirectory
                    to_explore.push(path.to_string());
                } else if path.ends_with(".parquet") {
                    has_files = true;
                }
            }

            // If this directory has parquet files (and is not the base), it's a leaf
            if has_files && current != self.base_path {
                leaf_partitions.push(current);
            }
        }

        info!(count = leaf_partitions.len(), "Found leaf partitions");
        Ok(leaf_partitions)
    }

    /// List parquet files in a specific partition path.
    async fn list_files_in_partition(&self, partition_path: &str) -> Result<Vec<FileInfo>> {
        let mut files = Vec::new();

        let entries: Vec<_> = self
            .operator
            .lister_with(partition_path)
            .recursive(false)
            .await?
            .try_collect()
            .await?;

        for entry in entries {
            let path = entry.path();
            if path.ends_with(".parquet") {
                let meta = self
                    .operator
                    .stat(path)
                    .await
                    .with_context(|| format!("Failed to stat {}", path))?;

                files.push(FileInfo {
                    path: format!("s3://{}/{}", self.bucket, path),
                    size_bytes: meta.content_length(),
                    record_count: 0, // Unknown without reading footer
                });
            }
        }

        debug!(partition = %partition_path, files = files.len(), "Listed files in partition");
        Ok(files)
    }

    /// Extract partition key from full path.
    /// E.g., "data/events/year=2024/month=01/" -> "year=2024/month=01"
    fn extract_partition_key(&self, partition_path: &str) -> String {
        partition_path
            .strip_prefix(&self.base_path)
            .unwrap_or(partition_path)
            .trim_matches('/')
            .to_string()
    }
}

#[async_trait]
impl TableSource for ParquetSource {
    fn format(&self) -> TableFormat {
        TableFormat::Parquet
    }

    fn location(&self) -> &str {
        &self.base_path
    }

    async fn schema(&self) -> Result<SchemaRef> {
        // Find first parquet file and read its schema from footer
        let partitions = self.list_leaf_partitions().await?;

        for partition in partitions {
            let files = self.list_files_in_partition(&partition).await?;
            if let Some(first) = files.first() {
                // Convert s3://bucket/path to just path for opendal
                let path = first
                    .path
                    .strip_prefix(&format!("s3://{}/", self.bucket))
                    .unwrap_or(&first.path);

                let bytes = self
                    .operator
                    .read(path)
                    .await
                    .with_context(|| format!("Failed to read {}", path))?
                    .to_vec();

                let reader = SerializedFileReader::new(Bytes::from(bytes))
                    .context("Failed to create parquet reader")?;

                let schema = parquet_to_arrow_schema(
                    reader.metadata().file_metadata().schema_descr(),
                    None,
                )
                .context("Failed to convert parquet schema to arrow")?;

                return Ok(Arc::new(schema));
            }
        }

        Err(anyhow::anyhow!("No parquet files found to read schema from"))
    }

    async fn discover(&self) -> Result<PartitionFileMap> {
        let partitions = self.list_leaf_partitions().await?;
        let mut result = PartitionFileMap::new();

        for partition_path in partitions {
            let partition_key = self.extract_partition_key(&partition_path);
            let files = self.list_files_in_partition(&partition_path).await?;

            if !files.is_empty() {
                result.insert(partition_key, files);
            }
        }

        let total_files: usize = result.values().map(|v| v.len()).sum();
        info!(
            partitions = result.len(),
            files = total_files,
            "Discovery complete"
        );

        Ok(result)
    }
}
