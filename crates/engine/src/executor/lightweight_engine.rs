//! Lightweight compaction engine using direct parquet reading without DataFusion.
//!
//! This engine has a much lower memory footprint compared to the DataFusion-based engine
//! by streaming records directly from parquet files without intermediate buffering.

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use common::S3Config;
use opendal::Operator;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use thiserror::Error;

use super::config::CompactionConfig;
use super::writer::{RollingParquetWriter, WrittenFile};

/// Errors that can occur during lightweight compaction.
#[derive(Debug, Error)]
pub enum LightweightCompactionError {
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("OpenDAL error: {0}")]
    OpenDal(#[from] opendal::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] common::StorageError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Configuration for the lightweight engine.
#[derive(Debug, Clone)]
pub struct LightweightConfig {
    /// Batch size for reading (number of rows per batch)
    pub batch_size: usize,
    /// Target output file size in bytes
    pub target_file_size_bytes: u64,
    /// Row group size for output files
    pub row_group_size: usize,
    /// Multipart upload chunk size
    pub multipart_chunk_bytes: usize,
    /// Compression codec
    pub compression: parquet::basic::Compression,
}

impl Default for LightweightConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            target_file_size_bytes: 256 * 1024 * 1024, // 256 MB
            row_group_size: 100_000,
            multipart_chunk_bytes: 8 * 1024 * 1024, // 8 MB (smaller than DataFusion version)
            compression: parquet::basic::Compression::UNCOMPRESSED,
        }
    }
}

impl From<&CompactionConfig> for LightweightConfig {
    fn from(cfg: &CompactionConfig) -> Self {
        Self {
            batch_size: cfg.batch_size,
            target_file_size_bytes: cfg.target_file_size_bytes,
            row_group_size: cfg.row_group_size,
            multipart_chunk_bytes: cfg.multipart_chunk_bytes.min(8 * 1024 * 1024), // Cap at 8MB
            compression: cfg.compression,
        }
    }
}

/// Lightweight compaction engine with minimal memory footprint.
///
/// This engine reads parquet files directly using the arrow parquet reader
/// and streams batches to the rolling writer without any intermediate buffering.
pub struct LightweightCompactionEngine {
    schema: SchemaRef,
    operator: Operator,
    config: LightweightConfig,
    s3_bucket: String,
}

impl LightweightCompactionEngine {
    /// Create a new lightweight compaction engine.
    pub fn new(
        schema: SchemaRef,
        operator: Operator,
        s3_bucket: String,
        config: LightweightConfig,
    ) -> Self {
        Self {
            schema,
            operator,
            config,
            s3_bucket,
        }
    }

    /// Create from S3 config.
    pub fn with_s3(
        schema: SchemaRef,
        s3_config: &S3Config,
        config: LightweightConfig,
    ) -> Result<Self, LightweightCompactionError> {
        let operator = s3_config.build_operator()?;
        Ok(Self::new(
            schema,
            operator,
            s3_config.bucket.clone(),
            config,
        ))
    }

    /// Execute a compaction task by reading input files and writing to output.
    ///
    /// This method processes files sequentially, streaming batches directly
    /// from reader to writer to minimize memory usage.
    pub async fn compact(
        &self,
        task_id: &str,
        input_files: Vec<String>,
        output_prefix: &str,
    ) -> Result<Vec<WrittenFile>, LightweightCompactionError> {
        tracing::debug!(
            %task_id,
            input_files = input_files.len(),
            "Starting lightweight compaction"
        );

        // Create compaction config for the writer
        let compaction_config = CompactionConfig {
            target_file_size_bytes: self.config.target_file_size_bytes,
            row_group_size: self.config.row_group_size,
            multipart_chunk_bytes: self.config.multipart_chunk_bytes,
            compression: self.config.compression,
            batch_size: self.config.batch_size,
            memory_limit_bytes: 0, // Not used by writer
        };

        let file_prefix = format!("compacted-{}", &task_id[..8.min(task_id.len())]);
        let mut writer = RollingParquetWriter::new(
            self.operator.clone(),
            self.schema.clone(),
            output_prefix.to_string(),
            file_prefix,
            &compaction_config,
        );

        let mut total_rows = 0u64;
        let s3_prefix = format!("s3://{}/", self.s3_bucket);

        // Process each input file sequentially
        for (i, input_path) in input_files.iter().enumerate() {
            // Strip s3://bucket/ prefix
            let relative_path = input_path
                .strip_prefix(&s3_prefix)
                .unwrap_or(input_path);

            tracing::trace!(
                file = i + 1,
                total = input_files.len(),
                path = %relative_path,
                "Reading input file"
            );

            // Read entire file into memory (parquet needs random access)
            let file_bytes = self.operator.read(relative_path).await?;
            let bytes = Bytes::from(file_bytes.to_vec());

            // Create parquet reader
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)?
                .with_batch_size(self.config.batch_size);

            let reader = builder.build()?;

            // Stream batches to writer
            for batch_result in reader {
                let batch = batch_result?;
                total_rows += batch.num_rows() as u64;
                writer.write_batch(&batch).await?;
            }
        }

        let written_files = writer.finish().await?;

        tracing::debug!(
            %task_id,
            input_rows = total_rows,
            output_files = written_files.len(),
            "Lightweight compaction complete"
        );

        Ok(written_files)
    }

    /// Get the schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Get the operator.
    pub fn operator(&self) -> &Operator {
        &self.operator
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lightweight_config_default() {
        let config = LightweightConfig::default();
        assert_eq!(config.batch_size, 8192);
        assert_eq!(config.target_file_size_bytes, 256 * 1024 * 1024);
        assert_eq!(config.multipart_chunk_bytes, 8 * 1024 * 1024);
    }
}
