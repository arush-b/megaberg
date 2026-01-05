//! Compaction engine with reusable DataFusion infrastructure.
use arrow::datatypes::SchemaRef;
use common::S3Config;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use futures::StreamExt;
use object_store_opendal::OpendalStore;
use opendal::Operator;
use std::sync::Arc;
use thiserror::Error;
use url::Url;

use super::config::{CompactionConfig, ExecutorConfig};
use super::writer::{RollingParquetWriter, WrittenFile};

/// Errors that can occur during compaction.
#[derive(Debug, Error)]
pub enum CompactionError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("OpenDAL error: {0}")]
    OpenDal(#[from] opendal::Error),

    #[error("Storage error: {0}")]
    Storage(#[from] common::StorageError),

    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("Configuration error: {0}")]
    Config(String),
}

/// Compaction engine that holds reusable DataFusion infrastructure.
///
/// Uses a single OpenDAL operator for both reads (via DataFusion) and writes.
pub struct CompactionEngine {
    ctx: SessionContext,
    schema: SchemaRef,
    /// Single operator for all S3 operations (reads and writes)
    operator: Operator,
    /// Compaction configuration (reused)
    config: CompactionConfig,
}

impl CompactionEngine {
    /// Create a new compaction engine.
    pub async fn new(
        executor_config: &ExecutorConfig,
        schema: SchemaRef,
    ) -> Result<Self, CompactionError> {
        Self::with_s3(
            schema,
            &executor_config.s3,
            executor_config.compaction.clone(),
        )
        .await
    }

    pub async fn with_s3(
        schema: SchemaRef,
        s3_config: &S3Config,
        compaction_config: CompactionConfig,
    ) -> Result<Self, CompactionError> {
        let operator = s3_config.build_operator()?;
        Self::with_operator(schema, s3_config, compaction_config, operator).await
    }

    /// Create a compaction engine with an external OpenDAL operator.
    /// This allows sharing the operator with other components (e.g., discovery).
    pub async fn with_operator(
        schema: SchemaRef,
        s3_config: &S3Config,
        compaction_config: CompactionConfig,
        operator: Operator,
    ) -> Result<Self, CompactionError> {
        // Build SessionContext with memory pool
        let session_config = SessionConfig::new()
            .with_batch_size(compaction_config.batch_size)
            .with_target_partitions(1); // Single partition for sequential processing

        let memory_pool = Arc::new(FairSpillPool::new(compaction_config.memory_limit_bytes));
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(memory_pool)
            .build()
            .map_err(|e| CompactionError::Config(e.to_string()))?;

        let ctx = SessionContext::new_with_config_rt(session_config, Arc::new(runtime_env));

        // Register OpenDAL as object store for DataFusion reads
        let store = OpendalStore::new(operator.clone());
        let url = Url::parse(&format!("s3://{}", s3_config.bucket))?;
        ctx.runtime_env()
            .register_object_store(&url, Arc::new(store));

        Ok(Self {
            ctx,
            schema,
            operator,
            config: compaction_config,
        })
    }

    /// Execute a compaction task.
    pub async fn compact(
        &self,
        task_id: &str,
        input_files: Vec<String>,
        output_prefix: &str,
    ) -> Result<Vec<WrittenFile>, CompactionError> {
        tracing::debug!(
            %task_id,
            input_files = input_files.len(),
            "Starting compaction"
        );

        let df = self
            .ctx
            .read_parquet(
                input_files,
                ParquetReadOptions::default().schema(&self.schema),
            )
            .await?;

        let column_names: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let df = df.select_columns(&column_names)?;

        let file_prefix = format!("compacted-{}", &task_id[..8.min(task_id.len())]);
        let mut writer = RollingParquetWriter::new(
            self.operator.clone(),
            self.schema.clone(),
            output_prefix.to_string(),
            file_prefix,
            &self.config,
        );

        let mut stream = df.execute_stream().await?;
        let mut total_rows = 0u64;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            total_rows += batch.num_rows() as u64;
            writer.write_batch(&batch).await?;
        }

        let written_files = writer.finish().await?;

        tracing::debug!(
            %task_id,
            input_rows = total_rows,
            output_files = written_files.len(),
            "Compaction complete"
        );

        Ok(written_files)
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Get the OpenDAL operator for external use.
    pub fn operator(&self) -> &Operator {
        &self.operator
    }
}
