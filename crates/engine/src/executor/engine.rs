//! Compaction engine with reusable DataFusion infrastructure.
use arrow::datatypes::SchemaRef;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use opendal::Operator;
use std::sync::Arc;
use thiserror::Error;
use url::Url;

use super::config::{CompactionConfig, ExecutorConfig, S3Config};
use super::writer::{RollingParquetWriter, WrittenFile};

/// Errors that can occur during compaction.
#[derive(Debug, Error)]
pub enum CompactionError {
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("OpenDAL error: {0}")]
    OpenDal(#[from] opendal::Error),

    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("Configuration error: {0}")]
    Config(String),
}

/// Compaction engine that holds reusable DataFusion infrastructure.
pub struct CompactionEngine {
    ctx: SessionContext,
    schema: SchemaRef,
    write_operator: Operator,
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

        Self::register_s3_object_store(&ctx, s3_config)?;

        let write_operator = Self::build_opendal_operator(s3_config)?;

        Ok(Self {
            ctx,
            schema,
            write_operator,
            config: compaction_config,
        })
    }

    fn register_s3_object_store(
        ctx: &SessionContext,
        s3_config: &S3Config,
    ) -> Result<(), CompactionError> {
        let store = AmazonS3Builder::new()
            .with_bucket_name(&s3_config.bucket)
            .with_region(&s3_config.region)
            .with_access_key_id(&s3_config.access_key)
            .with_secret_access_key(&s3_config.secret_key)
            .with_endpoint(&s3_config.endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()?;

        let url = Url::parse(&format!("s3://{}", s3_config.bucket))?;
        ctx.runtime_env()
            .register_object_store(&url, Arc::new(store));

        Ok(())
    }

    fn build_opendal_operator(s3_config: &S3Config) -> Result<Operator, CompactionError> {
        let builder = opendal::services::S3::default()
            .bucket(&s3_config.bucket)
            .region(&s3_config.region)
            .access_key_id(&s3_config.access_key)
            .secret_access_key(&s3_config.secret_key)
            .endpoint(&s3_config.endpoint)
            .disable_config_load()
            .disable_ec2_metadata();

        Ok(Operator::new(builder)?.finish())
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
            self.write_operator.clone(),
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
}
