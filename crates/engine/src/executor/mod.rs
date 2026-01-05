mod config;
mod engine;
mod lightweight_engine;
mod writer;

pub use config::{CompactionConfig, ExecutorConfig, S3Config};
pub use engine::{CompactionEngine, CompactionError};
pub use lightweight_engine::{LightweightCompactionEngine, LightweightCompactionError, LightweightConfig};
pub use writer::{add_field_ids_to_schema, ColumnStats, RollingParquetWriter, WrittenFile, PARQUET_FIELD_ID_META_KEY};

use arrow::datatypes::SchemaRef;
use tokio::sync::mpsc;

/// (task_id, input_files, output_prefix)
pub type Task = (String, Vec<String>, String);

/// (task_id, input_files, result)
pub type TaskResult = (String, Vec<String>, Result<Vec<WrittenFile>, String>);

pub struct Executor {
    config: ExecutorConfig,
    schema: SchemaRef,
}

impl Executor {
    pub fn new(config: ExecutorConfig, schema: SchemaRef) -> Self {
        Self { config, schema }
    }

    /// Spawns workers, returns (task_sender, result_receiver)
    pub async fn spawn(
        self,
    ) -> Result<
        (
            async_channel::Sender<Task>,
            mpsc::UnboundedReceiver<TaskResult>,
        ),
        CompactionError,
    > {
        let (task_tx, task_rx) = async_channel::bounded(self.config.worker_count * 2);
        let (result_tx, result_rx) = mpsc::unbounded_channel();

        for worker_id in 0..self.config.worker_count {
            let rx = task_rx.clone();
            let tx = result_tx.clone();
            let config = self.config.clone();
            let schema = self.schema.clone();

            tokio::spawn(async move {
                worker_loop(worker_id, rx, tx, config, schema).await;
            });
        }

        Ok((task_tx, result_rx))
    }
}

async fn worker_loop(
    worker_id: usize,
    task_rx: async_channel::Receiver<Task>,
    result_tx: mpsc::UnboundedSender<TaskResult>,
    config: ExecutorConfig,
    schema: SchemaRef,
) {
    // Create engine ONCE per worker
    let engine = match CompactionEngine::new(&config, schema).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(worker_id, error = %e, "Failed to create engine");
            return;
        }
    };

    tracing::info!(worker_id, "Worker started");

    while let Ok((task_id, input_files, output_prefix)) = task_rx.recv().await {
        let result = engine
            .compact(&task_id, input_files.clone(), &output_prefix)
            .await;

        let task_result = (task_id, input_files, result.map_err(|e| e.to_string()));

        if result_tx.send(task_result).is_err() {
            break;
        }
    }

    tracing::info!(worker_id, "Worker stopped");
}
