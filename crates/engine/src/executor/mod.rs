mod config;
mod engine;
mod metrics;
mod writer;

pub use config::{CompactionConfig, ExecutorConfig, S3Config, StorageError};
pub use engine::{CompactionEngine, CompactionError};
pub use metrics::{ExecutorMetrics, MetricsSnapshot};
pub use writer::{
    add_field_ids_to_schema, ColumnStats, RollingParquetWriter, WrittenFile,
    PARQUET_FIELD_ID_META_KEY,
};

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// (task_id, input_files, output_prefix)
pub type Task = (String, Vec<String>, String);

/// (task_id, input_files, result)
pub type TaskResult = (String, Vec<String>, Result<Vec<WrittenFile>, String>);

/// Errors that can occur when using the executor.
#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("Task queue is closed")]
    QueueClosed,

    #[error("Executor has been shut down")]
    ShutDown,

    #[error("Failed to create compaction engine: {0}")]
    EngineCreation(#[from] CompactionError),
}

/// Handle for controlling the executor worker pool.
///
/// Provides methods to submit tasks, check status, and gracefully shutdown.
/// The handle owns the worker pool - when dropped or shutdown, workers are stopped.
pub struct ExecutorHandle {
    /// Send tasks to workers
    task_tx: async_channel::Sender<Task>,
    /// Receive results from workers
    result_rx: mpsc::UnboundedReceiver<TaskResult>,
    /// Signal shutdown to workers
    shutdown_tx: watch::Sender<bool>,
    /// Worker join handles for cleanup
    worker_handles: Vec<JoinHandle<()>>,
    /// Shared metrics
    metrics: Arc<ExecutorMetrics>,
    /// Worker count for reference
    pub worker_count: usize,
}

impl ExecutorHandle {
    /// Submit a task for execution.
    ///
    /// This will block if the task queue is full (backpressure).
    pub async fn submit(&self, task: Task) -> Result<(), ExecutorError> {
        self.metrics.tasks_submitted.fetch_add(1, Ordering::Relaxed);
        self.task_tx
            .send(task)
            .await
            .map_err(|_| ExecutorError::QueueClosed)
    }

    /// Submit multiple tasks in batch.
    pub async fn submit_batch(&self, tasks: Vec<Task>) -> Result<(), ExecutorError> {
        for task in tasks {
            self.submit(task).await?;
        }
        Ok(())
    }

    /// Try to receive a result without blocking.
    pub fn try_recv(&mut self) -> Option<TaskResult> {
        self.result_rx.try_recv().ok()
    }

    /// Receive the next result (blocks until available).
    pub async fn recv(&mut self) -> Option<TaskResult> {
        self.result_rx.recv().await
    }

    /// Get reference to metrics.
    pub fn metrics(&self) -> &ExecutorMetrics {
        &self.metrics
    }

    /// Get current queue depth (approximate).
    pub fn queue_depth(&self) -> usize {
        self.task_tx.len()
    }

    /// Check if there are pending tasks in the queue or being processed.
    pub fn has_pending(&self) -> bool {
        let snapshot = self.metrics.snapshot();
        snapshot.in_progress() > 0 || self.queue_depth() > 0
    }

    /// Signal shutdown and wait for all workers to finish.
    ///
    /// Returns any results that were not yet received.
    pub async fn shutdown(mut self) -> Vec<TaskResult> {
        // 1. Close the task channel (no more submissions)
        self.task_tx.close();

        // 2. Signal workers to finish current work and exit
        let _ = self.shutdown_tx.send(true);

        // 3. Collect remaining results
        let mut results = Vec::new();
        while let Ok(result) = self.result_rx.try_recv() {
            results.push(result);
        }

        // 4. Wait for workers to finish (with timeout)
        let timeout = tokio::time::timeout(
            Duration::from_secs(300),
            futures::future::join_all(self.worker_handles),
        );

        match timeout.await {
            Ok(_) => tracing::info!("All workers shut down cleanly"),
            Err(_) => tracing::warn!("Shutdown timeout - some workers may still be running"),
        }

        // 5. Drain any final results
        while let Ok(result) = self.result_rx.try_recv() {
            results.push(result);
        }

        results
    }

    /// Shutdown and wait, returning collected results.
    pub async fn shutdown_and_collect(self) -> Vec<TaskResult> {
        self.shutdown().await
    }
}

pub struct Executor {
    config: ExecutorConfig,
    schema: SchemaRef,
}

impl Executor {
    pub fn new(config: ExecutorConfig, schema: SchemaRef) -> Self {
        Self { config, schema }
    }

    /// Spawn the worker pool and return a handle for control.
    ///
    /// This creates N workers where N = config.worker_count.
    /// Each worker creates its own CompactionEngine for isolation.
    pub async fn spawn(self) -> Result<ExecutorHandle, ExecutorError> {
        let worker_count = self.config.worker_count;

        // Bounded task queue: prevents unbounded memory growth
        // Size = workers * 2 gives enough buffer without excessive queuing
        let (task_tx, task_rx) = async_channel::bounded(worker_count * 2);

        // Unbounded result channel: results are small, workers shouldn't block
        let (result_tx, result_rx) = mpsc::unbounded_channel();

        // Shutdown signal via watch channel (efficient broadcast)
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Shared metrics
        let metrics = Arc::new(ExecutorMetrics::new(worker_count));

        // Spawn worker tasks
        let mut worker_handles = Vec::with_capacity(worker_count);

        for worker_id in 0..worker_count {
            let rx = task_rx.clone();
            let tx = result_tx.clone();
            let shutdown = shutdown_rx.clone();
            let config = self.config.clone();
            let schema = self.schema.clone();
            let worker_metrics = metrics.clone();

            let handle = tokio::spawn(async move {
                worker_loop(worker_id, rx, tx, shutdown, config, schema, worker_metrics).await;
            });

            worker_handles.push(handle);
        }

        tracing::info!(worker_count, "Executor worker pool started");

        Ok(ExecutorHandle {
            task_tx,
            result_rx,
            shutdown_tx,
            worker_handles,
            metrics,
            worker_count,
        })
    }

    /// Legacy spawn method for backward compatibility.
    ///
    /// Returns raw channels instead of ExecutorHandle.
    /// Note: This leaks worker handles - prefer spawn() for production.
    #[deprecated(since = "0.2.0", note = "Use spawn() which returns ExecutorHandle")]
    pub async fn spawn_legacy(
        self,
    ) -> Result<
        (
            async_channel::Sender<Task>,
            mpsc::UnboundedReceiver<TaskResult>,
        ),
        CompactionError,
    > {
        // Use the old implementation for true backward compatibility
        let (task_tx, task_rx) = async_channel::bounded(self.config.worker_count * 2);
        let (result_tx, result_rx) = mpsc::unbounded_channel();

        for worker_id in 0..self.config.worker_count {
            let rx = task_rx.clone();
            let tx = result_tx.clone();
            let config = self.config.clone();
            let schema = self.schema.clone();

            tokio::spawn(async move {
                legacy_worker_loop(worker_id, rx, tx, config, schema).await;
            });
        }

        Ok((task_tx, result_rx))
    }
}

/// Enhanced worker loop with shutdown signal and metrics.
async fn worker_loop(
    worker_id: usize,
    task_rx: async_channel::Receiver<Task>,
    result_tx: mpsc::UnboundedSender<TaskResult>,
    mut shutdown: watch::Receiver<bool>,
    config: ExecutorConfig,
    schema: SchemaRef,
    metrics: Arc<ExecutorMetrics>,
) {
    // Create engine ONCE per worker (reuse DataFusion context)
    let engine = match CompactionEngine::new(&config, schema).await {
        Ok(e) => e,
        Err(e) => {
            tracing::error!(worker_id, error = %e, "Failed to create engine");
            return;
        }
    };

    tracing::info!(worker_id, "Worker started");
    metrics.workers_active.fetch_add(1, Ordering::Relaxed);

    loop {
        tokio::select! {
            biased;

            // Check for shutdown signal first
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!(worker_id, "Shutdown signal received");
                    break;
                }
            }

            // Try to receive a task
            task = task_rx.recv() => {
                match task {
                    Ok((task_id, input_files, output_prefix)) => {
                        let start = std::time::Instant::now();
                        let input_count = input_files.len();

                        tracing::debug!(
                            worker_id,
                            %task_id,
                            input_files = input_count,
                            "Processing task"
                        );

                        let result = engine
                            .compact(&task_id, input_files.clone(), &output_prefix)
                            .await;

                        let duration = start.elapsed();

                        // Update metrics
                        if result.is_ok() {
                            metrics.tasks_completed.fetch_add(1, Ordering::Relaxed);
                        } else {
                            metrics.tasks_failed.fetch_add(1, Ordering::Relaxed);
                        }
                        metrics.total_task_time_us.fetch_add(
                            duration.as_micros() as u64,
                            Ordering::Relaxed
                        );

                        tracing::debug!(
                            worker_id,
                            %task_id,
                            duration_ms = duration.as_millis(),
                            success = result.is_ok(),
                            "Task completed"
                        );

                        let task_result = (task_id, input_files, result.map_err(|e| e.to_string()));

                        if result_tx.send(task_result).is_err() {
                            tracing::warn!(worker_id, "Result channel closed");
                            break;
                        }
                    }
                    Err(_) => {
                        // Channel closed - no more tasks
                        tracing::info!(worker_id, "Task channel closed");
                        break;
                    }
                }
            }
        }
    }

    metrics.workers_active.fetch_sub(1, Ordering::Relaxed);
    tracing::info!(worker_id, "Worker stopped");
}

/// Legacy worker loop without shutdown signal or metrics.
async fn legacy_worker_loop(
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
