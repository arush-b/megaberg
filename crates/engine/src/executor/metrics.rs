//! Executor metrics for monitoring worker pool performance.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Metrics for monitoring executor performance.
///
/// All fields are atomic for lock-free concurrent updates from multiple workers.
#[derive(Debug)]
pub struct ExecutorMetrics {
    /// Total tasks submitted to the queue
    pub tasks_submitted: AtomicU64,
    /// Tasks that completed successfully
    pub tasks_completed: AtomicU64,
    /// Tasks that failed with an error
    pub tasks_failed: AtomicU64,
    /// Total task execution time in microseconds
    pub total_task_time_us: AtomicU64,
    /// Currently active workers (processing tasks)
    pub workers_active: AtomicUsize,
    /// Total worker count
    pub workers_total: AtomicUsize,
}

impl ExecutorMetrics {
    /// Create new metrics for a worker pool.
    pub fn new(worker_count: usize) -> Self {
        Self {
            tasks_submitted: AtomicU64::new(0),
            tasks_completed: AtomicU64::new(0),
            tasks_failed: AtomicU64::new(0),
            total_task_time_us: AtomicU64::new(0),
            workers_active: AtomicUsize::new(0),
            workers_total: AtomicUsize::new(worker_count),
        }
    }

    /// Get a point-in-time snapshot of metrics.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            tasks_submitted: self.tasks_submitted.load(Ordering::Relaxed),
            tasks_completed: self.tasks_completed.load(Ordering::Relaxed),
            tasks_failed: self.tasks_failed.load(Ordering::Relaxed),
            total_task_time_us: self.total_task_time_us.load(Ordering::Relaxed),
            workers_active: self.workers_active.load(Ordering::Relaxed),
            workers_total: self.workers_total.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time metrics snapshot.
///
/// This is a non-atomic copy for easy use in logging/reporting.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub tasks_submitted: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub total_task_time_us: u64,
    pub workers_active: usize,
    pub workers_total: usize,
}

impl MetricsSnapshot {
    /// Average task duration in seconds.
    pub fn avg_task_duration_secs(&self) -> f64 {
        let completed = self.tasks_completed + self.tasks_failed;
        if completed == 0 {
            0.0
        } else {
            (self.total_task_time_us as f64 / completed as f64) / 1_000_000.0
        }
    }

    /// Tasks still in progress (submitted but not completed/failed).
    pub fn in_progress(&self) -> u64 {
        self.tasks_submitted
            .saturating_sub(self.tasks_completed + self.tasks_failed)
    }

    /// Success rate as a percentage (0.0 - 100.0).
    pub fn success_rate(&self) -> f64 {
        let total = self.tasks_completed + self.tasks_failed;
        if total == 0 {
            100.0
        } else {
            (self.tasks_completed as f64 / total as f64) * 100.0
        }
    }
}
