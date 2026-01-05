//! Common utilities shared between planner and executor.

mod storage;

pub use storage::{build_opendal_operator, S3Config, StorageError};
