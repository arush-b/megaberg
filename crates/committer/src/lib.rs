//! Committer library for Iceberg table commits.

mod glue;
mod manifest;
mod snapshot;
mod types;

pub use glue::{GlueCommitter, GlueConfig};
pub use types::{CommitRequest, DataFileInfo, OutputFile, TableProvider, TaskResult};
