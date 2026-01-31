//! Snapshot building utilities.

use std::collections::HashMap;

use iceberg::spec::{Operation, Snapshot, Summary};

use crate::types::{CommitContext, DataFileInfo};

/// Build a snapshot summary for the commit.
pub fn build_summary(added_files: &[DataFileInfo]) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("commit.operation".to_string(), "append".to_string());
    props.insert(
        "added-data-files".to_string(),
        added_files.len().to_string(),
    );
    props.insert(
        "added-records".to_string(),
        added_files
            .iter()
            .map(|f| f.record_count)
            .sum::<u64>()
            .to_string(),
    );
    props
}

/// Build a new snapshot.
pub fn build_snapshot(
    ctx: &CommitContext,
    schema_id: i32,
    summary_props: HashMap<String, String>,
) -> Snapshot {
    Snapshot::builder()
        .with_snapshot_id(ctx.new_snapshot_id)
        .with_parent_snapshot_id(ctx.parent_snapshot_id)
        .with_sequence_number(ctx.new_sequence_number)
        .with_schema_id(schema_id)
        .with_manifest_list(&ctx.manifest_list_path)
        .with_summary(Summary {
            operation: Operation::Append,
            additional_properties: summary_props,
        })
        .with_timestamp_ms(ctx.new_snapshot_id)
        .build()
}
