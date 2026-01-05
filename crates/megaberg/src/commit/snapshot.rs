//! Snapshot building utilities.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::{ManifestEntry, Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary, MAIN_BRANCH};
use iceberg::{TableRequirement, TableUpdate};

use super::types::{CommitContext, OutputFile};

/// Build a snapshot summary for compaction.
pub fn build_summary(
    added_files: &[OutputFile],
    deleted_entries: &[Arc<ManifestEntry>],
) -> HashMap<String, String> {
    let mut props = HashMap::new();

    props.insert("commit.operation".to_string(), "compaction".to_string());
    props.insert("added-data-files".to_string(), added_files.len().to_string());
    props.insert("deleted-data-files".to_string(), deleted_entries.len().to_string());
    props.insert(
        "added-records".to_string(),
        added_files.iter().map(|f| f.record_count).sum::<u64>().to_string(),
    );
    props.insert(
        "deleted-records".to_string(),
        deleted_entries.iter().map(|e| e.data_file.record_count()).sum::<u64>().to_string(),
    );

    props
}

/// Build a new snapshot for compaction.
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
            operation: Operation::Replace,
            additional_properties: summary_props,
        })
        .with_timestamp_ms(ctx.new_snapshot_id)
        .build()
}

/// Build table updates for adding a snapshot and setting the ref.
pub fn build_table_updates(snapshot: Snapshot, snapshot_id: i64) -> Vec<TableUpdate> {
    vec![
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ]
}

/// Build table requirements for the commit.
pub fn build_requirements(parent_snapshot_id: Option<i64>) -> Vec<TableRequirement> {
    vec![TableRequirement::RefSnapshotIdMatch {
        r#ref: MAIN_BRANCH.to_string(),
        snapshot_id: parent_snapshot_id,
    }]
}
