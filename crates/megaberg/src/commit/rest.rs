//! REST catalog commit implementation.

use anyhow::{Context, Result};
use iceberg::table::Table;
use iceberg::{TableIdent, TableRequirement, TableUpdate};
use serde::Serialize;

use super::manifest::{analyze_manifests, write_manifest, write_manifest_list};
use super::snapshot::{build_requirements, build_snapshot, build_summary, build_table_updates};
use super::types::{CommitContext, TaskResult};
use super::collect_files;

/// REST API commit request.
#[derive(Debug, Serialize)]
struct CommitTableRequest {
    identifier: TableIdent,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

/// Commits compaction results to an Iceberg table via REST catalog.
pub async fn commit(
    table: &Table,
    task_results: Vec<TaskResult>,
    s3_bucket: &str,
    rest_uri: &str,
    namespace: &str,
    table_name: &str,
) -> Result<()> {
    // Collect files from task results
    let (deleted_paths, added_files) = collect_files(task_results, s3_bucket);

    if deleted_paths.is_empty() && added_files.is_empty() {
        tracing::info!("No files to commit, skipping");
        return Ok(());
    }

    tracing::info!(
        deleted_files = deleted_paths.len(),
        added_files = added_files.len(),
        "Committing compaction results via REST"
    );

    let metadata = table.metadata();
    let file_io = table.file_io();

    // Create commit context
    let current_snapshot = metadata.current_snapshot();
    let parent_snapshot_id = current_snapshot.map(|s| s.snapshot_id());
    let current_sequence_number = current_snapshot.map(|s| s.sequence_number()).unwrap_or(0);
    let ctx = CommitContext::new(metadata.location(), parent_snapshot_id, current_sequence_number);

    // Analyze existing manifests
    let analysis = analyze_manifests(table, &deleted_paths).await?;

    // Write new manifest
    let new_manifest = write_manifest(
        file_io,
        &ctx,
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        metadata.default_partition_spec_id(),
        &added_files,
        &analysis.deleted_entries,
        s3_bucket,
    )
    .await?;

    // Write manifest list
    write_manifest_list(file_io, &ctx, new_manifest, analysis.existing_manifests).await?;

    // Build snapshot and commit request
    let summary_props = build_summary(&added_files, &analysis.deleted_entries);
    let snapshot = build_snapshot(&ctx, metadata.current_schema().schema_id(), summary_props);
    let updates = build_table_updates(snapshot, ctx.new_snapshot_id);
    let requirements = build_requirements(parent_snapshot_id);

    let request = CommitTableRequest {
        identifier: table.identifier().clone(),
        requirements,
        updates,
    };

    // Send commit request
    commit_via_rest(rest_uri, namespace, table_name, &request).await?;

    tracing::info!(
        added = added_files.len(),
        deleted = analysis.deleted_entries.len(),
        snapshot_id = ctx.new_snapshot_id,
        "Compaction committed successfully"
    );

    Ok(())
}

async fn commit_via_rest(
    rest_uri: &str,
    namespace: &str,
    table_name: &str,
    request: &CommitTableRequest,
) -> Result<()> {
    tracing::info!("Committing to catalog via REST API...");

    let client = reqwest::Client::new();
    let url = format!(
        "{}/v1/namespaces/{}/tables/{}",
        rest_uri.trim_end_matches('/'),
        namespace,
        table_name
    );

    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .json(request)
        .send()
        .await
        .context("Failed to send commit request")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Commit failed with status {}: {}", status, body);
    }

    Ok(())
}
