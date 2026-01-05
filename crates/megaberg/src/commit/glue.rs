//! Glue catalog commit implementation.

use anyhow::{Context, Result};
use iceberg::table::Table;

use super::manifest::{analyze_manifests, write_manifest, write_manifest_list};
use super::snapshot::{build_snapshot, build_summary, build_table_updates};
use super::types::{CommitContext, TaskResult};
use super::collect_files;

/// Commits compaction results to an Iceberg table in Glue catalog.
pub async fn commit(
    table: &Table,
    task_results: Vec<TaskResult>,
    s3_bucket: &str,
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
        "Committing compaction results to Glue"
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

    // Build snapshot and table updates
    let summary_props = build_summary(&added_files, &analysis.deleted_entries);
    let snapshot = build_snapshot(&ctx, metadata.current_schema().schema_id(), summary_props);
    let updates = build_table_updates(snapshot, ctx.new_snapshot_id);

    // Build new metadata by applying updates
    let mut metadata_builder = metadata.clone().into_builder(None);
    for update in &updates {
        metadata_builder = update
            .clone()
            .apply(metadata_builder)
            .context("Failed to apply update")?;
    }
    let new_metadata = metadata_builder
        .build()
        .context("Failed to build metadata")?
        .metadata;

    // Write new metadata file
    let new_metadata_location = format!(
        "{}/metadata/{:05}-{}.metadata.json",
        metadata.location(),
        ctx.new_sequence_number,
        ctx.commit_uuid
    );

    new_metadata
        .write_to(file_io, &new_metadata_location)
        .await
        .context("Failed to write metadata")?;

    tracing::debug!(path = %new_metadata_location, "Wrote new metadata file");

    // Update Glue table
    update_glue_table(table, &new_metadata_location).await?;

    tracing::info!(
        added = added_files.len(),
        deleted = analysis.deleted_entries.len(),
        snapshot_id = ctx.new_snapshot_id,
        "Compaction committed successfully to Glue"
    );

    Ok(())
}

/// Update Glue table's metadata_location via AWS SDK.
async fn update_glue_table(table: &Table, new_metadata_location: &str) -> Result<()> {
    tracing::info!("Updating Glue table metadata_location...");

    let ns = table.identifier().namespace().to_string();
    let name = table.identifier().name().to_string();

    let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let glue_client = aws_sdk_glue::Client::new(&aws_config);

    // Get current table to preserve other properties
    let current_glue_table = glue_client
        .get_table()
        .database_name(&ns)
        .name(&name)
        .send()
        .await
        .context("Failed to get current Glue table")?
        .table
        .context("Table not found in Glue")?;

    // Build updated table input with new metadata_location
    let mut parameters = current_glue_table.parameters().cloned().unwrap_or_default();
    parameters.insert("metadata_location".to_string(), new_metadata_location.to_string());
    parameters.insert(
        "previous_metadata_location".to_string(),
        table.metadata_location().unwrap_or("").to_string(),
    );

    let table_input = aws_sdk_glue::types::TableInput::builder()
        .name(&name)
        .set_storage_descriptor(current_glue_table.storage_descriptor().cloned())
        .set_parameters(Some(parameters))
        .table_type("EXTERNAL_TABLE")
        .build()
        .context("Failed to build table input")?;

    glue_client
        .update_table()
        .database_name(&ns)
        .table_input(table_input)
        .skip_archive(true)
        .send()
        .await
        .context("Failed to update Glue table")?;

    Ok(())
}
