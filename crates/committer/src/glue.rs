//! Glue catalog commit implementation.

use std::collections::HashSet;

use anyhow::{Context, Result};
use iceberg::table::Table;

use iceberg::spec::{SnapshotReference, SnapshotRetention, MAIN_BRANCH};
use iceberg::TableUpdate;

use crate::manifest::{
    filter_manifests, rewrite_manifest_keeping, write_delete_manifest, write_manifest,
    write_manifest_list,
};
use crate::snapshot::{build_snapshot, build_summary};
use crate::types::{CommitContext, DataFileInfo};

/// Configuration for Glue catalog.
pub struct GlueConfig {
    pub warehouse: String,
    pub region: Option<String>,
}

/// Glue catalog committer.
pub struct GlueCommitter {
    pub config: GlueConfig,
}

impl GlueCommitter {
    pub fn new(config: GlueConfig) -> Self {
        Self { config }
    }

    /// Takes a table reference from planner and commit to Glue
    /// Handles both added files and removed files.
    pub async fn commit_with_table(
        &self,
        table: &Table,
        added_files: Vec<DataFileInfo>,
        removed_files: Vec<DataFileInfo>,
        s3_bucket: &str,
    ) -> Result<i64> {
        if added_files.is_empty() && removed_files.is_empty() {
            tracing::info!("Nothing to commit, skipping commit.");
            return Ok(0);
        }

        // #TODO - p2 For raw parquet -> iceberg commits
        // add intelligent logic to determine the need for append/replace
        // for committing by looking at added_files + removed_files in the commit request

        tracing::info!(
            added = added_files.len(),
            removed = removed_files.len(),
            "Committing to Glue"
        );

        let metadata = table.metadata();

        // Create commit context
        let current_snapshot = metadata.current_snapshot();
        let parent_snapshot_id = current_snapshot.map(|s| s.snapshot_id());
        let current_seq = current_snapshot.map(|s| s.sequence_number()).unwrap_or(0);
        let ctx = CommitContext::new(metadata.location(), parent_snapshot_id, current_seq);

        let file_io = table.file_io();

        // Get existing manifests from current snapshot
        let existing_manifests = if let Some(snapshot) = current_snapshot {
            let manifest_list = snapshot
                .load_manifest_list(file_io, metadata)
                .await
                .context("Failed to load manifest list")?;
            manifest_list.entries().to_vec()
        } else {
            Vec::new()
        };

        let mut new_manifests = Vec::new();

        // #TODO - p2 A part of metadata compaction
        // Combine manifests together that fall in the similar partition
        // scheme.

        // Write manifest for added files
        if !added_files.is_empty() {
            let add_manifest = write_manifest(
                file_io,
                &ctx,
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                metadata.default_partition_spec_id(),
                &added_files,
                s3_bucket,
            )
            .await?;
            new_manifests.push(add_manifest);
        }

        // Write manifest for deleted files (marks original files as DELETED)
        if !removed_files.is_empty() {
            let delete_manifest = write_delete_manifest(
                file_io,
                &ctx,
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                metadata.default_partition_spec_id(),
                &removed_files,
                s3_bucket,
            )
            .await?;
            new_manifests.push(delete_manifest);
        }

        // Filter existing manifests for partial compaction support
        let filtered_existing = if !removed_files.is_empty() {
            let removed_paths: HashSet<String> =
                removed_files.iter().map(|f| f.path.clone()).collect();

            let filter_result = filter_manifests(file_io, existing_manifests, &removed_paths)
                .await
                .context("Failed to filter manifests")?;

            tracing::debug!(
                unaffected = filter_result.unaffected.len(),
                to_rewrite = filter_result.to_rewrite.len(),
                kept_files = filter_result.kept_file_count,
                removed_files = filter_result.removed_file_count,
                "Filtered existing manifests"
            );

            // Rewrite manifests that have a mix of kept and removed files
            let mut rewritten = Vec::new();
            for (idx, manifest) in filter_result.to_rewrite.iter().enumerate() {
                if let Some(rewritten_manifest) = rewrite_manifest_keeping(
                    file_io,
                    &ctx,
                    manifest,
                    &removed_paths,
                    metadata.current_schema().clone(),
                    metadata.default_partition_spec().clone(),
                    idx,
                )
                .await
                .context("Failed to rewrite manifest")?
                {
                    rewritten.push(rewritten_manifest);
                }
            }

            // Combine unaffected + rewritten manifests
            let mut result = filter_result.unaffected;
            result.extend(rewritten);
            result
        } else {
            existing_manifests
        };

        // Write manifest list with new manifests only (for compaction)
        write_manifest_list(file_io, &ctx, new_manifests, filtered_existing).await?;

        // Build snapshot summary

        // #TODO p0 - Add delete file summary to summary props
        let summary_props = build_summary(&added_files);
        let snapshot = build_snapshot(&ctx, metadata.current_schema().schema_id(), summary_props);

        // Build table updates
        let updates = vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    ctx.new_snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ];

        // Apply updates to build new metadata
        let mut builder = metadata.clone().into_builder(None);
        for update in &updates {
            builder = update
                .clone()
                .apply(builder)
                .context("Failed to apply update")?;
        }
        let new_metadata = builder
            .build()
            .context("Failed to build metadata")?
            .metadata;

        // Write metadata.json
        let metadata_path = format!(
            "{}/metadata/{:05}-{}.metadata.json",
            metadata.location(),
            ctx.new_sequence_number,
            ctx.commit_uuid
        );
        new_metadata
            .write_to(file_io, &metadata_path)
            .await
            .context("Failed to write metadata")?;

        tracing::debug!(path = %metadata_path, "Metadata written");

        // Update Glue table
        self.update_glue_table(table, &metadata_path).await?;

        tracing::info!(snapshot_id = ctx.new_snapshot_id, "Commit complete");
        Ok(ctx.new_snapshot_id)
    }

    /// Update Glue table's metadata_location.
    async fn update_glue_table(&self, table: &Table, new_metadata_location: &str) -> Result<()> {
        let ns = table.identifier().namespace().to_string();
        let name = table.identifier().name().to_string();

        let aws_config = match &self.config.region {
            Some(region) => {
                aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(aws_config::Region::new(region.clone()))
                    .load()
                    .await
            }
            None => aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await,
        };
        let glue_client = aws_sdk_glue::Client::new(&aws_config);

        // Get current table to preserve properties and version for OCC
        let get_response = glue_client
            .get_table()
            .database_name(&ns)
            .name(&name)
            .send()
            .await
            .context("Failed to get Glue table")?;
        let current = get_response.table.context("Table not found")?;
        let version_id = current.version_id.clone();

        // Update metadata_location
        let mut params = current.parameters().cloned().unwrap_or_default();
        params.insert(
            "metadata_location".to_string(),
            new_metadata_location.to_string(),
        );
        if let Some(prev) = table.metadata_location() {
            params.insert("previous_metadata_location".to_string(), prev.to_string());
        }

        let table_input = aws_sdk_glue::types::TableInput::builder()
            .name(&name)
            .set_storage_descriptor(current.storage_descriptor().cloned())
            .set_parameters(Some(params))
            .table_type("EXTERNAL_TABLE")
            .build()
            .context("Failed to build table input")?;

        let mut update_request = glue_client
            .update_table()
            .database_name(&ns)
            .table_input(table_input)
            .skip_archive(true);

        // Use version_id for optimistic concurrency control
        if let Some(vid) = version_id {
            update_request = update_request.version_id(vid.to_string());
        }

        update_request.send().await.map_err(|e| {
            // Check for version conflict (concurrent modification)
            if let Some(service_err) = e.as_service_error() {
                if service_err.is_concurrent_modification_exception() {
                    return anyhow::anyhow!(
                        "Commit conflict: table was modified by another process. Retry with fresh table state."
                    );
                }
            }
            anyhow::anyhow!("Failed to update Glue table: {e}")
        })?;

        tracing::debug!("Updated Glue metadata_location");
        Ok(())
    }
}
