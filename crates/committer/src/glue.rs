//! Glue catalog commit implementation.

use std::collections::HashMap;

use anyhow::{Context, Result};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_glue::GlueCatalogBuilder;

use iceberg::spec::{SnapshotReference, SnapshotRetention, MAIN_BRANCH};
use iceberg::TableUpdate;

use crate::manifest::{write_delete_manifest, write_manifest, write_manifest_list};
use crate::snapshot::{build_snapshot, build_summary};
use crate::types::{CommitContext, DataFileInfo};

/// Configuration for Glue catalog.
pub struct GlueConfig {
    pub warehouse: String,
    pub region: Option<String>,
}

/// Glue catalog committer.
pub struct GlueCommitter {
    config: GlueConfig,
}

impl GlueCommitter {
    pub fn new(config: GlueConfig) -> Self {
        Self { config }
    }

    /// Load table from Glue catalog.
    async fn load_table(&self, namespace: &str, table_name: &str) -> Result<Table> {
        let mut props = HashMap::new();
        props.insert("warehouse".to_string(), self.config.warehouse.clone());
        if let Some(ref region) = self.config.region {
            props.insert("region".to_string(), region.clone());
        }

        let catalog = GlueCatalogBuilder::default()
            .load("glue", props)
            .await
            .context("Failed to build Glue catalog")?;

        let table_ident = TableIdent::new(
            NamespaceIdent::new(namespace.to_string()),
            table_name.to_string(),
        );

        catalog
            .load_table(&table_ident)
            .await
            .context("Failed to load table from Glue")
    }

    /// Commit added files to Glue catalog. Returns new snapshot ID.
    pub async fn commit(
        &self,
        namespace: &str,
        table_name: &str,
        added_files: Vec<DataFileInfo>,
        s3_bucket: &str,
    ) -> Result<i64> {
        if added_files.is_empty() {
            tracing::info!("No files to commit, skipping");
            return Ok(0);
        }

        tracing::info!(added_files = added_files.len(), "Committing to Glue");

        let table = self.load_table(namespace, table_name).await?;
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

        // Write new manifest
        let new_manifest = write_manifest(
            file_io,
            &ctx,
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            metadata.default_partition_spec_id(),
            &added_files,
            s3_bucket,
        )
        .await?;

        // Write manifest list
        write_manifest_list(file_io, &ctx, vec![new_manifest], existing_manifests).await?;

        // Build snapshot
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
            builder = update.clone().apply(builder).context("Failed to apply update")?;
        }
        let new_metadata = builder.build().context("Failed to build metadata")?.metadata;

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

        tracing::debug!(path = %metadata_path, "Wrote metadata");

        // Update Glue table
        self.update_glue_table(&table, &metadata_path).await?;

        tracing::info!(snapshot_id = ctx.new_snapshot_id, "Commit complete");
        Ok(ctx.new_snapshot_id)
    }

    /// Commit with an already-loaded Table reference.
    /// This avoids reloading the table when the caller already has it.
    /// Handles both added files and removed files (compaction scenario).
    pub async fn commit_with_table(
        &self,
        table: &Table,
        added_files: Vec<DataFileInfo>,
        removed_files: Vec<DataFileInfo>,
        s3_bucket: &str,
    ) -> Result<i64> {
        if added_files.is_empty() && removed_files.is_empty() {
            tracing::info!("No files to commit, skipping");
            return Ok(0);
        }

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

        // Write manifest list with all new manifests + existing
        write_manifest_list(file_io, &ctx, new_manifests, existing_manifests).await?;

        // Build snapshot summary
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
            builder = update.clone().apply(builder).context("Failed to apply update")?;
        }
        let new_metadata = builder.build().context("Failed to build metadata")?.metadata;

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

        tracing::debug!(path = %metadata_path, "Wrote metadata");

        // Update Glue table
        self.update_glue_table(table, &metadata_path).await?;

        tracing::info!(snapshot_id = ctx.new_snapshot_id, "Commit complete");
        Ok(ctx.new_snapshot_id)
    }

    /// Update Glue table's metadata_location.
    async fn update_glue_table(&self, table: &Table, new_metadata_location: &str) -> Result<()> {
        let ns = table.identifier().namespace().to_string();
        let name = table.identifier().name().to_string();

        let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let glue_client = aws_sdk_glue::Client::new(&aws_config);

        // Get current table to preserve properties
        let current = glue_client
            .get_table()
            .database_name(&ns)
            .name(&name)
            .send()
            .await
            .context("Failed to get Glue table")?
            .table
            .context("Table not found")?;

        // Update metadata_location
        let mut params = current.parameters().cloned().unwrap_or_default();
        params.insert("metadata_location".to_string(), new_metadata_location.to_string());
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

        glue_client
            .update_table()
            .database_name(&ns)
            .table_input(table_input)
            .skip_archive(true)
            .send()
            .await
            .context("Failed to update Glue table")?;

        tracing::debug!("Updated Glue metadata_location");
        Ok(())
    }
}
