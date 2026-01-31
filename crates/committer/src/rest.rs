//! REST catalog commit implementation.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use iceberg::spec::{SnapshotReference, SnapshotRetention, MAIN_BRANCH};
use iceberg::table::Table;
use iceberg::TableUpdate;
use serde::{Deserialize, Serialize};

use crate::manifest::{
    filter_manifests, rewrite_manifest_keeping, write_delete_manifest, write_manifest,
    write_manifest_list,
};
use crate::snapshot::{build_snapshot, build_summary};
use crate::types::{CommitContext, DataFileInfo};

/// Configuration for REST catalog.
#[derive(Debug, Clone)]
pub struct RestConfig {
    pub uri: String,
    pub warehouse: Option<String>,
    pub props: HashMap<String, String>,
}

/// REST catalog committer.
pub struct RestCommitter {
    config: RestConfig,
    client: reqwest::Client,
}

/// REST API commit request body.
#[derive(Debug, Serialize)]
struct CommitTableRequest {
    requirements: Vec<TableRequirement>,
    updates: Vec<serde_json::Value>,
}

/// Table requirement for optimistic concurrency.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[allow(clippy::enum_variant_names, dead_code)]
enum TableRequirement {
    AssertCurrentSchemaId {
        #[serde(rename = "current-schema-id")]
        current_schema_id: i32,
    },
    AssertTableUuid {
        uuid: String,
    },
    AssertRefSnapshotId {
        #[serde(rename = "ref")]
        ref_name: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
}

/// REST API commit response.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CommitTableResponse {
    #[serde(rename = "metadata-location")]
    metadata_location: Option<String>,
    metadata: serde_json::Value,
}

impl RestCommitter {
    pub fn new(config: RestConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Takes a table reference and commits to REST catalog.
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

        tracing::info!(
            added = added_files.len(),
            removed = removed_files.len(),
            "Committing to REST catalog"
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

        // Write manifest list with new manifests
        write_manifest_list(file_io, &ctx, new_manifests, filtered_existing).await?;

        // Build snapshot summary
        let summary_props = build_summary(&added_files);
        let snapshot = build_snapshot(&ctx, metadata.current_schema().schema_id(), summary_props);

        // Build table updates (REST server will apply these and write metadata.json)
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

        // Send updates to REST catalog (server handles metadata.json)
        self.update_rest_table(table, &updates).await?;

        tracing::info!(snapshot_id = ctx.new_snapshot_id, "Commit complete");
        Ok(ctx.new_snapshot_id)
    }

    /// Update REST catalog's table via REST API.
    /// Server receives TableUpdates and handles metadata.json writing internally.
    async fn update_rest_table(&self, table: &Table, updates: &[TableUpdate]) -> Result<()> {
        let ns = table.identifier().namespace().to_string();
        let name = table.identifier().name().to_string();
        let metadata = table.metadata();

        // Build requirements for optimistic concurrency
        let requirements = vec![
            TableRequirement::AssertCurrentSchemaId {
                current_schema_id: metadata.current_schema().schema_id(),
            },
            TableRequirement::AssertRefSnapshotId {
                ref_name: MAIN_BRANCH.to_string(),
                snapshot_id: metadata.current_snapshot_id(),
            },
        ];

        // Convert TableUpdates to JSON
        // The REST API expects updates in a specific format
        let updates_json: Vec<serde_json::Value> = updates
            .iter()
            .map(|u| self.table_update_to_json(u))
            .collect::<Result<Vec<_>>>()?;

        let request = CommitTableRequest {
            requirements,
            updates: updates_json,
        };

        // Build URL: POST /v1/namespaces/{namespace}/tables/{table}
        let url = format!(
            "{}/v1/namespaces/{}/tables/{}",
            self.config.uri.trim_end_matches('/'),
            ns,
            name
        );

        tracing::debug!(url = %url, "Committing to REST catalog");

        // Build request with headers from props (e.g., "header.Authorization" -> "Authorization: value")
        let mut request_builder = self
            .client
            .post(&url)
            .header("Content-Type", "application/json");

        for (key, value) in &self.config.props {
            if let Some(header_name) = key.strip_prefix("header.") {
                request_builder = request_builder.header(header_name, value);
            }
        }

        let response = request_builder
            .json(&request)
            .send()
            .await
            .context("Failed to send commit request to REST catalog")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("REST catalog commit failed with status {status}: {body}");
        }

        let _commit_response: CommitTableResponse = response
            .json()
            .await
            .context("Failed to parse commit response")?;

        tracing::debug!("Updated REST catalog metadata location");
        Ok(())
    }

    /// Convert iceberg TableUpdate to REST API JSON format.
    fn table_update_to_json(&self, update: &TableUpdate) -> Result<serde_json::Value> {
        match update {
            TableUpdate::AddSnapshot { snapshot } => {
                // Build summary with operation + flattened additional properties
                let mut summary = serde_json::Map::new();
                summary.insert(
                    "operation".to_string(),
                    serde_json::Value::String(
                        format!("{:?}", snapshot.summary().operation).to_lowercase(),
                    ),
                );
                for (k, v) in &snapshot.summary().additional_properties {
                    summary.insert(k.clone(), serde_json::Value::String(v.clone()));
                }

                Ok(serde_json::json!({
                    "action": "add-snapshot",
                    "snapshot": {
                        "snapshot-id": snapshot.snapshot_id(),
                        "parent-snapshot-id": snapshot.parent_snapshot_id(),
                        "sequence-number": snapshot.sequence_number(),
                        "timestamp-ms": snapshot.timestamp_ms(),
                        "manifest-list": snapshot.manifest_list(),
                        "summary": summary,
                        "schema-id": snapshot.schema_id()
                    }
                }))
            }
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => Ok(serde_json::json!({
                "action": "set-snapshot-ref",
                "ref-name": ref_name,
                "snapshot-id": reference.snapshot_id,
                "type": "branch"
            })),
            _ => anyhow::bail!("Unsupported table update type for REST commit"),
        }
    }
}
