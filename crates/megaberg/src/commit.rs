//! Commit compaction results to Iceberg catalog.
//!
//! This module commits compaction results by removing old data files and adding
//! new compacted files in a single atomic transaction with operation type "replace".
//!
//! Supports both REST and Glue catalogs.

use std::collections::{HashMap, HashSet};
use std::ops::Deref;

use anyhow::{Context, Result};
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter, ManifestWriterBuilder,
    Struct, MAIN_BRANCH,
};
use iceberg::table::Table;
use iceberg::Catalog;
use serde::Serialize;
use uuid::Uuid;

/// Result of a single compaction task.
#[derive(Debug, Clone)]
pub struct TaskResult {
    /// Input file paths that were compacted (to be deleted)
    pub input_files: Vec<String>,
    /// Output files that were created
    pub output_files: Vec<OutputFile>,
}

/// Information about an output file.
#[derive(Debug, Clone)]
pub struct OutputFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}

/// REST API request for updating a table.
#[derive(Debug, Serialize)]
struct UpdateTableRequest {
    requirements: Vec<TableRequirementJson>,
    updates: Vec<TableUpdateJson>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum TableRequirementJson {
    #[serde(rename = "assert-ref-snapshot-id")]
    AssertRefSnapshotId {
        r#ref: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: Option<i64>,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
enum TableUpdateJson {
    #[serde(rename = "add-snapshot")]
    AddSnapshot { snapshot: SnapshotJson },
    #[serde(rename = "set-snapshot-ref")]
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        r#type: String,
    },
}

#[derive(Debug, Serialize)]
struct SnapshotJson {
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    #[serde(rename = "parent-snapshot-id", skip_serializing_if = "Option::is_none")]
    parent_snapshot_id: Option<i64>,
    #[serde(rename = "sequence-number")]
    sequence_number: i64,
    #[serde(rename = "timestamp-ms")]
    timestamp_ms: i64,
    #[serde(rename = "manifest-list")]
    manifest_list: String,
    summary: SummaryJson,
    #[serde(rename = "schema-id")]
    schema_id: i32,
}

#[derive(Debug, Serialize)]
struct SummaryJson {
    operation: String,
    #[serde(flatten)]
    properties: HashMap<String, String>,
}

/// Commits compaction results to an Iceberg table.
pub async fn commit_compaction(
    table: &Table,
    task_results: Vec<TaskResult>,
    s3_bucket: &str,
    rest_uri: &str,
    namespace: &str,
    table_name: &str,
) -> Result<()> {
    // Collect all input and output files
    let mut deleted_paths: HashSet<String> = HashSet::new();
    let mut added_files: Vec<OutputFile> = Vec::new();

    for result in task_results {
        for path in result.input_files {
            let full_path = if path.starts_with("s3://") {
                path
            } else {
                format!("s3://{}/{}", s3_bucket, path)
            };
            deleted_paths.insert(full_path);
        }
        added_files.extend(result.output_files);
    }

    if deleted_paths.is_empty() && added_files.is_empty() {
        tracing::info!("No files to commit, skipping");
        return Ok(());
    }

    tracing::info!(
        deleted_files = deleted_paths.len(),
        added_files = added_files.len(),
        "Committing compaction results"
    );

    let metadata = table.metadata();
    let file_io = table.file_io();

    // Get current snapshot info
    let current_snapshot = metadata.current_snapshot();
    let parent_snapshot_id = current_snapshot.map(|s| s.snapshot_id());
    let current_sequence_number = current_snapshot.map(|s| s.sequence_number()).unwrap_or(0);

    // Generate new snapshot ID and sequence number
    let new_snapshot_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let new_sequence_number = current_sequence_number + 1;
    let commit_uuid = Uuid::now_v7();

    // Get schema and partition spec
    let schema = metadata.current_schema();
    let partition_spec = metadata.default_partition_spec();
    let table_location = metadata.location();

    // Generate paths for manifest and manifest list
    let manifest_path = format!("{}/metadata/{}-m0.avro", table_location, commit_uuid);
    let manifest_list_path = format!(
        "{}/metadata/snap-{}-0-{}.avro",
        table_location, new_snapshot_id, commit_uuid
    );

    // Load current manifest entries to find files to delete
    let mut existing_manifests: Vec<iceberg::spec::ManifestFile> = Vec::new();
    let mut deleted_entries: Vec<std::sync::Arc<iceberg::spec::ManifestEntry>> = Vec::new();

    if let Some(snapshot) = current_snapshot {
        let manifest_list = snapshot
            .load_manifest_list(file_io, &metadata)
            .await
            .context("Failed to load manifest list")?;

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .context("Failed to load manifest")?;

            let mut has_deleted = false;
            for entry in manifest.entries() {
                if entry.is_alive() && deleted_paths.contains(entry.file_path()) {
                    has_deleted = true;
                    deleted_entries.push(entry.clone());
                }
            }

            if !has_deleted {
                existing_manifests.push(manifest_file.clone());
            }
        }
    }

    tracing::debug!(
        deleted_entries = deleted_entries.len(),
        existing_manifests = existing_manifests.len(),
        "Found entries to delete"
    );

    // Create new manifest for added and deleted files
    let manifest_output = file_io
        .new_output(&manifest_path)
        .context("Failed to create manifest output")?;

    let mut manifest_writer = ManifestWriterBuilder::new(
        manifest_output,
        Some(new_snapshot_id),
        None,
        schema.clone(),
        partition_spec.deref().clone(),
    )
    .build_v2_data();

    // Add new files
    for file in &added_files {
        let full_path = if file.path.starts_with("s3://") {
            file.path.clone()
        } else {
            format!("s3://{}/{}", s3_bucket, file.path)
        };

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(full_path)
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(file.size_bytes)
            .record_count(file.record_count)
            .partition(Struct::empty())
            .partition_spec_id(metadata.default_partition_spec_id())
            .build()
            .context("Failed to build DataFile")?;

        manifest_writer
            .add_file(data_file, new_sequence_number)
            .context("Failed to add file to manifest")?;
    }

    // Add delete entries
    for entry in &deleted_entries {
        manifest_writer
            .add_delete_file(
                entry.data_file.clone(),
                entry.sequence_number.unwrap_or(0),
                entry.file_sequence_number,
            )
            .context("Failed to add delete entry to manifest")?;
    }

    // Write manifest
    let new_manifest_file = manifest_writer
        .write_manifest_file()
        .await
        .context("Failed to write manifest")?;

    tracing::debug!(path = %manifest_path, "Wrote manifest file");

    // Combine with existing manifests
    let mut all_manifests = vec![new_manifest_file];
    all_manifests.extend(existing_manifests);

    // Write manifest list
    let manifest_list_output = file_io
        .new_output(&manifest_list_path)
        .context("Failed to create manifest list output")?;

    let mut manifest_list_writer = ManifestListWriter::v2(
        manifest_list_output,
        new_snapshot_id,
        parent_snapshot_id,
        new_sequence_number,
    );
    manifest_list_writer
        .add_manifests(all_manifests.into_iter())
        .context("Failed to add manifests to list")?;
    manifest_list_writer
        .close()
        .await
        .context("Failed to write manifest list")?;

    tracing::debug!(path = %manifest_list_path, "Wrote manifest list");

    // Build snapshot summary
    let mut summary_props = HashMap::new();
    summary_props.insert("commit.operation".to_string(), "compaction".to_string());
    summary_props.insert(
        "added-data-files".to_string(),
        added_files.len().to_string(),
    );
    summary_props.insert(
        "deleted-data-files".to_string(),
        deleted_entries.len().to_string(),
    );
    summary_props.insert(
        "added-records".to_string(),
        added_files
            .iter()
            .map(|f| f.record_count)
            .sum::<u64>()
            .to_string(),
    );
    summary_props.insert(
        "deleted-records".to_string(),
        deleted_entries
            .iter()
            .map(|e| e.data_file.record_count())
            .sum::<u64>()
            .to_string(),
    );

    // Build REST API request
    let request = UpdateTableRequest {
        requirements: vec![TableRequirementJson::AssertRefSnapshotId {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: parent_snapshot_id,
        }],
        updates: vec![
            TableUpdateJson::AddSnapshot {
                snapshot: SnapshotJson {
                    snapshot_id: new_snapshot_id,
                    parent_snapshot_id,
                    sequence_number: new_sequence_number,
                    timestamp_ms: new_snapshot_id,
                    manifest_list: manifest_list_path,
                    summary: SummaryJson {
                        operation: "replace".to_string(),
                        properties: summary_props,
                    },
                    schema_id: schema.schema_id(),
                },
            },
            TableUpdateJson::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                snapshot_id: new_snapshot_id,
                r#type: "branch".to_string(),
            },
        ],
    };

    // Commit via REST API
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
        .json(&request)
        .send()
        .await
        .context("Failed to send commit request")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Commit failed with status {}: {}", status, body);
    }

    tracing::info!(
        added = added_files.len(),
        deleted = deleted_entries.len(),
        snapshot_id = new_snapshot_id,
        "Compaction committed successfully"
    );

    Ok(())
}

/// Commits compaction results to an Iceberg table in Glue catalog.
pub async fn commit_compaction_glue<C: Catalog>(
    catalog: &C,
    table: &Table,
    task_results: Vec<TaskResult>,
    s3_bucket: &str,
) -> Result<()> {
    // Collect all input and output files
    let mut deleted_paths: HashSet<String> = HashSet::new();
    let mut added_files: Vec<OutputFile> = Vec::new();

    for result in task_results {
        for path in result.input_files {
            let full_path = if path.starts_with("s3://") {
                path
            } else {
                format!("s3://{}/{}", s3_bucket, path)
            };
            deleted_paths.insert(full_path);
        }
        added_files.extend(result.output_files);
    }

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

    // Get current snapshot info
    let current_snapshot = metadata.current_snapshot();
    let parent_snapshot_id = current_snapshot.map(|s| s.snapshot_id());
    let current_sequence_number = current_snapshot.map(|s| s.sequence_number()).unwrap_or(0);

    // Generate new snapshot ID and sequence number
    let new_snapshot_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let new_sequence_number = current_sequence_number + 1;
    let commit_uuid = Uuid::now_v7();

    // Get schema and partition spec
    let schema = metadata.current_schema();
    let partition_spec = metadata.default_partition_spec();
    let table_location = metadata.location();

    // Generate paths for manifest and manifest list
    let manifest_path = format!("{}/metadata/{}-m0.avro", table_location, commit_uuid);
    let manifest_list_path = format!(
        "{}/metadata/snap-{}-0-{}.avro",
        table_location, new_snapshot_id, commit_uuid
    );

    // Load current manifest entries to find files to delete
    let mut existing_manifests: Vec<iceberg::spec::ManifestFile> = Vec::new();
    let mut deleted_entries: Vec<std::sync::Arc<iceberg::spec::ManifestEntry>> = Vec::new();

    if let Some(snapshot) = current_snapshot {
        let manifest_list = snapshot
            .load_manifest_list(file_io, &metadata)
            .await
            .context("Failed to load manifest list")?;

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .context("Failed to load manifest")?;

            let mut has_deleted = false;
            for entry in manifest.entries() {
                if entry.is_alive() && deleted_paths.contains(entry.file_path()) {
                    has_deleted = true;
                    deleted_entries.push(entry.clone());
                }
            }

            if !has_deleted {
                existing_manifests.push(manifest_file.clone());
            }
        }
    }

    tracing::debug!(
        deleted_entries = deleted_entries.len(),
        existing_manifests = existing_manifests.len(),
        "Found entries to delete"
    );

    // Create new manifest for added and deleted files
    let manifest_output = file_io
        .new_output(&manifest_path)
        .context("Failed to create manifest output")?;

    let mut manifest_writer = ManifestWriterBuilder::new(
        manifest_output,
        Some(new_snapshot_id),
        None,
        schema.clone(),
        partition_spec.deref().clone(),
    )
    .build_v2_data();

    // Add new files
    for file in &added_files {
        let full_path = if file.path.starts_with("s3://") {
            file.path.clone()
        } else {
            format!("s3://{}/{}", s3_bucket, file.path)
        };

        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(full_path)
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(file.size_bytes)
            .record_count(file.record_count)
            .partition(Struct::empty())
            .partition_spec_id(metadata.default_partition_spec_id())
            .build()
            .context("Failed to build DataFile")?;

        manifest_writer
            .add_file(data_file, new_sequence_number)
            .context("Failed to add file to manifest")?;
    }

    // Add delete entries
    for entry in &deleted_entries {
        manifest_writer
            .add_delete_file(
                entry.data_file.clone(),
                entry.sequence_number.unwrap_or(0),
                entry.file_sequence_number,
            )
            .context("Failed to add delete entry to manifest")?;
    }

    // Write manifest
    let new_manifest_file = manifest_writer
        .write_manifest_file()
        .await
        .context("Failed to write manifest")?;

    tracing::debug!(path = %manifest_path, "Wrote manifest file");

    // Combine with existing manifests
    let mut all_manifests = vec![new_manifest_file];
    all_manifests.extend(existing_manifests);

    // Write manifest list
    let manifest_list_output = file_io
        .new_output(&manifest_list_path)
        .context("Failed to create manifest list output")?;

    let mut manifest_list_writer = ManifestListWriter::v2(
        manifest_list_output,
        new_snapshot_id,
        parent_snapshot_id,
        new_sequence_number,
    );
    manifest_list_writer
        .add_manifests(all_manifests.into_iter())
        .context("Failed to add manifests to list")?;
    manifest_list_writer
        .close()
        .await
        .context("Failed to write manifest list")?;

    tracing::debug!(path = %manifest_list_path, "Wrote manifest list");

    // Build snapshot summary
    let mut summary_props = HashMap::new();
    summary_props.insert("commit.operation".to_string(), "compaction".to_string());
    summary_props.insert(
        "added-data-files".to_string(),
        added_files.len().to_string(),
    );
    summary_props.insert(
        "deleted-data-files".to_string(),
        deleted_entries.len().to_string(),
    );
    summary_props.insert(
        "added-records".to_string(),
        added_files
            .iter()
            .map(|f| f.record_count)
            .sum::<u64>()
            .to_string(),
    );
    summary_props.insert(
        "deleted-records".to_string(),
        deleted_entries
            .iter()
            .map(|e| e.data_file.record_count())
            .sum::<u64>()
            .to_string(),
    );

    // Build new table metadata
    let new_snapshot = iceberg::spec::Snapshot::builder()
        .with_snapshot_id(new_snapshot_id)
        .with_parent_snapshot_id(parent_snapshot_id)
        .with_sequence_number(new_sequence_number)
        .with_schema_id(schema.schema_id())
        .with_manifest_list(manifest_list_path.clone())
        .with_summary(iceberg::spec::Summary {
            operation: iceberg::spec::Operation::Replace,
            additional_properties: summary_props,
        })
        .with_timestamp_ms(new_snapshot_id)
        .build();

    // Build table updates
    let updates = vec![
        iceberg::TableUpdate::AddSnapshot {
            snapshot: new_snapshot,
        },
        iceberg::TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: iceberg::spec::SnapshotReference::new(
                new_snapshot_id,
                iceberg::spec::SnapshotRetention::branch(None, None, None),
            ),
        },
    ];

    // Build requirements
    let requirements = if let Some(snapshot_id) = parent_snapshot_id {
        vec![iceberg::TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: Some(snapshot_id),
        }]
    } else {
        vec![]
    };

    // Build new metadata by applying updates to current metadata
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

    // Write new metadata file (format: {version}-{uuid}.metadata.json)
    let new_metadata_location = format!(
        "{}/metadata/{:05}-{}.metadata.json",
        table_location, new_sequence_number, commit_uuid
    );
    let metadata_output = file_io
        .new_output(&new_metadata_location)
        .context("Failed to create metadata output")?;

    new_metadata
        .write_to(file_io, &new_metadata_location)
        .await
        .context("Failed to write metadata")?;

    tracing::debug!(path = %new_metadata_location, "Wrote new metadata file");

    // Update Glue table via AWS SDK
    tracing::info!("Updating Glue table metadata_location...");

    // Get table namespace and name from identifier
    let ns = table.identifier().namespace().to_string();
    let name = table.identifier().name().to_string();

    // Use AWS SDK to update the table
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
    parameters.insert(
        "metadata_location".to_string(),
        new_metadata_location.clone(),
    );
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

    tracing::info!(
        added = added_files.len(),
        deleted = deleted_entries.len(),
        snapshot_id = new_snapshot_id,
        "Compaction committed successfully to Glue"
    );

    Ok(())
}
