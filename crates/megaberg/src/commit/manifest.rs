//! Manifest writing utilities.

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{Context, Result};
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestEntry, ManifestFile,
    ManifestListWriter, ManifestWriterBuilder, PartitionSpec, Schema, Struct,
};
use iceberg::table::Table;

use super::types::{CommitContext, OutputFile};

/// Result of loading existing manifests and finding entries to delete.
pub struct ManifestAnalysis {
    /// Manifests that don't contain any deleted files (to be preserved)
    pub existing_manifests: Vec<ManifestFile>,
    /// Entries that should be marked as deleted
    pub deleted_entries: Vec<Arc<ManifestEntry>>,
}

/// Load current manifest entries and find files to delete.
///
/// Note: We keep ALL existing manifests in the manifest list. The new manifest
/// we create will have DELETED entries that override the ADDED entries in
/// existing manifests. This is how Iceberg handles file deletion - the most
/// recent entry for a file path determines its status.
pub async fn analyze_manifests(
    table: &Table,
    deleted_paths: &HashSet<String>,
) -> Result<ManifestAnalysis> {
    let metadata = table.metadata();
    let file_io = table.file_io();
    let current_snapshot = metadata.current_snapshot();

    let mut existing_manifests: Vec<ManifestFile> = Vec::new();
    let mut deleted_entries: Vec<Arc<ManifestEntry>> = Vec::new();

    if let Some(snapshot) = current_snapshot {
        let manifest_list = snapshot
            .load_manifest_list(file_io, metadata)
            .await
            .context("Failed to load manifest list")?;

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file
                .load_manifest(file_io)
                .await
                .context("Failed to load manifest")?;

            // Find entries to delete from this manifest
            for entry in manifest.entries() {
                if entry.is_alive() && deleted_paths.contains(entry.file_path()) {
                    deleted_entries.push(entry.clone());
                }
            }

            // Keep ALL manifests - deleted entries in new manifest will override
            existing_manifests.push(manifest_file.clone());
        }
    }

    tracing::debug!(
        deleted_entries = deleted_entries.len(),
        existing_manifests = existing_manifests.len(),
        "Analyzed manifests"
    );

    Ok(ManifestAnalysis {
        existing_manifests,
        deleted_entries,
    })
}

/// Write a new manifest with added and deleted files.
pub async fn write_manifest(
    file_io: &FileIO,
    ctx: &CommitContext,
    schema: Arc<Schema>,
    partition_spec: Arc<PartitionSpec>,
    partition_spec_id: i32,
    added_files: &[OutputFile],
    deleted_entries: &[Arc<ManifestEntry>],
    s3_bucket: &str,
) -> Result<ManifestFile> {
    let manifest_output = file_io
        .new_output(&ctx.manifest_path)
        .context("Failed to create manifest output")?;

    let mut manifest_writer = ManifestWriterBuilder::new(
        manifest_output,
        Some(ctx.new_snapshot_id),
        None,
        schema,
        partition_spec.deref().clone(),
    )
    .build_v2_data();

    // Add new files
    for file in added_files {
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
            .partition_spec_id(partition_spec_id)
            .build()
            .context("Failed to build DataFile")?;

        manifest_writer
            .add_file(data_file, ctx.new_sequence_number)
            .context("Failed to add file to manifest")?;
    }

    // Add delete entries
    for entry in deleted_entries {
        manifest_writer
            .add_delete_file(
                entry.data_file.clone(),
                entry.sequence_number.unwrap_or(0),
                entry.file_sequence_number,
            )
            .context("Failed to add delete entry to manifest")?;
    }

    let manifest_file = manifest_writer
        .write_manifest_file()
        .await
        .context("Failed to write manifest")?;

    tracing::debug!(path = %ctx.manifest_path, "Wrote manifest file");

    Ok(manifest_file)
}

/// Write a manifest list combining new and existing manifests.
pub async fn write_manifest_list(
    file_io: &FileIO,
    ctx: &CommitContext,
    new_manifest: ManifestFile,
    existing_manifests: Vec<ManifestFile>,
) -> Result<()> {
    let mut all_manifests = vec![new_manifest];
    all_manifests.extend(existing_manifests);

    let manifest_list_output = file_io
        .new_output(&ctx.manifest_list_path)
        .context("Failed to create manifest list output")?;

    let mut manifest_list_writer = ManifestListWriter::v2(
        manifest_list_output,
        ctx.new_snapshot_id,
        ctx.parent_snapshot_id,
        ctx.new_sequence_number,
    );

    manifest_list_writer
        .add_manifests(all_manifests.into_iter())
        .context("Failed to add manifests to list")?;

    manifest_list_writer
        .close()
        .await
        .context("Failed to write manifest list")?;

    tracing::debug!(path = %ctx.manifest_list_path, "Wrote manifest list");

    Ok(())
}
