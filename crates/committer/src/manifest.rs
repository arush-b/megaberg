//! Manifest writing utilities.

use std::ops::Deref;
use std::sync::Arc;

use anyhow::{Context, Result};
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestFile,
    ManifestListWriter, ManifestWriterBuilder, PartitionSpec, Schema, Struct,
};

use crate::types::{CommitContext, DataFileInfo};

/// Build a partition struct with null values matching the partition spec.
/// iceberg-rs requires the partition struct to have the same number of fields
/// as the partition spec, even if we don't know the actual values.
fn build_null_partition(partition_spec: &PartitionSpec) -> Struct {
    let num_fields = partition_spec.fields().len();
    if num_fields == 0 {
        return Struct::empty();
    }
    let null_values: Vec<Option<Literal>> = vec![None; num_fields];
    Struct::from_iter(null_values)
}

/// Write a new manifest with added files.
pub async fn write_manifest(
    file_io: &FileIO,
    ctx: &CommitContext,
    schema: Arc<Schema>,
    partition_spec: Arc<PartitionSpec>,
    partition_spec_id: i32,
    added_files: &[DataFileInfo],
    s3_bucket: &str,
) -> Result<ManifestFile> {
    let manifest_output = file_io
        .new_output(&ctx.manifest_path)
        .context("Failed to create manifest output")?;

    let mut writer = ManifestWriterBuilder::new(
        manifest_output,
        Some(ctx.new_snapshot_id),
        None,
        schema,
        partition_spec.deref().clone(),
    )
    .build_v2_data();

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
            .partition(build_null_partition(&partition_spec))
            .partition_spec_id(partition_spec_id)
            .build()
            .context("Failed to build DataFile")?;

        writer
            .add_file(data_file, ctx.new_sequence_number)
            .context("Failed to add file to manifest")?;
    }

    let manifest_file = writer
        .write_manifest_file()
        .await
        .context("Failed to write manifest")?;

    tracing::debug!(path = %ctx.manifest_path, "Wrote manifest");
    Ok(manifest_file)
}

/// Write a manifest with deleted file entries.
/// This marks the input files that were compacted as DELETED.
pub async fn write_delete_manifest(
    file_io: &FileIO,
    ctx: &CommitContext,
    schema: Arc<Schema>,
    partition_spec: Arc<PartitionSpec>,
    partition_spec_id: i32,
    deleted_files: &[DataFileInfo],
    s3_bucket: &str,
) -> Result<ManifestFile> {
    // Use a different manifest path for deletes
    let delete_manifest_path = format!(
        "{}/metadata/{}-m1.avro",
        ctx.manifest_path.rsplit_once("/metadata/").map(|(loc, _)| loc).unwrap_or(""),
        ctx.commit_uuid
    );

    let manifest_output = file_io
        .new_output(&delete_manifest_path)
        .context("Failed to create delete manifest output")?;

    let mut writer = ManifestWriterBuilder::new(
        manifest_output,
        Some(ctx.new_snapshot_id),
        None,
        schema,
        partition_spec.deref().clone(),
    )
    .build_v2_data();

    for file in deleted_files {
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
            .partition(build_null_partition(&partition_spec))
            .partition_spec_id(partition_spec_id)
            .build()
            .context("Failed to build DataFile for deletion")?;

        // Use add_delete_file to mark the file as DELETED
        // sequence_number: the original sequence when file was added
        // file_sequence_number: same as sequence_number for data files
        let seq_num = file.sequence_number.unwrap_or(1);
        writer
            .add_delete_file(data_file, seq_num, Some(seq_num))
            .context("Failed to add delete entry to manifest")?;
    }

    let manifest_file = writer
        .write_manifest_file()
        .await
        .context("Failed to write delete manifest")?;

    tracing::debug!(path = %delete_manifest_path, "Wrote delete manifest");
    Ok(manifest_file)
}

/// Write manifest list with new manifests + existing manifests.
pub async fn write_manifest_list(
    file_io: &FileIO,
    ctx: &CommitContext,
    new_manifests: Vec<ManifestFile>,
    existing_manifests: Vec<ManifestFile>,
) -> Result<()> {
    let mut all_manifests = new_manifests;
    all_manifests.extend(existing_manifests);

    let output = file_io
        .new_output(&ctx.manifest_list_path)
        .context("Failed to create manifest list output")?;

    let mut writer = ManifestListWriter::v2(
        output,
        ctx.new_snapshot_id,
        ctx.parent_snapshot_id,
        ctx.new_sequence_number,
    );

    writer
        .add_manifests(all_manifests.into_iter())
        .context("Failed to add manifests")?;

    writer.close().await.context("Failed to write manifest list")?;

    tracing::debug!(path = %ctx.manifest_list_path, "Wrote manifest list");
    Ok(())
}
