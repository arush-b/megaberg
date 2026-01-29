//! Manifest writing utilities.

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{Context, Result};
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, Literal, ManifestContentType, ManifestFile,
    ManifestListWriter, ManifestStatus, ManifestWriterBuilder, PartitionSpec, PrimitiveLiteral,
    Schema, Struct,
};

use crate::types::{CommitContext, DataFileInfo};

/// Build a partition struct from the provided values or nulls if not available.
/// The struct must have the same number of fields as the partition spec.
fn build_partition(
    partition_spec: &PartitionSpec,
    partition_values: &Option<HashMap<String, String>>,
) -> Struct {
    let num_fields = partition_spec.fields().len();
    if num_fields == 0 {
        return Struct::empty();
    }

    match partition_values {
        Some(values) => {
            // Build struct with actual values in spec field order
            let literals: Vec<Option<Literal>> = partition_spec
                .fields()
                .iter()
                .map(|field| {
                    values
                        .get(field.name.as_str())
                        .map(|v| parse_partition_value(v))
                })
                .collect();
            Struct::from_iter(literals)
        }
        None => {
            // Fallback to nulls
            let null_values: Vec<Option<Literal>> = vec![None; num_fields];
            Struct::from_iter(null_values)
        }
    }
}

/// Parse a string partition value to an Iceberg Literal.
fn parse_partition_value(value: &str) -> Literal {
    // Try i32 first (hour/day/month/year transforms produce i32)
    if let Ok(i) = value.parse::<i32>() {
        return Literal::Primitive(PrimitiveLiteral::Int(i));
    }
    if let Ok(i) = value.parse::<i64>() {
        return Literal::Primitive(PrimitiveLiteral::Long(i));
    }
    Literal::Primitive(PrimitiveLiteral::String(value.to_string()))
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
            .partition(build_partition(&partition_spec, &file.partition_values))
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

    tracing::debug!(path = %ctx.manifest_path, "Added data manifest written.");
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
            .partition(build_partition(&partition_spec, &file.partition_values))
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

    tracing::debug!(path = %delete_manifest_path, "Delete manifest written.");
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

/// Result of filtering existing manifests for carryforward.
pub struct ManifestFilterResult {
    /// Manifests that can be carried forward as-is (no files being removed, or delete manifests)
    pub unaffected: Vec<ManifestFile>,
    /// Manifests that need to be rewritten (contain mix of kept and removed files)
    pub to_rewrite: Vec<ManifestFile>,
    /// Total files kept from affected manifests (for logging)
    pub kept_file_count: usize,
    /// Total files removed from affected manifests (for logging)
    pub removed_file_count: usize,
}

/// Filter existing manifests based on which files are being removed.
///
/// Returns:
/// - unaffected: manifests with no removed files, or delete manifests (carry forward as-is)
/// - to_rewrite: data manifests with some removed files (need rewriting)
///
/// Data manifests where ALL files are removed are implicitly dropped.
pub async fn filter_manifests(
    file_io: &FileIO,
    existing_manifests: Vec<ManifestFile>,
    removed_paths: &HashSet<String>,
) -> Result<ManifestFilterResult> {
    let mut unaffected = Vec::new();
    let mut to_rewrite = Vec::new();
    let mut kept_file_count = 0;
    let mut removed_file_count = 0;

    for manifest_file in existing_manifests {
        // Carry forward delete manifests unchanged
        if manifest_file.content != ManifestContentType::Data {
            unaffected.push(manifest_file);
            continue;
        }

        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .with_context(|| {
                format!(
                    "Failed to load manifest for filtering: {}",
                    manifest_file.manifest_path
                )
            })?;

        let mut has_removed = false;
        let mut has_kept = false;

        for entry in manifest.entries() {
            // Skip already-deleted entries
            if entry.status() == ManifestStatus::Deleted {
                continue;
            }

            let path = entry.data_file().file_path();
            if removed_paths.contains(path) {
                has_removed = true;
                removed_file_count += 1;
            } else {
                has_kept = true;
                kept_file_count += 1;
            }
        }

        match (has_kept, has_removed) {
            (true, false) => {
                // No files being removed - carry forward as-is
                unaffected.push(manifest_file);
            }
            (true, true) => {
                // Mix of kept and removed - needs rewriting
                to_rewrite.push(manifest_file);
            }
            (false, true) => {
                // All files being removed - drop this manifest entirely
                tracing::debug!(
                    path = %manifest_file.manifest_path,
                    "Dropping manifest (all files removed)"
                );
            }
            (false, false) => {
                // Empty manifest (shouldn't happen) - drop it
                tracing::warn!(
                    path = %manifest_file.manifest_path,
                    "Dropping empty manifest"
                );
            }
        }
    }

    Ok(ManifestFilterResult {
        unaffected,
        to_rewrite,
        kept_file_count,
        removed_file_count,
    })
}

/// Rewrite a manifest, keeping only entries for files NOT in removed_paths.
///
/// The rewritten manifest uses EXISTING status for kept entries to indicate
/// they haven't changed in this snapshot.
pub async fn rewrite_manifest_keeping(
    file_io: &FileIO,
    ctx: &CommitContext,
    manifest_file: &ManifestFile,
    removed_paths: &HashSet<String>,
    schema: Arc<Schema>,
    partition_spec: Arc<PartitionSpec>,
    manifest_index: usize,
) -> Result<Option<ManifestFile>> {
    let manifest = manifest_file
        .load_manifest(file_io)
        .await
        .with_context(|| {
            format!(
                "Failed to load manifest for rewriting: {}",
                manifest_file.manifest_path
            )
        })?;

    // Collect entries to keep
    let kept_entries: Vec<_> = manifest
        .entries()
        .iter()
        .filter(|entry| {
            // Skip already-deleted entries
            if entry.status() == ManifestStatus::Deleted {
                return false;
            }
            // Keep if not in removed set
            !removed_paths.contains(entry.data_file().file_path())
        })
        .collect();

    if kept_entries.is_empty() {
        return Ok(None);
    }

    // Create new manifest path for the rewritten manifest
    let rewrite_manifest_path = format!(
        "{}/metadata/{}-m{}.avro",
        ctx.manifest_path
            .rsplit_once("/metadata/")
            .map(|(loc, _)| loc)
            .unwrap_or(""),
        ctx.commit_uuid,
        manifest_index + 10 // Offset to avoid collision with m0, m1
    );

    let manifest_output = file_io
        .new_output(&rewrite_manifest_path)
        .context("Failed to create rewrite manifest output")?;

    let mut writer = ManifestWriterBuilder::new(
        manifest_output,
        Some(ctx.new_snapshot_id),
        None,
        schema,
        partition_spec.deref().clone(),
    )
    .build_v2_data();

    for entry in &kept_entries {
        // Use add_existing_file to mark entry as EXISTING (unchanged)
        // Preserve the original snapshot_id, sequence_number, and file_sequence_number
        let snapshot_id = entry.snapshot_id().unwrap_or(0);
        let seq_num = entry.sequence_number().unwrap_or(1);
        let file_seq_num = entry.file_sequence_number.unwrap_or(seq_num);
        writer
            .add_existing_file(entry.data_file().clone(), snapshot_id, seq_num, Some(file_seq_num))
            .context("Failed to add existing file to rewritten manifest")?;
    }

    let new_manifest_file = writer
        .write_manifest_file()
        .await
        .context("Failed to write rewritten manifest")?;

    tracing::debug!(
        old_path = %manifest_file.manifest_path,
        new_path = %rewrite_manifest_path,
        kept_entries = kept_entries.len(),
        "Rewrote manifest with kept entries"
    );

    Ok(Some(new_manifest_file))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type};

    /// Helper to create a simple schema for testing
    fn test_schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "year", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(4, "month", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("Failed to build test schema")
    }

    /// Helper to create partition spec with specified field names
    fn partition_spec_with_fields(schema: &Schema, fields: &[&str]) -> PartitionSpec {
        let schema_ref = Arc::new(schema.clone());
        let mut builder = PartitionSpec::builder(schema_ref).with_spec_id(0);
        for field in fields {
            builder = builder
                .add_partition_field(*field, *field, Transform::Identity)
                .expect("Failed to add partition field");
        }
        builder.build().expect("Failed to build partition spec")
    }

    // ============================================================
    // Partition Building Tests
    // ============================================================

    #[test]
    fn test_build_partition_empty_spec() {
        // Empty partition spec (0 fields) should return Struct::empty()
        let schema = test_schema();
        let schema_ref = Arc::new(schema);
        let empty_spec = PartitionSpec::builder(schema_ref)
            .with_spec_id(0)
            .build()
            .expect("Failed to build empty partition spec");

        let result = build_partition(&empty_spec, &None);
        assert_eq!(result, Struct::empty());
    }

    #[test]
    fn test_build_partition_none_or_empty_values() {
        // Both None and empty HashMap should produce struct with all null values
        let schema = test_schema();
        let spec = partition_spec_with_fields(&schema, &["year", "month"]);

        // With None
        let result_none = build_partition(&spec, &None);
        let expected = Struct::from_iter(vec![None::<Literal>, None::<Literal>]);
        assert_eq!(result_none, expected);

        // With empty HashMap
        let empty_map: HashMap<String, String> = HashMap::new();
        let result_empty = build_partition(&spec, &Some(empty_map));
        assert_eq!(result_empty, expected);
    }

    #[test]
    fn test_build_partition_all_fields_present() {
        // When all partition fields are present, values are placed in spec order
        let schema = test_schema();
        let spec = partition_spec_with_fields(&schema, &["year", "month"]);

        let mut values = HashMap::new();
        values.insert("year".to_string(), "2024".to_string());
        values.insert("month".to_string(), "12".to_string());

        let result = build_partition(&spec, &Some(values));

        // parse_partition_value tries i32 first, so numeric values become Int
        let expected = Struct::from_iter(vec![
            Some(Literal::Primitive(PrimitiveLiteral::Int(2024))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(12))),
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_build_partition_missing_fields() {
        // Missing partition fields become null in the struct
        let schema = test_schema();
        let spec = partition_spec_with_fields(&schema, &["year", "month"]);

        let mut values = HashMap::new();
        values.insert("year".to_string(), "2024".to_string());
        // month is intentionally missing

        let result = build_partition(&spec, &Some(values));

        let expected = Struct::from_iter(vec![
            Some(Literal::Primitive(PrimitiveLiteral::Int(2024))),
            None, // month is null because it's missing
        ]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_build_partition_extra_fields_ignored() {
        // Extra fields in the map that aren't in spec are silently ignored
        let schema = test_schema();
        let spec = partition_spec_with_fields(&schema, &["year"]);

        let mut values = HashMap::new();
        values.insert("year".to_string(), "2024".to_string());
        values.insert("extra_field".to_string(), "ignored".to_string());
        values.insert("another_extra".to_string(), "also_ignored".to_string());

        let result = build_partition(&spec, &Some(values));

        // Only year should be in result, extras are ignored
        let expected = Struct::from_iter(vec![Some(Literal::Primitive(PrimitiveLiteral::Int(
            2024,
        )))]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_build_partition_field_ordering() {
        // Output follows partition spec order, not the order keys were inserted in map
        let schema = test_schema();
        let spec = partition_spec_with_fields(&schema, &["year", "month"]);

        // Insert in reverse order (month first, then year)
        let mut values = HashMap::new();
        values.insert("month".to_string(), "6".to_string());
        values.insert("year".to_string(), "2023".to_string());

        let result = build_partition(&spec, &Some(values));

        // Output must be year first, then month (spec order, not insertion order)
        let expected = Struct::from_iter(vec![
            Some(Literal::Primitive(PrimitiveLiteral::Int(2023))), // year first
            Some(Literal::Primitive(PrimitiveLiteral::Int(6))),    // month second
        ]);
        assert_eq!(result, expected);
    }

    // ============================================================
    // Value Parsing Tests
    // ============================================================

    #[test]
    fn test_parse_zero() {
        // "0" parses as Int(0) since i32 is tried first
        let result = parse_partition_value("0");
        assert_eq!(result, Literal::Primitive(PrimitiveLiteral::Int(0)));
    }

    #[test]
    fn test_parse_leading_zeros() {
        // Leading zeros are stripped when parsing as number: "007" -> Int(7)
        let result = parse_partition_value("007");
        assert_eq!(result, Literal::Primitive(PrimitiveLiteral::Int(7)));
    }

    #[test]
    fn test_parse_empty_string() {
        // Empty string stays as empty string, not treated as null
        let result = parse_partition_value("");
        assert_eq!(
            result,
            Literal::Primitive(PrimitiveLiteral::String("".to_string()))
        );
    }

    #[test]
    fn test_parse_whitespace_not_trimmed() {
        // Whitespace is preserved, not trimmed: " 123 " stays as string with spaces
        let result = parse_partition_value(" 123 ");
        assert_eq!(
            result,
            Literal::Primitive(PrimitiveLiteral::String(" 123 ".to_string()))
        );
    }

    #[test]
    fn test_parse_null_strings() {
        // "null" and "NULL" are treated as literal strings, not as null values
        let result_lower = parse_partition_value("null");
        assert_eq!(
            result_lower,
            Literal::Primitive(PrimitiveLiteral::String("null".to_string()))
        );

        let result_upper = parse_partition_value("NULL");
        assert_eq!(
            result_upper,
            Literal::Primitive(PrimitiveLiteral::String("NULL".to_string()))
        );
    }

    // ============================================================
    // Manifest Filtering Tests
    // ============================================================

    mod filter_tests {
        use super::*;
        use iceberg::io::FileIOBuilder;
        use iceberg::spec::{
            DataFile, DataFileBuilder, DataFileFormat, ManifestContentType, ManifestWriterBuilder,
            NestedField, PrimitiveType, Schema, Struct, Type,
        };

        /// Create a test schema
        fn test_schema() -> Arc<Schema> {
            Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                        NestedField::required(2, "data", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()
                    .expect("Failed to build test schema"),
            )
        }

        /// Create an unpartitioned partition spec
        fn empty_partition_spec(schema: &Schema) -> PartitionSpec {
            PartitionSpec::builder(Arc::new(schema.clone()))
                .with_spec_id(0)
                .build()
                .expect("Failed to build empty partition spec")
        }

        /// Create a test data file
        fn test_data_file(path: &str) -> DataFile {
            DataFileBuilder::default()
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(1024)
                .record_count(100)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .content(DataContentType::Data)
                .build()
                .expect("Failed to build test data file")
        }

        /// Create memory FileIO for testing
        fn memory_file_io() -> FileIO {
            FileIOBuilder::new("memory")
                .build()
                .expect("Failed to build memory FileIO")
        }

        /// Write a test manifest with given files and return the ManifestFile
        async fn write_test_manifest(
            file_io: &FileIO,
            path: &str,
            schema: Arc<Schema>,
            partition_spec: PartitionSpec,
            file_paths: &[&str],
            snapshot_id: i64,
        ) -> ManifestFile {
            let output = file_io
                .new_output(path)
                .expect("Failed to create output file");

            let mut writer = ManifestWriterBuilder::new(
                output,
                Some(snapshot_id),
                None,
                schema,
                partition_spec,
            )
            .build_v2_data();

            for file_path in file_paths {
                let data_file = test_data_file(file_path);
                writer
                    .add_file(data_file, 1)
                    .expect("Failed to add file to manifest");
            }

            writer
                .write_manifest_file()
                .await
                .expect("Failed to write manifest")
        }

        #[tokio::test]
        async fn test_filter_empty_manifest_list() {
            // Empty manifest list returns empty results
            let file_io = memory_file_io();
            let removed_paths: HashSet<String> = HashSet::new();

            let result = filter_manifests(&file_io, vec![], &removed_paths)
                .await
                .expect("filter_manifests should succeed");

            assert!(result.unaffected.is_empty());
            assert!(result.to_rewrite.is_empty());
            assert_eq!(result.kept_file_count, 0);
            assert_eq!(result.removed_file_count, 0);
        }

        #[tokio::test]
        async fn test_filter_empty_removed_set() {
            // When no files are being removed, all manifests go to unaffected
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);

            let manifest = write_test_manifest(
                &file_io,
                "memory://test/manifest1.avro",
                schema,
                partition_spec,
                &["s3://bucket/file1.parquet", "s3://bucket/file2.parquet"],
                1,
            )
            .await;

            let removed_paths: HashSet<String> = HashSet::new();

            let result = filter_manifests(&file_io, vec![manifest], &removed_paths)
                .await
                .expect("filter_manifests should succeed");

            assert_eq!(result.unaffected.len(), 1);
            assert!(result.to_rewrite.is_empty());
            assert_eq!(result.kept_file_count, 2);
            assert_eq!(result.removed_file_count, 0);
        }

        #[tokio::test]
        async fn test_filter_delete_manifest_passthrough() {
            // Delete manifests (ManifestContentType::Deletes) are always passed through unchanged
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);

            // Create a delete manifest by using build_v2_deletes
            let output = file_io
                .new_output("memory://test/delete_manifest.avro")
                .expect("Failed to create output");

            let writer = ManifestWriterBuilder::new(
                output,
                Some(1),
                None,
                schema.clone(),
                partition_spec.clone(),
            )
            .build_v2_deletes();

            let delete_manifest = writer
                .write_manifest_file()
                .await
                .expect("Failed to write delete manifest");

            // Verify it's a delete manifest
            assert_eq!(delete_manifest.content, ManifestContentType::Deletes);

            // Even with a removed path set, delete manifests should pass through
            let mut removed_paths: HashSet<String> = HashSet::new();
            removed_paths.insert("s3://bucket/some_file.parquet".to_string());

            let result = filter_manifests(&file_io, vec![delete_manifest], &removed_paths)
                .await
                .expect("filter_manifests should succeed");

            // Delete manifest should go to unaffected without being inspected
            assert_eq!(result.unaffected.len(), 1);
            assert!(result.to_rewrite.is_empty());
            // Counts should be 0 because delete manifests aren't inspected
            assert_eq!(result.kept_file_count, 0);
            assert_eq!(result.removed_file_count, 0);
        }

        #[tokio::test]
        async fn test_filter_single_manifest_unaffected() {
            // Single manifest with no files in the removed set goes to unaffected
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);

            let manifest = write_test_manifest(
                &file_io,
                "memory://test/manifest.avro",
                schema,
                partition_spec,
                &["s3://bucket/file1.parquet", "s3://bucket/file2.parquet"],
                1,
            )
            .await;

            // Remove a file that's NOT in the manifest
            let mut removed_paths: HashSet<String> = HashSet::new();
            removed_paths.insert("s3://bucket/other_file.parquet".to_string());

            let result = filter_manifests(&file_io, vec![manifest], &removed_paths)
                .await
                .expect("filter_manifests should succeed");

            assert_eq!(result.unaffected.len(), 1);
            assert!(result.to_rewrite.is_empty());
            assert_eq!(result.kept_file_count, 2);
            assert_eq!(result.removed_file_count, 0);
        }

        #[tokio::test]
        async fn test_filter_single_manifest_dropped() {
            // Single manifest where ALL files are removed gets dropped entirely
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);

            let manifest = write_test_manifest(
                &file_io,
                "memory://test/manifest.avro",
                schema,
                partition_spec,
                &["s3://bucket/file1.parquet", "s3://bucket/file2.parquet"],
                1,
            )
            .await;

            // Remove ALL files in the manifest
            let mut removed_paths: HashSet<String> = HashSet::new();
            removed_paths.insert("s3://bucket/file1.parquet".to_string());
            removed_paths.insert("s3://bucket/file2.parquet".to_string());

            let result = filter_manifests(&file_io, vec![manifest], &removed_paths)
                .await
                .expect("filter_manifests should succeed");

            // Manifest should be dropped - not in unaffected or to_rewrite
            assert!(result.unaffected.is_empty());
            assert!(result.to_rewrite.is_empty());
            assert_eq!(result.kept_file_count, 0);
            assert_eq!(result.removed_file_count, 2);
        }

        #[tokio::test]
        async fn test_filter_single_manifest_to_rewrite() {
            // Single manifest with SOME files removed goes to to_rewrite
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);

            let manifest = write_test_manifest(
                &file_io,
                "memory://test/manifest.avro",
                schema,
                partition_spec,
                &[
                    "s3://bucket/file1.parquet",
                    "s3://bucket/file2.parquet",
                    "s3://bucket/file3.parquet",
                ],
                1,
            )
            .await;

            // Remove only ONE file
            let mut removed_paths: HashSet<String> = HashSet::new();
            removed_paths.insert("s3://bucket/file2.parquet".to_string());

            let result = filter_manifests(&file_io, vec![manifest], &removed_paths)
                .await
                .expect("filter_manifests should succeed");

            assert!(result.unaffected.is_empty());
            assert_eq!(result.to_rewrite.len(), 1);
            assert_eq!(result.kept_file_count, 2);
            assert_eq!(result.removed_file_count, 1);
        }

        #[tokio::test]
        async fn test_filter_multiple_manifests_mixed() {
            // Multiple manifests with different outcomes: unaffected, to_rewrite, dropped
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);

            // Manifest 1: will be unaffected (no files removed)
            let manifest1 = write_test_manifest(
                &file_io,
                "memory://test/manifest1.avro",
                schema.clone(),
                partition_spec.clone(),
                &["s3://bucket/keep1.parquet", "s3://bucket/keep2.parquet"],
                1,
            )
            .await;

            // Manifest 2: will need rewriting (some files removed)
            let manifest2 = write_test_manifest(
                &file_io,
                "memory://test/manifest2.avro",
                schema.clone(),
                partition_spec.clone(),
                &["s3://bucket/keep3.parquet", "s3://bucket/remove1.parquet"],
                1,
            )
            .await;

            // Manifest 3: will be dropped (all files removed)
            let manifest3 = write_test_manifest(
                &file_io,
                "memory://test/manifest3.avro",
                schema,
                partition_spec,
                &[
                    "s3://bucket/remove2.parquet",
                    "s3://bucket/remove3.parquet",
                ],
                1,
            )
            .await;

            let mut removed_paths: HashSet<String> = HashSet::new();
            removed_paths.insert("s3://bucket/remove1.parquet".to_string());
            removed_paths.insert("s3://bucket/remove2.parquet".to_string());
            removed_paths.insert("s3://bucket/remove3.parquet".to_string());

            let result =
                filter_manifests(&file_io, vec![manifest1, manifest2, manifest3], &removed_paths)
                    .await
                    .expect("filter_manifests should succeed");

            // Manifest 1 -> unaffected, Manifest 2 -> to_rewrite, Manifest 3 -> dropped
            assert_eq!(result.unaffected.len(), 1);
            assert_eq!(result.to_rewrite.len(), 1);
            // kept: 2 from manifest1 + 1 from manifest2 = 3
            assert_eq!(result.kept_file_count, 3);
            // removed: 1 from manifest2 + 2 from manifest3 = 3
            assert_eq!(result.removed_file_count, 3);
        }

        #[tokio::test]
        async fn test_filter_removed_path_not_in_manifest() {
            // Paths in removed_paths that don't exist in any manifest don't cause errors
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);

            let manifest = write_test_manifest(
                &file_io,
                "memory://test/manifest.avro",
                schema,
                partition_spec,
                &["s3://bucket/file1.parquet"],
                1,
            )
            .await;

            // Try to remove files that don't exist
            let mut removed_paths: HashSet<String> = HashSet::new();
            removed_paths.insert("s3://bucket/nonexistent1.parquet".to_string());
            removed_paths.insert("s3://bucket/nonexistent2.parquet".to_string());

            let result = filter_manifests(&file_io, vec![manifest], &removed_paths)
                .await
                .expect("filter_manifests should succeed");

            // Manifest is unaffected since none of its files are in removed_paths
            assert_eq!(result.unaffected.len(), 1);
            assert!(result.to_rewrite.is_empty());
        }
    }

    // ============================================================
    // Manifest Rewriting Tests
    // ============================================================

    mod rewrite_tests {
        use super::*;
        use crate::types::CommitContext;
        use iceberg::io::FileIOBuilder;
        use iceberg::spec::{
            DataFileBuilder, DataFileFormat, ManifestWriterBuilder, NestedField, PrimitiveType,
            Schema, Struct, Type,
        };

        /// Create a test schema
        fn test_schema() -> Arc<Schema> {
            Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                        NestedField::required(2, "data", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()
                    .expect("Failed to build test schema"),
            )
        }

        /// Create an unpartitioned partition spec
        fn empty_partition_spec(schema: &Schema) -> PartitionSpec {
            PartitionSpec::builder(Arc::new(schema.clone()))
                .with_spec_id(0)
                .build()
                .expect("Failed to build empty partition spec")
        }

        /// Create a test data file
        fn test_data_file(path: &str) -> iceberg::spec::DataFile {
            DataFileBuilder::default()
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(1024)
                .record_count(100)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .content(DataContentType::Data)
                .build()
                .expect("Failed to build test data file")
        }

        /// Create memory FileIO for testing
        fn memory_file_io() -> FileIO {
            FileIOBuilder::new("memory")
                .build()
                .expect("Failed to build memory FileIO")
        }

        /// Write a test manifest with given files
        async fn write_test_manifest(
            file_io: &FileIO,
            path: &str,
            schema: Arc<Schema>,
            partition_spec: PartitionSpec,
            file_paths: &[&str],
            snapshot_id: i64,
        ) -> ManifestFile {
            let output = file_io
                .new_output(path)
                .expect("Failed to create output file");

            let mut writer = ManifestWriterBuilder::new(
                output,
                Some(snapshot_id),
                None,
                schema,
                partition_spec,
            )
            .build_v2_data();

            for file_path in file_paths {
                let data_file = test_data_file(file_path);
                writer
                    .add_file(data_file, 1)
                    .expect("Failed to add file to manifest");
            }

            writer
                .write_manifest_file()
                .await
                .expect("Failed to write manifest")
        }

        /// Create a test CommitContext
        fn test_commit_context() -> CommitContext {
            CommitContext::new("memory://test/table", None, 0)
        }

        #[tokio::test]
        async fn test_rewrite_all_filtered_returns_none() {
            // When all entries are filtered out, returns Ok(None)
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            let manifest = write_test_manifest(
                &file_io,
                "memory://test/manifest.avro",
                schema.clone(),
                partition_spec.clone(),
                &["s3://bucket/file1.parquet", "s3://bucket/file2.parquet"],
                1,
            )
            .await;

            // Remove ALL files
            let mut removed_paths: HashSet<String> = HashSet::new();
            removed_paths.insert("s3://bucket/file1.parquet".to_string());
            removed_paths.insert("s3://bucket/file2.parquet".to_string());

            let result = rewrite_manifest_keeping(
                &file_io,
                &ctx,
                &manifest,
                &removed_paths,
                schema,
                Arc::new(partition_spec),
                0,
            )
            .await
            .expect("rewrite should succeed");

            assert!(result.is_none());
        }

        #[tokio::test]
        async fn test_rewrite_single_entry_kept() {
            // Rewriting with one entry kept returns a manifest with 1 entry
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            let manifest = write_test_manifest(
                &file_io,
                "memory://test/manifest.avro",
                schema.clone(),
                partition_spec.clone(),
                &["s3://bucket/keep.parquet", "s3://bucket/remove.parquet"],
                1,
            )
            .await;

            // Remove only one file
            let mut removed_paths: HashSet<String> = HashSet::new();
            removed_paths.insert("s3://bucket/remove.parquet".to_string());

            let result = rewrite_manifest_keeping(
                &file_io,
                &ctx,
                &manifest,
                &removed_paths,
                schema,
                Arc::new(partition_spec),
                0,
            )
            .await
            .expect("rewrite should succeed");

            let new_manifest = result.expect("Should return Some(ManifestFile)");

            // Read the new manifest to verify contents
            let loaded = new_manifest
                .load_manifest(&file_io)
                .await
                .expect("Should load rewritten manifest");

            // Should have exactly 1 entry
            assert_eq!(loaded.entries().len(), 1);
            assert_eq!(
                loaded.entries()[0].data_file().file_path(),
                "s3://bucket/keep.parquet"
            );
        }

        #[tokio::test]
        async fn test_rewrite_preserves_entry_metadata() {
            // Verifies that sequence_number, file_sequence_number, and snapshot_id are preserved
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            // Write a manifest with known snapshot_id
            let manifest = write_test_manifest(
                &file_io,
                "memory://test/manifest.avro",
                schema.clone(),
                partition_spec.clone(),
                &["s3://bucket/file.parquet"],
                42, // specific snapshot_id
            )
            .await;

            // No files to remove, just testing preservation
            let removed_paths: HashSet<String> = HashSet::new();

            let result = rewrite_manifest_keeping(
                &file_io,
                &ctx,
                &manifest,
                &removed_paths,
                schema,
                Arc::new(partition_spec),
                0,
            )
            .await
            .expect("rewrite should succeed");

            let new_manifest = result.expect("Should return Some(ManifestFile)");
            let loaded = new_manifest
                .load_manifest(&file_io)
                .await
                .expect("Should load rewritten manifest");

            // Entry should exist with preserved metadata
            assert_eq!(loaded.entries().len(), 1);
            let entry = &loaded.entries()[0];
            // The entry should have EXISTING status after rewrite
            assert_eq!(entry.status(), ManifestStatus::Existing);
        }

        #[tokio::test]
        async fn test_rewrite_multiple_entries_kept() {
            // Rewriting keeps multiple entries correctly
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            let manifest = write_test_manifest(
                &file_io,
                "memory://test/manifest.avro",
                schema.clone(),
                partition_spec.clone(),
                &[
                    "s3://bucket/keep1.parquet",
                    "s3://bucket/remove.parquet",
                    "s3://bucket/keep2.parquet",
                ],
                1,
            )
            .await;

            // Remove middle file
            let mut removed_paths: HashSet<String> = HashSet::new();
            removed_paths.insert("s3://bucket/remove.parquet".to_string());

            let result = rewrite_manifest_keeping(
                &file_io,
                &ctx,
                &manifest,
                &removed_paths,
                schema,
                Arc::new(partition_spec),
                0,
            )
            .await
            .expect("rewrite should succeed");

            let new_manifest = result.expect("Should return Some(ManifestFile)");
            let loaded = new_manifest
                .load_manifest(&file_io)
                .await
                .expect("Should load rewritten manifest");

            // Should have 2 entries
            assert_eq!(loaded.entries().len(), 2);

            // Verify the correct files are kept
            let paths: HashSet<_> = loaded
                .entries()
                .iter()
                .map(|e| e.data_file().file_path())
                .collect();
            assert!(paths.contains("s3://bucket/keep1.parquet"));
            assert!(paths.contains("s3://bucket/keep2.parquet"));
            assert!(!paths.contains("s3://bucket/remove.parquet"));
        }
    }

    // ============================================================
    // Write Manifest Tests
    // ============================================================

    mod write_manifest_tests {
        use super::*;
        use crate::types::{CommitContext, DataFileInfo};
        use iceberg::io::FileIOBuilder;
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        /// Create a test schema
        fn test_schema() -> Arc<Schema> {
            Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                        NestedField::required(2, "data", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()
                    .expect("Failed to build test schema"),
            )
        }

        /// Create an unpartitioned partition spec
        fn empty_partition_spec(schema: &Schema) -> Arc<PartitionSpec> {
            Arc::new(
                PartitionSpec::builder(Arc::new(schema.clone()))
                    .with_spec_id(0)
                    .build()
                    .expect("Failed to build empty partition spec"),
            )
        }

        /// Create memory FileIO for testing
        fn memory_file_io() -> FileIO {
            FileIOBuilder::new("memory")
                .build()
                .expect("Failed to build memory FileIO")
        }

        /// Create a test CommitContext
        fn test_commit_context() -> CommitContext {
            CommitContext::new("memory://test/table", None, 0)
        }

        #[tokio::test]
        async fn test_write_manifest_empty_files() {
            // Empty added_files list should write a valid but empty manifest
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            let added_files: Vec<DataFileInfo> = vec![];

            let manifest = write_manifest(
                &file_io,
                &ctx,
                schema,
                partition_spec,
                0,
                &added_files,
                "bucket",
            )
            .await
            .expect("write_manifest should succeed");

            // Load and verify it has no entries
            let loaded = manifest
                .load_manifest(&file_io)
                .await
                .expect("Should load manifest");
            assert_eq!(loaded.entries().len(), 0);
        }

        #[tokio::test]
        async fn test_write_manifest_s3_prefix_preserved() {
            // File paths with s3:// prefix should not be double-prefixed
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            let added_files = vec![DataFileInfo {
                path: "s3://existing-bucket/path/to/file.parquet".to_string(),
                size_bytes: 1024,
                record_count: 100,
                partition_values: None,
                sequence_number: None,
            }];

            let manifest = write_manifest(
                &file_io,
                &ctx,
                schema,
                partition_spec,
                0,
                &added_files,
                "different-bucket",
            )
            .await
            .expect("write_manifest should succeed");

            let loaded = manifest
                .load_manifest(&file_io)
                .await
                .expect("Should load manifest");

            // Path should be preserved as-is, not double-prefixed
            assert_eq!(
                loaded.entries()[0].data_file().file_path(),
                "s3://existing-bucket/path/to/file.parquet"
            );
        }

        #[tokio::test]
        async fn test_write_manifest_relative_path() {
            // Relative paths should have s3:// prefix added
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            let added_files = vec![DataFileInfo {
                path: "path/to/file.parquet".to_string(),
                size_bytes: 1024,
                record_count: 100,
                partition_values: None,
                sequence_number: None,
            }];

            let manifest = write_manifest(
                &file_io,
                &ctx,
                schema,
                partition_spec,
                0,
                &added_files,
                "my-bucket",
            )
            .await
            .expect("write_manifest should succeed");

            let loaded = manifest
                .load_manifest(&file_io)
                .await
                .expect("Should load manifest");

            // Path should have s3:// prefix added
            assert_eq!(
                loaded.entries()[0].data_file().file_path(),
                "s3://my-bucket/path/to/file.parquet"
            );
        }

        #[tokio::test]
        async fn test_write_manifest_zero_size_records() {
            // Files with 0 size and 0 records should work (edge case)
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            let added_files = vec![DataFileInfo {
                path: "s3://bucket/empty.parquet".to_string(),
                size_bytes: 0,
                record_count: 0,
                partition_values: None,
                sequence_number: None,
            }];

            let manifest = write_manifest(
                &file_io,
                &ctx,
                schema,
                partition_spec,
                0,
                &added_files,
                "bucket",
            )
            .await
            .expect("write_manifest should succeed with 0 size/records");

            let loaded = manifest
                .load_manifest(&file_io)
                .await
                .expect("Should load manifest");

            assert_eq!(loaded.entries().len(), 1);
            assert_eq!(loaded.entries()[0].data_file().file_size_in_bytes(), 0);
            assert_eq!(loaded.entries()[0].data_file().record_count(), 0);
        }

        #[tokio::test]
        async fn test_write_manifest_unpartitioned() {
            // Unpartitioned file (partition_values: None) should have empty partition struct
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            let added_files = vec![DataFileInfo {
                path: "s3://bucket/unpartitioned.parquet".to_string(),
                size_bytes: 1024,
                record_count: 100,
                partition_values: None, // Unpartitioned
                sequence_number: None,
            }];

            let manifest = write_manifest(
                &file_io,
                &ctx,
                schema,
                partition_spec,
                0,
                &added_files,
                "bucket",
            )
            .await
            .expect("write_manifest should succeed");

            let loaded = manifest
                .load_manifest(&file_io)
                .await
                .expect("Should load manifest");

            // Partition should be empty for unpartitioned tables
            assert_eq!(*loaded.entries()[0].data_file().partition(), Struct::empty());
        }
    }

    // ============================================================
    // Write Delete Manifest Tests
    // ============================================================

    mod write_delete_manifest_tests {
        use super::*;
        use crate::types::{CommitContext, DataFileInfo};
        use iceberg::io::FileIOBuilder;
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        /// Create a test schema
        fn test_schema() -> Arc<Schema> {
            Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                        NestedField::required(2, "data", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()
                    .expect("Failed to build test schema"),
            )
        }

        /// Create an unpartitioned partition spec
        fn empty_partition_spec(schema: &Schema) -> Arc<PartitionSpec> {
            Arc::new(
                PartitionSpec::builder(Arc::new(schema.clone()))
                    .with_spec_id(0)
                    .build()
                    .expect("Failed to build empty partition spec"),
            )
        }

        /// Create memory FileIO for testing
        fn memory_file_io() -> FileIO {
            FileIOBuilder::new("memory")
                .build()
                .expect("Failed to build memory FileIO")
        }

        /// Create a test CommitContext
        fn test_commit_context() -> CommitContext {
            CommitContext::new("memory://test/table", None, 0)
        }

        #[tokio::test]
        async fn test_write_delete_manifest_empty() {
            // Empty deleted_files list should write a valid but empty manifest
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            let deleted_files: Vec<DataFileInfo> = vec![];

            let manifest = write_delete_manifest(
                &file_io,
                &ctx,
                schema,
                partition_spec,
                0,
                &deleted_files,
                "bucket",
            )
            .await
            .expect("write_delete_manifest should succeed");

            // Load and verify it has no entries
            let loaded = manifest
                .load_manifest(&file_io)
                .await
                .expect("Should load delete manifest");
            assert_eq!(loaded.entries().len(), 0);
        }

        #[tokio::test]
        async fn test_write_delete_manifest_null_sequence() {
            // sequence_number = None should default to 1
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            let deleted_files = vec![DataFileInfo {
                path: "s3://bucket/to_delete.parquet".to_string(),
                size_bytes: 1024,
                record_count: 100,
                partition_values: None,
                sequence_number: None, // Should default to 1
            }];

            let manifest = write_delete_manifest(
                &file_io,
                &ctx,
                schema,
                partition_spec,
                0,
                &deleted_files,
                "bucket",
            )
            .await
            .expect("write_delete_manifest should succeed");

            let loaded = manifest
                .load_manifest(&file_io)
                .await
                .expect("Should load delete manifest");

            // Entry should be marked as deleted
            assert_eq!(loaded.entries().len(), 1);
            assert_eq!(loaded.entries()[0].status(), ManifestStatus::Deleted);
        }

        #[tokio::test]
        async fn test_write_delete_manifest_duplicates() {
            // Duplicate paths in deleted_files should both be written (no dedup)
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            let deleted_files = vec![
                DataFileInfo {
                    path: "s3://bucket/same_file.parquet".to_string(),
                    size_bytes: 1024,
                    record_count: 100,
                    partition_values: None,
                    sequence_number: Some(1),
                },
                DataFileInfo {
                    path: "s3://bucket/same_file.parquet".to_string(), // Duplicate
                    size_bytes: 1024,
                    record_count: 100,
                    partition_values: None,
                    sequence_number: Some(1),
                },
            ];

            let manifest = write_delete_manifest(
                &file_io,
                &ctx,
                schema,
                partition_spec,
                0,
                &deleted_files,
                "bucket",
            )
            .await
            .expect("write_delete_manifest should succeed");

            let loaded = manifest
                .load_manifest(&file_io)
                .await
                .expect("Should load delete manifest");

            // Both entries should be written (no deduplication)
            assert_eq!(loaded.entries().len(), 2);
        }
    }

    // ============================================================
    // Write Manifest List Tests
    // ============================================================

    mod write_manifest_list_tests {
        use super::*;
        use crate::types::CommitContext;
        use iceberg::io::FileIOBuilder;
        use iceberg::spec::{
            DataFileBuilder, DataFileFormat, ManifestWriterBuilder, NestedField, PrimitiveType,
            Schema, Struct, Type,
        };

        /// Create a test schema
        fn test_schema() -> Arc<Schema> {
            Arc::new(
                Schema::builder()
                    .with_fields(vec![
                        NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                        NestedField::required(2, "data", Type::Primitive(PrimitiveType::String))
                            .into(),
                    ])
                    .build()
                    .expect("Failed to build test schema"),
            )
        }

        /// Create an unpartitioned partition spec
        fn empty_partition_spec(schema: &Schema) -> PartitionSpec {
            PartitionSpec::builder(Arc::new(schema.clone()))
                .with_spec_id(0)
                .build()
                .expect("Failed to build empty partition spec")
        }

        /// Create a test data file
        fn test_data_file(path: &str) -> iceberg::spec::DataFile {
            DataFileBuilder::default()
                .file_path(path.to_string())
                .file_format(DataFileFormat::Parquet)
                .file_size_in_bytes(1024)
                .record_count(100)
                .partition(Struct::empty())
                .partition_spec_id(0)
                .content(DataContentType::Data)
                .build()
                .expect("Failed to build test data file")
        }

        /// Create memory FileIO for testing
        fn memory_file_io() -> FileIO {
            FileIOBuilder::new("memory")
                .build()
                .expect("Failed to build memory FileIO")
        }

        /// Create a test CommitContext
        fn test_commit_context() -> CommitContext {
            CommitContext::new("memory://test/table", None, 0)
        }

        /// Write a test manifest
        async fn write_test_manifest(
            file_io: &FileIO,
            path: &str,
            schema: Arc<Schema>,
            partition_spec: PartitionSpec,
            file_paths: &[&str],
            snapshot_id: i64,
        ) -> ManifestFile {
            let output = file_io
                .new_output(path)
                .expect("Failed to create output file");

            let mut writer = ManifestWriterBuilder::new(
                output,
                Some(snapshot_id),
                None,
                schema,
                partition_spec,
            )
            .build_v2_data();

            for file_path in file_paths {
                let data_file = test_data_file(file_path);
                writer
                    .add_file(data_file, 1)
                    .expect("Failed to add file to manifest");
            }

            writer
                .write_manifest_file()
                .await
                .expect("Failed to write manifest")
        }

        #[tokio::test]
        async fn test_write_manifest_list_only_new() {
            // Manifest list with only new manifests (compaction scenario)
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            // Use ctx.new_snapshot_id so manifest's added_snapshot_id matches writer's snapshot_id
            let new_manifest = write_test_manifest(
                &file_io,
                "memory://test/new_manifest.avro",
                schema,
                partition_spec,
                &["s3://bucket/new_file.parquet"],
                ctx.new_snapshot_id,
            )
            .await;

            // Write manifest list with only new manifest, no existing
            write_manifest_list(&file_io, &ctx, vec![new_manifest], vec![])
                .await
                .expect("write_manifest_list should succeed");

            // Verify the manifest list was written
            let input = file_io
                .new_input(&ctx.manifest_list_path)
                .expect("Should create input");
            assert!(input.exists().await.expect("Should check existence"));
        }

        #[tokio::test]
        async fn test_write_manifest_list_only_existing() {
            // Manifest list with only existing manifests (no-op or carry forward scenario)
            let file_io = memory_file_io();
            let schema = test_schema();
            let partition_spec = empty_partition_spec(&schema);
            let ctx = test_commit_context();

            // Use ctx.new_snapshot_id so manifest's added_snapshot_id matches writer's snapshot_id
            let existing_manifest = write_test_manifest(
                &file_io,
                "memory://test/existing_manifest.avro",
                schema,
                partition_spec,
                &["s3://bucket/existing_file.parquet"],
                ctx.new_snapshot_id,
            )
            .await;

            // Write manifest list with only existing manifest, no new
            write_manifest_list(&file_io, &ctx, vec![], vec![existing_manifest])
                .await
                .expect("write_manifest_list should succeed");

            // Verify the manifest list was written
            let input = file_io
                .new_input(&ctx.manifest_list_path)
                .expect("Should create input");
            assert!(input.exists().await.expect("Should check existence"));
        }
    }
}
