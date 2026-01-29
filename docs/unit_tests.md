# Committer Crate Unit Tests

## Partition Building (`build_partition`)

| # | Test Name | Description | Notes |
|---|-----------|-------------|-------|
| 1 | `test_build_partition_empty_spec` | 0 partition fields in spec | Should return `Struct::empty()` |
| 2+3 | `test_build_partition_none_or_empty_values` | `partition_values` is `None` OR `Some({})` | Both should produce struct with all nulls |
| 4 | `test_build_partition_all_fields_present` | Map has all fields | Values should be in correct spec order |
| 5 | `test_build_partition_missing_fields` | Map missing some fields | Missing fields become null |
| 6 | `test_build_partition_extra_fields_ignored` | Map has extra fields not in spec | Extra keys silently ignored |
| 7 | `test_build_partition_field_ordering` | Map fields in different order than spec | Output follows spec order, not map order |

## Value Parsing (`parse_partition_value`)

| # | Test Name | Description | Notes |
|---|-----------|-------------|-------|
| 8 | `test_parse_zero` | "0" | Should become `Int(0)` |
| 14 | `test_parse_leading_zeros` | "007" | Should become `Int(7)` (parsed as number) |
| 17 | `test_parse_empty_string` | "" | Should become `String("")` |
| 18 | `test_parse_whitespace_not_trimmed` | " 123 " | Should become `String(" 123 ")` (no trimming) |
| 19 | `test_parse_null_strings` | "null", "NULL" | Should become `String("null")` etc. (not special-cased) |

## Manifest Filtering (`filter_manifests`)

| # | Test Name | Description | Notes |
|---|-----------|-------------|-------|
| 20 | `test_filter_empty_manifest_list` | Empty manifest list | Returns empty unaffected and to_rewrite |
| 21 | `test_filter_empty_removed_set` | Empty removed_paths set | All manifests go to unaffected |
| 22 | `test_filter_manifest_only_deleted_entries` | Manifest with 0 non-DELETED entries | Treated as empty, dropped |
| 23 | `test_filter_all_entries_deleted_status` | Manifest where all entries already DELETED status | Same as 22, manifest dropped |
| 24 | `test_filter_deleted_entries_skipped` | DELETED entries with paths matching removed_paths | DELETED entries ignored when checking |
| 25 | `test_filter_removed_path_not_in_manifest` | Path in removed_paths doesn't exist in any manifest | No error, manifest unaffected |
| 26 | `test_filter_same_path_multiple_manifests` | Same path appears in multiple manifests | Path removed from all affected manifests |
| 27 | `test_filter_delete_manifest_passthrough` | Delete manifest type (ManifestContentType::Deletes) | Always goes to unaffected, never inspected |
| 28 | `test_filter_single_manifest_unaffected` | Single manifest, no files removed | Goes to unaffected list |
| 29 | `test_filter_single_manifest_dropped` | Single manifest, all files removed | Dropped entirely (not in any list) |
| 30* | `test_filter_single_manifest_to_rewrite` | Single manifest, some files removed | Goes to to_rewrite list |
| 31* | `test_filter_multiple_manifests_mixed` | Multiple manifests with mixed outcomes | Each classified correctly |

**Notes:**
- 30*: Need to verify counts (kept_file_count, removed_file_count) are accurate
- 31*: Good integration-style test covering all 4 outcomes in one test

## Manifest Rewriting (`rewrite_manifest_keeping`)

| # | Test Name | Description | Notes |
|---|-----------|-------------|-------|
| 32 | `test_rewrite_all_filtered_returns_none` | All entries filtered out | Returns `Ok(None)` |
| 33 | `test_rewrite_single_entry_kept` | Single entry kept | Returns manifest with 1 entry |
| 34 | `test_rewrite_preserves_sequence_number` | `sequence_number` is None | Defaults to 1 |
| 35 | `test_rewrite_preserves_file_sequence_number` | `file_sequence_number` is None | Defaults to seq_num |
| 36 | `test_rewrite_preserves_snapshot_id` | `snapshot_id` is None | Defaults to 0 |
| 37 | `test_rewrite_skips_deleted_entries` | DELETED entries in source manifest | Not copied to new manifest |

## CommitContext

| # | Test Name | Description | Notes |
|---|-----------|-------------|-------|
| 39 | `test_context_first_snapshot` | First snapshot: `parent_snapshot_id = None`, `current_seq = 0` | new_sequence_number = 1 |
| 40* | `test_context_subsequent_snapshot` | Subsequent snapshot with parent | parent_snapshot_id set, seq increments |
| 41 | `test_context_manifest_path_format` | Path format check | `{location}/metadata/{uuid}-m0.avro` |
| 42 | `test_context_manifest_list_path_format` | Manifest list path format | `{location}/metadata/snap-{id}-0-{uuid}.avro` |

**Notes:**
- 40*: Should verify sequence = parent_seq + 1

## Write Manifest (`write_manifest`)

| # | Test Name | Description | Notes |
|---|-----------|-------------|-------|
| 44 | `test_write_manifest_empty_files` | Empty added_files list | Should write valid empty manifest |
| 45 | `test_write_manifest_s3_prefix_preserved` | File path already has `s3://` prefix | Don't double-prefix |
| 46 | `test_write_manifest_relative_path` | File path is relative | Add s3:// prefix |
| 47 | `test_write_manifest_zero_size_records` | File with 0 size / 0 record count | Should work (edge case) |
| 48* | `test_write_manifest_unpartitioned` | Unpartitioned file (`partition_values: None`) | partition struct should be empty |

**Notes:**
- 48*: Important for unpartitioned tables

## Write Delete Manifest (`write_delete_manifest`)

| # | Test Name | Description | Notes |
|---|-----------|-------------|-------|
| 49 | `test_write_delete_manifest_empty` | Empty deleted_files list | Valid but empty manifest |
| 50 | `test_write_delete_manifest_null_sequence` | `sequence_number = None` | Defaults to 1 |
| 51 | `test_write_delete_manifest_duplicates` | Duplicate paths in deleted_files | Both entries written (no dedup) |

## Write Manifest List (`write_manifest_list`)

| # | Test Name | Description | Notes |
|---|-----------|-------------|-------|
| 53 | `test_write_manifest_list_only_new` | Only new manifests | Valid manifest list |
| 54 | `test_write_manifest_list_only_existing` | Only existing manifests | Valid manifest list |

**Question:** Are 52 (empty both), 53, 54 the only required tests for manifest list?

**Answer:** Probably yes. The function is simple - it just combines two lists and writes them. The interesting edge cases are:
- Empty input (covered by having 52 if needed, but an empty manifest list might be invalid in Iceberg)
- Only new (53) - compaction scenario
- Only existing (54) - no-op commit scenario
- Both (implicit in normal operation)

The ordering (new first, then existing) could be tested but is implementation detail.

## Glue Commit (`commit_with_table`)

| # | Test Name | Description | Notes |
|---|-----------|-------------|-------|
| 55 | `test_commit_empty_added_and_removed` | Empty added AND removed | Early return, no commit |
| 56 | `test_commit_only_added_files` | Only added files (append) | Normal append operation |
| 57 | `test_commit_no_current_snapshot` | No current snapshot (first commit) | Handle None parent |
| 58 | `test_commit_glue_update_fails` | Metadata write succeeds but Glue update fails | Orphaned metadata scenario |

---

## Test Implementation Notes

### Pure Logic Tests (no FileIO needed)
- Partition Building (1-7)
- Value Parsing (8, 14, 17-19)
- CommitContext (39-42)

### Tests Requiring FileIO Mock/Stub
- Manifest Filtering (20-31)
- Manifest Rewriting (32-37)
- Write Manifest (44-48)
- Write Delete Manifest (49-51)
- Write Manifest List (53-54)

### Tests Requiring Glue Mock
- Glue Commit (55-58)

---

## Total: 42 tests
