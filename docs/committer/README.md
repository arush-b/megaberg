# Committer Service Design

## Overview

The committer is responsible for atomically updating Iceberg table metadata after data operations (ingestion, compaction). This document outlines the design for extracting the committer as a standalone service that can be shared between Megaberg (compaction) and Laminar (ingestion).

---

## CommitRequest Structure

```rust
pub enum TableProvider {
    RawParquet,  // Just data files, no catalog
    Iceberg,     // Full Iceberg with catalog
}

pub struct CommitRequest {
    pub provider: TableProvider,
    pub table_location: String,          // s3://bucket/path/
    pub namespace: Option<String>,       // Only for Iceberg
    pub table_name: Option<String>,      // Only for Iceberg
    pub added_data_files: Vec<DataFileInfo>,
    pub deleted_data_files: Vec<String>, // Only for compaction
}

pub struct DataFileInfo {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
    pub partition_values: HashMap<String, String>,
}
```

### Field Semantics

| Field | Purpose |
|-------|---------|
| `added_data_files` | New data files to add to table (ingestion output, compaction output) |
| `deleted_data_files` | Data files to remove from table (compaction input files being replaced) |

### Future Extension (Full 4-Vector Model)

When delete file support is needed:

```rust
pub struct CommitRequest {
    // ... existing fields ...
    pub added_delete_files: Vec<DeleteFileInfo>,    // For row-level deletes
    pub deleted_delete_files: Vec<String>,          // After compaction applies deletes
}
```

---

## Current Architecture Analysis

### Planner → Executor → Committer Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                           main.rs                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. Load Config                                                     │
│       ↓                                                             │
│  2. load_table_info() ──→ Catalog (Glue/REST)                      │
│       ↓                    Returns: TableInfo {location, snapshot}  │
│  3. IcebergDiscovery::discover()                                    │
│       ↓                    Returns: PartitionFileMap                │
│  4. pack_files()                                                    │
│       ↓                    Returns: Vec<FileGroup>                  │
│  5. build_tasks()                                                   │
│       ↓                    Returns: Vec<CompactionTask>             │
│  6. Executor runs tasks                                             │
│       ↓                    Returns: Vec<WrittenFile>                │
│  7. Convert to Vec<TaskResult>                                      │
│       ↓                                                             │
│  8. commit_compaction() or commit_compaction_glue()                 │
│       ↓                    Updates catalog                          │
│  Done                                                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Communication Between Components

**Planner → Executor:**
```rust
// CompactionTask sent to executor
pub struct CompactionTask {
    pub task_id: String,
    pub partition: String,
    pub input_files: Vec<String>,    // S3 paths to read
    pub output_prefix: String,       // S3 prefix to write to
}
```

**Executor → Committer:**
```rust
// TaskResult passed to committer
pub struct TaskResult {
    pub input_files: Vec<String>,    // Files to delete
    pub output_files: Vec<OutputFile>, // Files to add
}

pub struct OutputFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}
```

---

## Concurrency Control Analysis

### Current State: NO Concurrency Control

The current implementation has **no explicit concurrency control**:

1. **Single Process**: Megaberg runs as a single CLI process
2. **Sequential Commit**: Only one commit happens at the end of the run
3. **Optimistic Locking (REST only)**: Uses `TableRequirement::RefSnapshotIdMatch`

```rust
// rest.rs - Line 76
let requirements = build_requirements(parent_snapshot_id);

// This creates:
TableRequirement::RefSnapshotIdMatch {
    ref_name: "main",
    snapshot_id: parent_snapshot_id,  // Expected current snapshot
}
```

### What Happens on Conflict?

**REST Catalog:**
- If another process committed between planning and commit
- The `RefSnapshotIdMatch` requirement fails
- Commit is rejected with HTTP 409/412
- **No automatic retry** - megaberg just fails

**Glue Catalog:**
- Uses `update_table()` API
- **No optimistic locking** in current implementation
- Last writer wins (potential data loss)
- If two processes commit simultaneously, one's changes may be lost

### The Problem for Laminar (100 Pipelines)

```
Pipeline 1 ─┐
Pipeline 2 ─┼──→ All read snapshot 5
Pipeline 3 ─┘
     ↓
All try to commit as snapshot 6
     ↓
Only one succeeds, others fail (REST) or overwrite (Glue)
```

---

## Committer Responsibilities

### Core Responsibilities

1. **Manifest Management**
   - Create new manifest files (Avro) with ADD entries
   - Create DELETE entries for removed files
   - Write to `{table_location}/metadata/{uuid}-m0.avro`

2. **Manifest List Management**
   - Combine new manifest with existing manifests
   - Write to `{table_location}/metadata/snap-{id}-{uuid}.avro`

3. **Snapshot Creation**
   - Generate snapshot ID (timestamp-based)
   - Increment sequence number
   - Build summary (file counts, record counts, operation type)

4. **Metadata JSON (for Glue)**
   - Build new `metadata.json` with updated snapshot refs
   - Write to `{table_location}/metadata/{seq}-{uuid}.metadata.json`

5. **Catalog Update**
   - REST: POST with optimistic locking
   - Glue: Update `metadata_location` parameter

### What It Does NOT Do

- Read/write data files (executor's job)
- Decide what to compact (planner's job)
- Schema management
- Partition discovery

---

## Proposed Service Architecture

For 100+ concurrent pipelines, a centralized service is recommended:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Committer Service                           │
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
│  │ gRPC Server │───→│ Commit Queue│───→│ Sequential Committer│ │
│  └─────────────┘    └─────────────┘    └─────────────────────┘ │
│        ↑                                       │                │
│        │                                       ↓                │
│        │                                ┌──────────┐            │
│        │                                │ Catalog  │            │
│   ┌────┴────┐                           └──────────┘            │
│   │ Clients │                                                   │
│   ├─────────┤                                                   │
│   │ Laminar │  (100 pipelines)                                  │
│   │ Megaberg│  (compaction)                                     │
│   └─────────┘                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Benefits

1. **Serialized commits** - No conflicts between pipelines
2. **Single point of catalog interaction** - Easier to manage
3. **Batching opportunity** - Combine multiple commits
4. **Retry logic centralized** - Handle failures once

---

## Provider-Specific Behavior

| Action | RawParquet | Iceberg |
|--------|------------|---------|
| Write data files | Already done by caller | Already done by caller |
| Track in manifest | No | Yes |
| Delete from manifest | No | Yes |
| Update catalog | No | Yes |
| Physical file deletion | Direct S3 delete | Via expiration |

```rust
impl Committer {
    pub async fn commit(&self, req: CommitRequest) -> Result<()> {
        match req.provider {
            TableProvider::RawParquet => {
                // Just delete old files from S3 if needed
                if !req.deleted_data_files.is_empty() {
                    self.s3_delete(req.deleted_data_files).await?;
                }
                Ok(())
            }
            TableProvider::Iceberg => {
                self.commit_iceberg(req).await
            }
        }
    }
}
```

---

## Current Code Location

| Component | Location |
|-----------|----------|
| Commit types | `crates/megaberg/src/commit/types.rs` |
| Manifest operations | `crates/megaberg/src/commit/manifest.rs` |
| Snapshot building | `crates/megaberg/src/commit/snapshot.rs` |
| REST commit | `crates/megaberg/src/commit/rest.rs` |
| Glue commit | `crates/megaberg/src/commit/glue.rs` |
| Orchestration | `crates/megaberg/src/main.rs` (lines 483-563) |

---

## Next Steps

1. Extract commit logic into `crates/committer/` as a library
2. Add proper concurrency control (distributed lock or queue)
3. Add gRPC interface for service mode
4. Update Laminar to use committer service
5. Update Megaberg to optionally use committer service
