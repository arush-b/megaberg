# Coordinator-Worker Contract

Interface for parallel development. Coordinator and Worker can be built independently.

---

## CompactionTask (Coordinator → Worker)

```rust
pub struct CompactionTask {
    /// Unique identifier for this task
    pub task_id: String,

    /// Partition key (e.g., "date=2024-01-01/region=us-east")
    pub partition: String,

    /// Files to compact
    pub input_files: Vec<InputFile>,

    /// Where to write the compacted file
    pub output_path: String,
}

pub struct InputFile {
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
}
```

**Coordinator generates `output_path`:**
```
{table_location}/data/{partition}/compacted-{uuid}.parquet
```

---

## CompactionResult (Worker → Coordinator)

```rust
pub struct CompactionResult {
    /// Matches task_id from CompactionTask
    pub task_id: String,

    /// Input files that were processed (echoed back for commit)
    pub input_files: Vec<String>,

    /// Output stats - None if failed
    pub output_stats: Option<OutputStats>,

    /// Error if failed
    pub error: Option<CompactionError>,
}

pub struct OutputStats {
    /// Actual size after compression
    pub size_bytes: u64,

    /// Actual record count after merge
    pub record_count: u64,
}
```

**Success:** `output_stats = Some(...)`, `error = None`
**Failure:** `output_stats = None`, `error = Some(CompactionError::...)`

---

## CompactionError

```rust
#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    /// Failed to read input file from storage
    #[error("read error: {path}: {source}")]
    ReadError {
        path: String,
        #[source]
        source: anyhow::Error,
    },

    /// Failed to write output file to storage
    #[error("write error: {path}: {source}")]
    WriteError {
        path: String,
        #[source]
        source: anyhow::Error,
    },

    /// Input file not found (may have been deleted)
    #[error("file not found: {path}")]
    FileNotFound { path: String },

    /// Parquet/Arrow processing error
    #[error("processing error: {0}")]
    ProcessingError(#[from] anyhow::Error),
}
```

Worker returns specific error variant so coordinator can decide how to handle (e.g., `FileNotFound` means skip group, `WriteError` might be retryable).

---

## Responsibilities

| Component | Responsibilities |
|-----------|------------------|
| **Coordinator** | Discovery, task creation, generate output_path, dispatch, collect results, commit, sidecar |
| **Worker** | Read input files, merge, write to output_path, return stats |

---

## Flow

```
Coordinator:
  1. Discovery → Vec<FileGroup>
  2. For each group:
     - Generate output_path = {table}/data/{partition}/compacted-{uuid}.parquet
     - Create CompactionTask
     - Send to work queue

Worker:
  1. Receive CompactionTask
  2. Read input_files from storage
  3. Merge via DataFusion
  4. Write to output_path
  5. Return CompactionResult with stats

Coordinator:
  6. Collect all results
  7. Filter successful (output_stats.is_some())
  8. Build manifest: input_files → DELETED, output_path → ADDED
  9. Commit to catalog
```

---

## Commit Retry

On conflict (snapshot changed):

```
1. Re-read current snapshot
2. Validate: input files still exist?
   - No → abort (files deleted by another writer)
   - Yes → retry commit with same output files
3. Max 3 retries, then abort run
```

Compacted files already written are reused on retry - no re-compaction needed.

---

## Orphan Files

If commit fails after workers write files:
- Files exist on S3 but not referenced in any manifest
- Harmless for queries (only manifest-referenced files are read)
- Cleanup tool removes orphans periodically
