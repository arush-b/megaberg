# Megaberg Engine Architecture

## Overview

Batch compaction engine: start → discover → compact → finalize → terminate.

Single-node execution, compute for discovery and compaction parallelized.

---

## Execution Model

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              ENGINE (single node)                           │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        PHASE 1: DISCOVERY                             │  │
│  │                                                                       │  │
│  │   Sequential                          Parallel                        │  │
│  │  ┌─────────────┐                    ┌─────────────────────────────┐   │  │
│  │  │ Load state  │                    │   Manifest Scanning         │   │  │
│  │  │ Read meta   │──────────────────▶ │   (N concurrent tasks)      │   │  │
│  │  │ Read list   │                    │   M1 ─┐                     │   │  │
│  │  │ Filter      │                    │   M2 ─┼─▶ Merge ─▶ Bin-pack │   │  │
│  │  └─────────────┘                    │   M3 ─┘                     │   │  │
│  │                                     └─────────────────────────────┘   │  │
│  │                                                   │                   │  │
│  │                                                   ▼                   │  │
│  │                                            Vec<FileGroup>             │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                           │                                 │
│                                           ▼                                 │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        PHASE 2: COMPACTION                            │  │
│  │                                                                       │  │
│  │                    Parallel Workers (N concurrent)                    │  │
│  │                                                                       │  │
│  │   ┌─────────────────────────────────────────────────────────────┐     │  │
│  │   │                    Shared Work Queue                        │     │  │
│  │   │              [G1] [G2] [G3] [G4] [G5] ...                   │     │  │
│  │   └────────┬─────────────┬─────────────┬────────────────────────┘     │  │
│  │            │             │             │                              │  │
│  │            ▼             ▼             ▼                              │  │
│  │      ┌──────────┐  ┌──────────┐  ┌──────────┐                         │  │
│  │      │ Worker 0 │  │ Worker 1 │  │ Worker N │   Each worker:          │  │
│  │      │          │  │          │  │          │   - Read (I/O)          │  │
│  │      │ G1 ────▶ │  │ G2 ────▶ │  │ G3 ────▶ │   - Merge (CPU)         │  │
│  │      │  result  │  │  result  │  │  result  │   - Write (I/O)         │  │
│  │      └────┬─────┘  └────┬─────┘  └────┬─────┘   - Return result       │  │
│  │           │             │             │                               │  │
│  │           └─────────────┴─────────────┘                               │  │
│  │                         │                                             │  │
│  │                         ▼                                             │  │
│  │                   Result Channel                                      │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                           │                                 │
│                                           ▼                                 │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        PHASE 3: FINALIZE                              │  │
│  │                                                                       │  │
│  │   Sequential: Collect results → Commit → Update sidecar → Summary     │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

**Catalog + Storage are siblings (Option B):** Configured independently for flexibility (REST catalog + MinIO storage, Glue + cross-account S3, etc.)

---

## Phase 1: Discovery (Separate Module)

Discovery is a standalone module called by the engine. Returns `Vec<FileGroup>`.

**Source of truth:** Partition. Files within a partition are compacted together. Never merge files across partitions.

---

### Step 1.1: Load Compaction State (Sidecar)

Read sidecar: `s3://bucket/table/.megaberg/compaction_state.json`

```json
{
  "last_processed_snapshot": 8,
  "partition_state": {
    "(2024-01-01, us-east)": { "last_compacted_snapshot": 8 },
    "(2024-01-02, us-east)": { "last_compacted_snapshot": 5 }
  }
}
```

- `last_processed_snapshot` - which snapshot we last scanned up to
- `partition_state` - per-partition, when it was last compacted

If missing → first run, full scan mode.

---

### Step 1.2: Read Table Metadata

Read `metadata.json` from catalog-provided location.

Extract:
- `current-snapshot-id`
- `snapshots[]` → find current snapshot entry → get `manifest-list` path
- `partition-spec` → defines partition columns (e.g., `[date, region]`)

---

### Step 1.3: Read Manifest List

Read `manifest-list.avro` from current snapshot.

Each entry contains:
```json
{
  "manifest_path": "s3://bucket/table/metadata/snap-8-m0.avro",
  "added_snapshot_id": 8,
  "content": 0,
  "partition_summaries": [
    { "lower_bound": "2024-01-01", "upper_bound": "2024-01-05" },
    { "lower_bound": "us-east", "upper_bound": "us-west" }
  ]
}
```

- `content`: 0 = data files, 1 = delete files (skip deletes for now)
- `partition_summaries`: bounds per partition field, maps 1:1 to partition spec order

**Note:** `partition_summaries` are independent bounds per field, not actual partition combinations. Used for elimination only.

---

### Step 1.4: Two-Pass Filtering

**Pass 1: Identify dirty partitions (scan new manifests only)**

```
For each manifest where added_snapshot_id > last_processed_snapshot:
    For each entry in manifest:
        if entry.status == ADDED:
            dirty_partitions.add(entry.partition)

Result: dirty_partitions = {P1, P2, P3}
```

**Pass 2: Rebuild complete file list for dirty partitions**

For each dirty partition, we need ALL its files (not just new ones). Scan all manifests, using `partition_summaries` to skip irrelevant ones.

```
For each manifest in manifest_list:
    # Elimination check - can skip if bounds don't overlap any dirty partition
    if not overlaps(manifest.partition_summaries, dirty_partitions):
        continue  # guaranteed no relevant files

    # Must scan - bounds overlap, but doesn't guarantee files exist
    For each entry in manifest:
        if entry.status == DELETED:
            continue
        if entry.partition in dirty_partitions:
            partition_map[entry.partition].add(entry)
```

**Key insight:** `partition_summaries` enable elimination (skip manifests that definitely don't have our partitions), but don't guarantee inclusion (must scan to confirm).

**Why two passes?** New files are in new manifests, but a partition's complete file list spans old AND new manifests. Pass 1 identifies which partitions are dirty. Pass 2 rebuilds their complete file list from all manifests.

### Dirty Bounds Optimization

Instead of checking each dirty partition against each manifest (O(D × M)), we compute bounds once:

```
Dirty partitions: {(2024-01-15, us-east), (2024-01-16, us-west)}

Extract dirty bounds:
  date:   [min: 2024-01-15, max: 2024-01-16]
  region: [min: us-east,    max: us-west]

For each manifest:
  if manifest.bounds overlap dirty_bounds → scan
  else → skip (O(M) total)
```

False positives are OK - we filter by exact partition when scanning entries.

---

### Step 1.5: Parallel Manifest Scanning

```
Filtered Manifests
       │
       ├── M1 ──→ Task ──┐
       ├── M2 ──→ Task ──┼──→ Merge ──→ HashMap<Partition, Vec<FileInfo>>
       └── M3 ──→ Task ──┘
```

- Bounded concurrency: 10-50 concurrent reads (S3 rate limits ~3500 req/s per prefix)
- Each task returns `Vec<(PartitionKey, FileInfo)>`
- Merge is sequential after all tasks complete (in-memory, fast)

---

### Step 1.6: Filter Compaction Candidates

For each partition in map:
- `file_count >= min_file_count` (e.g., 5)
- Filter out files where `size >= max_file_size` (e.g., 128MB) - already optimal

Partitions with fewer files → skip (not worth the overhead).

**Why filter large files?** Previously compacted files are already optimal size. We only compact the small files, leaving large ones untouched.

**Re-compaction:** A partition compacted before can be compacted again if new files were added since and total file count exceeds threshold.

---

### Step 1.7: Bin-Pack into FileGroups

Split large partitions into groups:
- Sort files by size descending
- Accumulate until `max_group_size` reached
- Emit group, start new one
- Skip groups with < 2 files

Output: `Vec<FileGroup>`

---

## Manifest Entry Structure

Each manifest contains array of entries:

```json
{
  "status": 1,
  "snapshot_id": 8,
  "file_path": "s3://bucket/table/data/date=2024-01-01/region=us-east/file1.parquet",
  "file_format": "PARQUET",
  "partition": {
    "date": "2024-01-01",
    "region": "us-east"
  },
  "record_count": 50000,
  "file_size_in_bytes": 12582912
}
```

| Status | Meaning | Action |
|--------|---------|--------|
| 0 | EXISTING | Include (file from previous snapshot, still valid) |
| 1 | ADDED | Include (file added in this snapshot) |
| 2 | DELETED | Skip (file removed) |

One manifest can contain files from multiple partitions.

---

## Phase 2: Compaction (Parallel Workers)

### Coordinator

Dispatches FileGroups, collects results, owns commit.

```
1. Spawn N worker tasks
2. For each FileGroup:
     work_tx.send(CompactionTask).await
3. Drop work_tx (signals completion)
4. Collect CompactionResults from result_rx
5. Filter successful results
6. Commit all changes in single snapshot
```

### Workers (Stateless)

Each worker is a pure data processor. No catalog access needed.

```
While work_rx.recv():
    1. Read: stream Parquet files via DataFusion
    2. Merge: combine into single record batch stream
    3. Write: output compacted Parquet file to storage
    4. Report: send CompactionResult to coordinator
```

**Why stateless?** Simpler worker code, fewer dependencies, all coordination logic in one place.

### Channels

| Channel | Pattern | Capacity | Why |
|---------|---------|----------|-----|
| Work | SPMC (async_channel) | Bounded (2×N) | Backpressure, load balancing |
| Result | MPSC (tokio) | Unbounded | Don't block workers |

### Why Shared Queue?

Fast workers take more work → natural load balancing.

Alternative (round-robin dispatch) causes uneven distribution if group sizes vary.

### Commit Strategy

- **Coordinator-owned commits** - workers are stateless, coordinator does single batch commit
- **Single snapshot per run** - fewer snapshots = less metadata overhead
- Optimistic concurrency (no locks) - sink is P0, compaction must not block
- Pre-commit validation - check files still exist in current snapshot
- Conflict → retry N times, then abort entire run (leave for next run)

### Commit Flow (Coordinator Batch Commit)

```
Workers complete:
  W1 → Result { input: [f1,f2,f3], output: compacted_001.parquet }
  W2 → Result { input: [f4,f5],    output: compacted_002.parquet }
  W3 → Result { status: Failed }

Coordinator:
  1. Filter successful results (W1, W2)
  2. Build single commit:
     - f1, f2, f3, f4, f5 → DELETED
     - compacted_001, compacted_002 → ADDED
  3. Write new manifest (to S3)
  4. Write new manifest list (to S3)
  5. Write new metadata.json (to S3)
  6. Catalog commit (atomic pointer update):
     catalog.commit(table, old_metadata → new_metadata)
```

**Trade-off:** If coordinator crashes after workers finish but before commit → orphan files. Acceptable for batch job, can add cleanup tool later.

**Optimistic concurrency:** If another writer committed between our read and commit → conflict → retry with fresh metadata.

---

## Phase 3: Finalize

Sequential operations:

1. **Collect results** - wait for all workers, aggregate success/failure
2. **Commit** - single atomic commit with all successful compactions (see Commit Flow above)
3. **Update sidecar** - write new `last_processed_snapshot` and partition states
4. **Print summary** - report to stdout

---

## Compaction State (External Sidecar)

Location: `s3://bucket/table/.megaberg/compaction_state.json`

```json
{
  "last_processed_snapshot": 8,
  "partition_state": {
    "(2024-01-01, us-east)": { "last_compacted_snapshot": 8 },
    "(2024-01-02, us-east)": { "last_compacted_snapshot": 5 }
  }
}
```

**Why external?**
- Avoids bloating `metadata.json` (loaded on every query/sink operation)
- No extra commits for bookkeeping
- Doesn't impact sink/query performance

**Sidecar lifecycle:**
- First run: doesn't exist → full scan
- After run: updated with current snapshot and compacted partitions
- Subsequent runs: used to filter which manifests/partitions to process

**Field usage:**
- `last_processed_snapshot` - **actively used** to filter new manifests in Pass 1
- `partition_state` - **tracking/audit only**, not used for filtering (partitions are re-compacted based on file count, not last compaction time)

---

## Error Handling

| Error | Action |
|-------|--------|
| Sidecar not found | First run, full scan |
| Metadata read failure | Abort entire run |
| Manifest read failure | Abort entire run |
| File missing during read | Abort group |
| Write failure | Abort group |
| Commit conflict | Retry N times, then abort group |
| Worker panic | Log, continue other workers |

---

## Shutdown

**Graceful:**
1. Coordinator dispatches all groups
2. Drops `work_tx`
3. Workers exit after current group
4. Collect remaining results
5. Update sidecar, print summary

**Ctrl+C:**
1. Drop channels
2. Workers abort on next recv()
3. Orphan files may remain (future: cleanup tool)

---

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `worker_count` | 2-4 × num_cpus | I/O bound, more workers = more concurrency |
| `manifest_concurrency` | 20 | Parallel manifest reads |
| `channel_capacity` | 2 × workers | Work queue size |
| `min_file_count` | 5 | Min files in partition to trigger compaction |
| `max_file_size` | 128MB | Skip files larger than this (already optimal) |
| `target_file_size` | 256MB | Output file target |
| `max_group_size` | 512MB | Max input per FileGroup |
| `max_retries` | 3 | Commit retry attempts |

---

## Async Concurrency Model

We use **tokio async tasks**, not OS threads or hyperthreading.

```rust
// Parallel manifest scanning
let results: Vec<_> = stream::iter(manifests)
    .map(|m| async { scan_manifest(&storage, m).await })
    .buffer_unordered(20)  // Max 20 concurrent
    .collect()
    .await;
```

**Why async works for us:** Workload is I/O bound, not CPU bound. While waiting for S3, other tasks can progress.

| Layer | Technology |
|-------|------------|
| Runtime | Tokio (async executor, thread pool) |
| Code | `async/await`, `tokio::spawn`, `futures` |
| Pattern | Bounded concurrency with `buffer_unordered` |

---

## Catalog vs Storage Responsibilities

| Responsibility | Owner |
|----------------|-------|
| Table name → metadata location | **Catalog** |
| Atomic metadata pointer update | **Catalog** |
| Data files (Parquet) | **Storage** |
| Manifest files | **Storage** |
| Metadata JSON content | **Storage** |
| Compaction sidecar | **Storage** |

Catalog only holds the pointer. Storage holds all actual files.

---

## Open Questions

1. **Worker count tuning** - profile to find optimal for S3 throughput
2. **Worker failure** - restart or let die?
3. **Progress reporting** - per-group logs or periodic?
4. **Metrics** - prometheus or final summary only?
5. **Orphan cleanup** - this tool or separate?
