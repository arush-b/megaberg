## Executive Summary

This document outlines the design and implementation strategy for an Iceberg table management service focused on optimizing read performance through data file compaction and delete file optimization.

---

## Motivation

### Current Challenges
- Multiple small Parquet data files per partition impacting read performance
- Inefficient equality deletes requiring value comparisons at query time
- Growing metadata overhead from orphaned files and expired snapshots

### Goals
- Achieve optimal data file sizes (128-256MB) per partition
- Convert equality deletes to positional deletes for faster reads
- Minimize metadata operation overhead
- Reduce query planning time and execution latency

---

## 1. Data File Compaction Service

### Overview
Periodic service to consolidate small Parquet data files within partitions into optimally-sized files.

### Design Specifications

| Component | Specification |
|-----------|--------------|
| **Target File Size** | 128MB - 256MB per file |
| **Trigger Mechanism** | Hybrid: Time-based + Threshold-based |
| **Partition Handling** | Process cold partitions first |
| **Sort Order** | Preserve existing order for query performance/metadata optimization |

### Compaction Strategy
- **Cold Partitions** (>24hrs): Aggressive compaction to target size
- **Warm Partitions** (1-24hrs): Conservative compaction, higher threshold
- **Hot Partitions** (<1hr): Skip to avoid write conflicts

### Compaction Triggers

| Partition Type | File Count Threshold | File Size Threshold |
|----------------|---------------------|-------------------|
| **Cold** | TBD | Avg size < X |
| **Warm** | TBD | Avg size < Y (Y ≤ X) |
| **Hot** | TBD | Skip/TBD |

Still need some clarity on tackling partitions based on freshness of data

### Key Considerations
- Implement partition-level locking to prevent concurrent write conflicts
  - **Possible approaches:** Iceberg's optimistic concurrency control (snapshot isolation with retry), external coordinator (Zookeeper/etcd), or table metadata properties
  - **Approach TBD:** Will evaluate based on retry frequency and infrastructure constraints
- Track compaction history to avoid reprocessing - figure out a way to tell engine not to scan the already compacted partitions again
- Backpressure handling - configure that based on cluster capacity
- Maintain data file statistics for query optimization - update metadata simulaneously
- Some of these might not be relevant for CDC use-case so might not be fully considered for the first phase - refer Implementation Roadmap

---

## 2. Equality to Positional Delete Conversion

### Overview
Convert equality deletes to positional deletes for improved query performance.

### Conversion Process Steps
1. **Partition Selection**
   - Identify partitions containing equality delete files
   - Prioritize based on read frequency and partition age

2. **Data Scanning Phase**
   - Read all data files in the partition
   - Read equality delete predicates (e.g., `id = 123`, `user_id = 'abc'`)
   - Match delete conditions against every row in data files
   - Record file path + row position for each match

3. **Positional Delete Generation**
   - Create new positional delete file with collected positions
   - Group positions by file for efficiency (i.e. mentioning all rows to be deleted from a file together)

4. **Atomic Replacement**
   - Begin Iceberg transaction
   - Remove equality delete file reference
   - Add positional delete file reference
   - Commit with retry on conflict

5. **Validation**
   - Verify row count matches between delete types
   - Ensure snapshot consistency
   - Update conversion history

### Selection Criteria (TBD based on workload analysis)
- Partition age threshold
- Read frequency patterns
- Delete file size threshold
- Cost-benefit analysis framework

### Implementation Considerations
- Conversion runs as separate job from compaction
- Process cold partitions first to minimize conflicts
- Monitor conversion impact on cluster resources
- Track which partitions have been converted

---

## 3. Metadata Optimization

### Snapshot Expiration
- **Retention:** Keep last few days OR last few snapshots, numbers TBD based on use-case.
- **Execution:** Daily/weekly cleanup during low-traffic hours

### Orphan File Removal
- **Detection:** Data files not referenced in any active snapshot
- **Safety:** A grace period before deletion
- **Frequency:** Weekly scan and cleanup

### Manifest Compaction
- **Threshold:** When manifest count exceeds a certain number (X) per table
- **Target:** Consolidate to Y (< X) manifest files

---

## Implementation Roadmap

### Phase 1: Data File Compaction
-  Design service architecture
-  Implement size-based compaction logic
-  Add partition-aware scheduling
-  Create monitoring dashboards
-  Deployment in a pre-prod environment

### Phase 2: Delete Conversion
-  Build equality → positional delete converter
-  Add conversion job scheduler
-  Test on mock partitions

### Phase 3: Metadata Cleanup
-  Snapshot expiration service
-  Orphan file detection
-  Manifest compaction logic

---

## Open Questions

1. **Compaction Priority:** Should we prioritize by partition size, age, or query frequency?
    1. Set a balance between partitions (how cold) to compact <-> query volume on those partitions (avoid wasteful compute)
2. **Delete Conversion Threshold:** What's the minimum ROI to justify conversion?
3. **Service Architecture:** Standalone service or integrated with ingestion?
    1. We want to decouple this from ingestion for now and run it as a separate engine, but thoughts are open.d