# Megaberg Architecture

## High-Level Overview

Megaberg is an Iceberg table compaction tool that consolidates small files into larger, optimally-sized files while maintaining Iceberg metadata consistency.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              MEGABERG ARCHITECTURE                          │
└─────────────────────────────────────────────────────────────────────────────┘

                              ┌──────────────┐
                              │  Config JSON │
                              └──────┬───────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                PLANNER                                       │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │   Catalog   │───▶│  Discovery  │───▶│   Packer    │───▶│Task Builder │  │
│  │ (REST/Glue) │    │  (Iceberg)  │    │ (Bin-pack)  │    │             │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│        │                   │                  │                  │          │
│        ▼                   ▼                  ▼                  ▼          │
│   TableInfo         PartitionFileMap    Vec<FileGroup>   Vec<CompactionTask>│
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                               EXECUTOR                                       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                       CompactionEngine                                 │  │
│  │  ┌─────────────────┐         ┌──────────────────────────────────────┐ │  │
│  │  │   DataFusion    │         │        RollingParquetWriter          │ │  │
│  │  │  SessionContext │────────▶│  • Rolling file output (256 MB)      │ │  │
│  │  │                 │ Stream  │  • Multipart upload (64 MB chunks)   │ │  │
│  │  │  • read_parquet │ of      │  • Iceberg field IDs                 │ │  │
│  │  │  • execute_stream│Batches │  • Statistics extraction             │ │  │
│  │  └─────────────────┘         └──────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                     │                                        │
│                                     ▼                                        │
│                              Vec<WrittenFile>                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                COMMIT                                        │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐  │
│  │ Write Manifest  │───▶│Write Manifest   │───▶│   Update Catalog        │  │
│  │ (added/deleted) │    │     List        │    │  (REST API / Glue SDK)  │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
                              ┌──────────────┐
                              │   Complete   │
                              └──────────────┘
```

---

## Talking Points

### 1. Planning Phase

**Catalog-First Discovery**
- Loads table metadata from catalog (REST or Glue) - not S3 scanning
- Gets authoritative snapshot ID and manifest list path
- Avoids issues with orphaned metadata from failed commits

**Iceberg Manifest Reading**
- Reads manifest-list (Avro) to find all manifest files
- Parses each manifest to extract file paths, sizes, record counts
- Groups files by partition key

**Bin-Packing Algorithm**
- Greedy bin-packing: largest files first
- Respects target file size (256 MB default)
- Skips partitions with < N files (configurable threshold)

---

### 2. DataFusion Integration

**How We Use DataFusion**
```
┌──────────────────────────────────────────────────────────────┐
│                    DataFusion Pipeline                        │
│                                                               │
│  S3 Files ──▶ OpenDAL ──▶ SessionContext ──▶ RecordBatch     │
│     │            │              │                  │          │
│     │            │              │                  │          │
│   Input      Object Store    Query Engine      Streaming     │
│  Parquet     Abstraction     (read, project)    Output       │
└──────────────────────────────────────────────────────────────┘
```

**Key Configuration**
- `SessionContext`: Reused across all tasks (expensive to create)
- `batch_size`: 8192 rows per batch (memory-efficient streaming)
- `target_partitions`: Controls parallel reads
- `FairSpillPool`: Memory limiter with spill-to-disk support

**Object Store Registration**
```rust
let store = OpendalStore::new(operator);
ctx.register_object_store("s3://bucket", store);
```
- OpenDAL provides unified API for S3, MinIO, local FS
- DataFusion uses this for all I/O operations

---

### 3. Rolling Parquet Writer

**Design Goals**
- Write optimally-sized files (256 MB target)
- Support streaming writes (don't buffer entire dataset)
- Extract Iceberg-compatible statistics

**Rolling Mechanism**
```
┌─────────────────────────────────────────────────────────────┐
│                    RollingParquetWriter                      │
│                                                              │
│  write_batch(RecordBatch)                                    │
│       │                                                      │
│       ▼                                                      │
│  ┌─────────────────┐                                         │
│  │ bytes_written   │──▶ >= target_size? ──▶ roll_file()     │
│  │ >= 256 MB?      │                                         │
│  └─────────────────┘                                         │
│       │                                                      │
│       ▼                                                      │
│  AsyncArrowWriter.write(batch)                               │
│       │                                                      │
│       ▼                                                      │
│  OpenDAL Writer (multipart upload, 64 MB chunks)             │
└─────────────────────────────────────────────────────────────┘
```

**Iceberg Field IDs**
- Adds `PARQUET:field_id` metadata to each schema field
- Required for Iceberg schema evolution
- Sequential IDs starting from 1

**Statistics Extraction**
After closing each file, extracts from Parquet metadata:
- `column_sizes`: Compressed bytes per field
- `value_counts`: Total values per field
- `null_value_counts`: Null count per field
- `lower_bounds` / `upper_bounds`: Min/max values (for pruning)

---

### 4. Commit Process

**Two-Phase Approach**
```
Phase 1: Write Metadata to S3
  └─▶ New manifest (Avro) with ADDED + DELETED entries
  └─▶ New manifest-list (Avro) combining new + existing manifests

Phase 2: Update Catalog
  └─▶ REST: POST /v1/namespaces/{ns}/tables/{table} with updates
  └─▶ Glue: Write metadata.json, then update Glue table pointer
```

**Iceberg Deletion Mechanism**
- Files aren't physically deleted during compaction
- New manifest has DELETED entries for old files
- DELETED entries override ADDED entries from older manifests
- Garbage collection removes files later

**Optimistic Concurrency Control**
```rust
TableRequirement::RefSnapshotIdMatch {
    ref: "main",
    snapshot_id: parent_snapshot_id,
}
```
- If another writer commits between discovery and commit → fails
- Must retry with fresh discovery

---

## Data Flow Summary

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│   ┌─────────┐    ┌──────────────┐    ┌─────────────┐    ┌────────────┐ │
│   │ Catalog │───▶│ Manifest List│───▶│  Manifests  │───▶│ File List  │ │
│   │         │    │    (Avro)    │    │   (Avro)    │    │            │ │
│   └─────────┘    └──────────────┘    └─────────────┘    └─────┬──────┘ │
│                                                                │        │
│                                                                ▼        │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │                         COMPACTION                               │  │
│   │                                                                  │  │
│   │  800 x 1MB files ──────────▶ 3 x 256MB files                    │  │
│   │       │                              │                           │  │
│   │       ▼                              ▼                           │  │
│   │  ┌─────────────┐              ┌─────────────┐                   │  │
│   │  │ DataFusion  │   Stream     │   Rolling   │                   │  │
│   │  │   Reader    │─────────────▶│   Writer    │                   │  │
│   │  └─────────────┘  RecordBatch └─────────────┘                   │  │
│   │                                                                  │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                │        │
│                                                                ▼        │
│   ┌──────────────┐    ┌──────────────┐    ┌─────────────────────────┐ │
│   │ New Manifest │───▶│ Manifest List│───▶│    Catalog Update       │ │
│   │ (add+delete) │    │   (updated)  │    │ (atomic commit)         │ │
│   └──────────────┘    └──────────────┘    └─────────────────────────┘ │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `target_file_size_mb` | 256 | Target output file size |
| `min_files_per_partition` | 2 | Min files to trigger compaction |
| `row_group_size` | 300,000 | Rows per parquet row group |
| `multipart_chunk_bytes` | 64 MB | S3 multipart upload chunk |
| `batch_size` | 8,192 | DataFusion batch size |
| `memory_limit_bytes` | 512 MB | Memory pool limit |
| `compression` | none | Parquet compression codec |

---

## Key Design Decisions

1. **Reusable Engine**: Create DataFusion SessionContext once, reuse for all tasks
2. **Streaming Processing**: Never buffer entire dataset in memory
3. **Catalog-First**: Always get metadata from catalog, not S3 scanning
4. **Rolling Files**: Automatic file rollover at target size
5. **Statistics Extraction**: Extract min/max for Iceberg partition pruning
6. **Atomic Commits**: Use catalog APIs for consistency
