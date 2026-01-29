# Compaction Engine Comparison Framework

This document defines different dimensions for comparing Megaberg to existing table maintenance solutions in the market. The goal is to have a source of truth that helps us define unique propositions for our product (ease of use, better performance, better scalability, or operational simplicity).

---

## 1. Execution Model

| Model | Description | Examples |
|-------|-------------|----------|
| **Background Daemon** | Always running, continuously monitors and compacts | - |
| **Batch (One-shot)** | Run once, compact, terminate | Megaberg, Nimtable |
| **Triggered** | Runs on event (new data) or schedule | Spark maintenance commands |

**Megaberg:** Batch (one-shot). Start → discover → compact → commit → exit.

---

## 2. Coupling

| Model | Description | Examples |
|-------|-------------|----------|
| **Ingestion-integrated** | Part of the write path, compacts as data arrives | Flink Iceberg sink |
| **Query-integrated** | Part of query engine, runs via SQL commands | Spark `CALL rewrite_data_files()`, Trino |
| **Decoupled** | Standalone, no dependency on query/ingestion engine | Nimtable, Megaberg |

**Megaberg:** Decoupled. Can run independently without Spark/Flink/Trino.

---

## 3. Compaction Strategy

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **Bin-packing** | Merge small files into target size | Most common, reduces file count |
| **Sort-based** | Rewrite with sorted order (z-order, hilbert) | Improve query locality |
| **Time-based** | Compact partitions older than threshold | Avoid compacting hot partitions |
| **Tiered** | Different strategies for hot/cold data | Balance freshness vs efficiency |

**Megaberg:** Bin-packing (initial), extensible to sort-based.

---

## 4. Commit Model

| Model | Description | Trade-offs |
|-------|-------------|------------|
| **Single snapshot** | One commit at end of run | Simpler, all-or-nothing |
| **Per-partition commits** | Commit after each partition | Faster visibility, more snapshots |
| **Streaming commits** | Continuous micro-commits | Low latency, snapshot bloat |

**Megaberg:** Single snapshot per run. Cleaner history, atomic.

---

## 5. Scalability

| Model | Description | Examples |
|-------|-------------|----------|
| **Single-node** | One process, limited by machine resources | - |
| **Distributed (fixed)** | Coordinator + worker pool | Megaberg |
| **Distributed (elastic)** | Auto-scaling workers (Spark, Flink) | Spark `rewrite_data_files` |
| **Serverless** | Fully managed, pay-per-use | AWS Glue compaction |

**Megaberg:** Distributed (fixed). Coordinator dispatches to bounded worker pool.

---

## 6. State Management

| Model | Description | Trade-offs |
|-------|-------------|------------|
| **Stateless** | Reads all metadata each run, no resume | Simpler, always consistent |
| **Stateful** | Tracks progress, can resume mid-run | Handles failures better |
| **Checkpoint-based** | Periodic checkpoints for recovery | Balance of both |

**Megaberg:** Stateless. Each run is independent. Failed runs just retry from scratch.

---

## 7. Conflict Handling

| Strategy | Description | When to use |
|----------|-------------|-------------|
| **Fail fast** | Abort on conflict, no retry | Manual intervention preferred |
| **Retry (full)** | Retry entire run on conflict | Simple, but wasteful |
| **Retry (partial)** | Retry only failed groups, reuse written files | Efficient for large tables |
| **Optimistic merge** | Attempt to merge concurrent changes | Complex, rarely used |

**Megaberg:** Retry (partial). On conflict, reuse already-written files, retry commit only.

---

## 8. Table Format Support

| Scope | Examples |
|-------|----------|
| **Single format** | Iceberg-only, Delta-only |
| **Multi-format** | Apache XTable, Onetable |

**Megaberg:** Iceberg-only (v1 and v2).

---

## 9. Scheduling

| Model | Description |
|-------|-------------|
| **External** | Cron, Airflow, Kubernetes CronJob |
| **Built-in** | Engine has its own scheduler |
| **Event-driven** | Triggered by commit hooks, SNS, etc. |

**Megaberg:** External. Just a binary, schedule however you want.

---

## 10. Observability

| Feature | Description |
|---------|-------------|
| **Metrics** | Prometheus/StatsD metrics (files compacted, bytes written) |
| **Logging** | Structured logs for debugging |
| **Progress tracking** | Real-time progress for long runs |
| **Sidecar output** | JSON summary for downstream processing |

**Megaberg:** All of the above (planned).

---

## Comparison Matrix

| Dimension | Megaberg | Nimtable | Spark rewrite | Flink sink |
|-----------|----------|----------|---------------|------------|
| Execution | Batch | Batch | Triggered | Daemon |
| Coupling | Decoupled | Decoupled | Query-integrated | Ingestion-integrated |
| Strategy | Bin-pack | Bin-pack | Bin-pack/Sort | Bin-pack |
| Commit | Single | ? | Per-group | Streaming |
| Scalability | Distributed | Single-node? | Elastic | Elastic |
| State | Stateless | ? | Stateless | Stateful |
| Conflict | Partial retry | ? | Fail | N/A (owns writes) |

---

## Why Decoupled Matters

Integrated compaction (Spark, Flink) requires:
- Running a heavyweight engine just for maintenance
- Compute resources tied to query/ingestion workloads
- Coordination with existing jobs

Decoupled compaction:
- Lightweight, runs only when needed
- Independent resource allocation
- Can run on different infrastructure (spot instances, off-peak)
- No dependency on query engine availability
