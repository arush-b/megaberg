## Megaberg TODOs

### P0 - Critical
1. **Manifest Carry Forward (Partial Compaction Support)**
   - Currently drops all existing manifests when there are deletes - only works for full compaction
   - Need to: identify affected manifests, rewrite them with EXISTING entries for kept files
   - Location: `crates/committer/src/glue.rs`

### P1 - Testing
2. **Unit + Integration Tests for All Modules**
   - committer: manifest writing, snapshot updates, Glue API calls
   - planner: discovery, file grouping, partition handling
   - executor: parquet read/write, compaction logic
   - engine: end-to-end orchestration

3. **Test Containers for End-to-End Flow**
   - Options:
     - **Minio (local)**: Fast iteration, no AWS costs, good for CI
     - **EC2 instance**: Real AWS environment, tests actual S3/Glue behavior
   - Recommendation: Minio for local dev + CI, EC2 for final validation

### P2 - Infrastructure
4. **CI + Docker Setup**
   - GitHub Actions workflow for build/test
   - Docker compose for local development environment
   - Container images for deployment

---

## What's Achievable Today

**Realistic for 1 day:**

1. **Unit tests for committer module** (~3-4 hours)
   - Test manifest.rs functions (build_partition, parse_partition_value, write_manifest)
   - Test snapshot.rs (build_snapshot_summary)
   - Mock FileIO for isolated testing

2. **Docker compose with Minio** (~2-3 hours)
   - Already have docker-compose.yml in repo
   - Add Minio service for S3-compatible storage
   - Basic init script for test bucket/data

3. **Basic CI setup** (~1-2 hours)
   - GitHub Actions: cargo build, cargo test, cargo clippy
   - Run on PR/push to main

**Stretch goals (if time permits):**
- Integration test with Minio that runs discovery → plan → execute → commit flow
- Manifest carry forward design doc (not implementation)

**Not realistic for 1 day:**
- Full manifest carry forward implementation (complex, needs careful design)
- Complete test coverage for all modules
- EC2-based testing setup

---

## Code Cleanups

### Committer Crate

1. **Deduplicate `glue.rs` / `rest.rs` commit logic**
   - ~80% identical code between the two files
   - Extract shared logic into a `commit_core()` function
   - Each committer calls `commit_core()` + their specific catalog update

2. **Remove unused `GlueConfig` storage**
   - `GlueCommitter.config` field is stored but never read
   - `self.config.warehouse` and `self.config.region` are never accessed
   - Everything comes from the `Table` object passed to `commit_with_table()`
   - Either remove the field or actually use it for AWS region configuration

### Planner Module

3. **Remove dead `filter_partitions_by_time()` function**
   - Location: `crates/engine/src/planner/table/parquet.rs:372`
   - Was replaced by `generate_time_window_prefixes()` (targeted listing)
   - Function exists but is never called

4. **Consider splitting large `parquet.rs` file (1780 lines)**
   - Optional quality-of-life improvement
   - Could split into: `parquet/source.rs`, `parquet/time_window.rs`, `parquet/tests.rs`

### Executor Module

5. **Verify `StorageError` in config.rs is still needed**
   - `build_operator()` moved to `common` crate
   - `StorageError` in executor config.rs may be dead code
