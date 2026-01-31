# Production Dependency Cleanup

This document tracks dependencies that are not required for production and recommended changes.

## Unused Dependencies (Remove)

These are declared but never used in the codebase:

| Dependency | Location | Status |
|------------|----------|--------|
| `serde_yaml` | `Cargo.toml` (workspace) | Declared but never imported |
| `object_store` | `Cargo.toml` (workspace) | Declared but never imported (only `object_store_opendal` is used) |
| `tempfile` | `Cargo.toml` (workspace) | Testing only - should not be in workspace deps |

### Action Required

```toml
# Remove from workspace Cargo.toml [workspace.dependencies]:
serde_yaml = "0.9"
object_store = { version = "0.12", features = ["aws"] }
tempfile = "3"
```

---

## Dev-Only Dependencies

These are correctly placed in `[dev-dependencies]` sections:

| Dependency | Crates | Purpose |
|------------|--------|---------|
| `testcontainers` | committer, iceberg-data-gen, megaberg | Container-based integration tests |
| `testcontainers-modules` | committer, iceberg-data-gen, megaberg | MinIO containers for tests |
| `serial_test` | engine | Test serialization |
| `aws-sdk-s3` | iceberg-data-gen, megaberg | Test bucket setup (not used for actual S3 ops) |

No action required - already in dev-dependencies.

---

## Non-Production Crate

The `iceberg-data-gen` crate is a test data generator and should not be included in production builds.

### Current State

```toml
# Cargo.toml workspace members includes:
[workspace]
members = [
    "crates/megaberg",
    "crates/engine",
    "crates/common",
    "crates/committer",
    "crates/iceberg-data-gen",  # <-- Test utility
]
```

### Recommendation

For production builds, exclude this crate:

```bash
# Build only production crates
cargo build --release -p megaberg
```

Or create a separate workspace for production:

```toml
# Cargo.toml
[workspace]
default-members = [
    "crates/megaberg",
    "crates/engine",
    "crates/common",
    "crates/committer",
]
members = [
    "crates/megaberg",
    "crates/engine",
    "crates/common",
    "crates/committer",
    "crates/iceberg-data-gen",
]
```

---

## Feature-Gate Candidates

These dependencies are only needed for specific catalog backends. Consider feature-gating to reduce binary size.

### Glue Catalog Dependencies

| Dependency | Crates |
|------------|--------|
| `iceberg-catalog-glue` | committer, engine, megaberg, iceberg-data-gen |
| `aws-sdk-glue` | committer, megaberg |
| `aws-config` | committer, megaberg, iceberg-data-gen |

### REST Catalog Dependencies

| Dependency | Crates |
|------------|--------|
| `iceberg-catalog-rest` | committer, engine, megaberg, iceberg-data-gen |
| `reqwest` | committer, megaberg |

### Proposed Feature Flags

```toml
# Example for megaberg/Cargo.toml
[features]
default = ["rest-catalog"]
rest-catalog = ["iceberg-catalog-rest", "reqwest"]
glue-catalog = ["iceberg-catalog-glue", "aws-sdk-glue", "aws-config"]
```

This would allow:

```bash
# REST-only build (smaller)
cargo build --release -p megaberg --no-default-features --features rest-catalog

# Glue-only build
cargo build --release -p megaberg --no-default-features --features glue-catalog

# Both catalogs
cargo build --release -p megaberg --features "rest-catalog,glue-catalog"
```

---

## Required Production Dependencies

These are essential and must remain:

| Category | Dependencies |
|----------|--------------|
| Async Runtime | `tokio`, `async-channel`, `async-trait`, `futures` |
| Data Processing | `arrow`, `parquet`, `datafusion`, `apache-avro` |
| Storage | `opendal`, `object_store_opendal`, `bytes` |
| Iceberg Core | `iceberg` |
| Serialization | `serde`, `serde_json` |
| Error Handling | `anyhow`, `thiserror` |
| Logging | `tracing`, `tracing-subscriber` |
| CLI | `clap` |
| Utilities | `chrono`, `uuid`, `url`, `hex`, `percent-encoding` |

---

## Summary Checklist

- [ ] Remove `serde_yaml` from workspace dependencies
- [ ] Remove `object_store` from workspace dependencies
- [ ] Remove `tempfile` from workspace dependencies
- [ ] Consider `default-members` to exclude `iceberg-data-gen` from default builds
- [ ] (Optional) Implement feature flags for catalog backends

---

## PR Strategy: Committer Branch → Main

Strategy for merging changes from `committer` branch to `main` in 4 incremental PRs.

### Dependency Chain

```
common (standalone)
    ↓
committer (standalone)
    ↓
engine (depends on: common, committer)
    ↓
megaberg (depends on: common, engine, committer)
```

---

### PR 1: New crates - `common` + `committer`

**~2700 lines | Both are standalone, no internal dependencies**

| Files |
|-------|
| `crates/common/Cargo.toml` |
| `crates/common/src/lib.rs` |
| `crates/common/src/storage.rs` |
| `crates/committer/Cargo.toml` |
| `crates/committer/src/lib.rs` |
| `crates/committer/src/types.rs` |
| `crates/committer/src/manifest.rs` |
| `crates/committer/src/snapshot.rs` |
| `crates/committer/src/glue.rs` |
| `crates/committer/src/rest.rs` |
| `Cargo.toml` (workspace members + deps) |

**Why together**: Both are new crates with zero internal dependencies. Gets the foundational pieces in.

---

### PR 2: Engine enhancements

**~3500 lines | All planner + executor changes**

| Add | Delete | Modify |
|-----|--------|--------|
| `crates/engine/src/planner/table/mod.rs` | `crates/engine/src/planner/discovery/iceberg.rs` | `crates/engine/src/planner/mod.rs` |
| `crates/engine/src/planner/table/source.rs` | `crates/engine/tests/compaction_test.rs` | `crates/engine/src/planner/discovery/mod.rs` |
| `crates/engine/src/planner/table/sink.rs` | | `crates/engine/src/planner/task_builder.rs` |
| `crates/engine/src/planner/table/parquet.rs` | | `crates/engine/src/executor/mod.rs` |
| `crates/engine/src/planner/table/iceberg.rs` | | `crates/engine/src/executor/config.rs` |
| `crates/engine/src/executor/metrics.rs` | | `crates/engine/src/executor/engine.rs` |
| `crates/engine/tests/parquet_discovery_test.rs` | | `crates/engine/src/executor/writer.rs` |
| `crates/engine/tests/time_window_test.rs` | | `crates/engine/Cargo.toml` |

**Why together**: All engine internals - table abstractions, discovery, executor improvements. Cohesive unit.

---

### PR 3: Megaberg CLI refactor

**~2000 lines | Complete CLI overhaul**

| Add | Modify |
|-----|--------|
| `crates/megaberg/src/configs.rs` | `crates/megaberg/src/main.rs` |
| `crates/megaberg/src/planner.rs` | `crates/megaberg/Cargo.toml` |
| `crates/megaberg/src/runner.rs` | |
| `crates/megaberg/src/source.rs` | |

**Why together**: All CLI module changes - config system, orchestration modules.

---

### PR 4: Infrastructure + Docs

**~1500 lines | Non-code changes**

| Files |
|-------|
| `Dockerfile` |
| `docker-compose.yml` |
| `charts/megaberg/*` (Helm) |
| `spark-defaults.conf` |
| `config/config.json` (1 example) |
| `docs/ARCHITECTURE.md` |
| `docs/committer/README.md` |
| `docs/Planner.md` |
| Other selected docs |

---

### PR Order

```
PR1 (common + committer)
    → PR2 (engine)
        → PR3 (megaberg CLI)
            → PR4 (infra + docs)
```

Each PR depends on the previous one.

---

### Files to Exclude from All PRs

| Files | Reason |
|-------|--------|
| `mock_data/**` | Test data, shouldn't be in repo |
| `scripts/*` | One-off scripts |
| `crates/iceberg-data-gen/` | Test utility - separate PR later |
| `config_*.json` (most) | Too many config variants in root |
| `crates/megaberg/examples/generate_data.rs` | Depends on iceberg-data-gen |

---

### PR Checklist

- [ ] PR1: `common` + `committer` crates merged
- [ ] PR2: Engine enhancements merged
- [ ] PR3: Megaberg CLI refactor merged
- [ ] PR4: Infrastructure + docs merged

---

## Post-Merge TODOs

- [ ] `snapshot.rs`: Add `deleted-data-files` and `deleted-records` to `build_summary()` for compaction operations that delete input files
- [ ] `glue.rs`: Cache Glue client in struct using `tokio::sync::OnceCell` instead of creating per-commit (low priority, minor latency optimization)
