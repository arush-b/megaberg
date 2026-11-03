# Megaberg
 
A single-node compaction engine for Apache Iceberg tables, written in Rust.
 
## How it works
 
1. **Discover**: reads Iceberg metadata and manifests, then groups data files by partition.
2. **Pack**: bin-packs each partition's small files into groups of about the target size.
3. **Compact**: parallel DataFusion workers merge each group and write larger Parquet files to S3.
## Layout
 
```
crates/engine/     # library: planner + executor
crates/megaberg/   # CLI (stub)
docs/              # architecture + contract
docker-compose.yml # MinIO, Iceberg REST catalog, Spark
```
 
## Quick start
 
```bash
cargo build --release
docker compose up -d
cargo test --package engine --release
```
