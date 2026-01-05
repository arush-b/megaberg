# Megaberg

Iceberg table compaction tool written in Rust.

## Prerequisites

- Docker and Docker Compose
- Rust toolchain (for local development)
- AWS CLI (for Glue testing)

---

## Local Testing (MinIO + REST Catalog)

### 1. Start Local Infrastructure

```bash
docker-compose up -d minio mc rest
```

This starts:
- **MinIO** (S3-compatible storage) at `localhost:9000`
- **REST Catalog** (Iceberg catalog) at `localhost:8181`

### 2. Generate Test Data

```bash
./scripts/reset_and_generate_local.sh
```

This creates a table `default.test_table` with:
- 20 parquet files × ~29 MB each
- ~580 MB total (10 million rows)

### 3. Run Compaction

```bash
# Single worker (default)
cargo run --release -p megaberg -- --config local_compaction_config.json

# Multiple workers
cargo run --release -p megaberg -- --config local_compaction_config.json --workers 2

# Lightweight engine (lower memory)
cargo run --release -p megaberg -- --config local_compaction_config.json --lightweight
```

### 4. Local Config File

`local_compaction_config.json`:
```json
{
  "s3": {
    "endpoint": "http://localhost:9000",
    "bucket": "lakehouse",
    "region": "us-east-1",
    "access_key": "admin",
    "secret_key": "password",
    "use_default_credentials": false
  },
  "catalog": {
    "type": "rest",
    "uri": "http://localhost:8181",
    "warehouse": "s3://lakehouse"
  },
  "table": {
    "namespace": "default",
    "name": "test_table"
  },
  "compaction": {
    "target_file_size_mb": 256,
    "min_files_per_partition": 2
  }
}
```

---

## AWS Glue Testing (S3 + Glue Catalog)

### 1. Set AWS Credentials

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### 2. Generate Test Data

```bash
# Default: laminar_iceberg.laminar_shreyas_compaction
./scripts/reset_and_generate.sh

# Custom table
DATABASE=my_db TABLE=my_table BUCKET=my-bucket ./scripts/reset_and_generate.sh
```

### 3. Run Compaction

```bash
cargo run --release -p megaberg -- --config generate_data_config.json
```

### 4. Glue Config File

`generate_data_config.json`:
```json
{
  "s3": {
    "endpoint": "https://s3.us-east-1.amazonaws.com",
    "bucket": "laminar-dev",
    "region": "us-east-1"
  },
  "catalog": {
    "type": "glue",
    "warehouse": "s3://laminar-dev/warehouse",
    "region": "us-east-1"
  },
  "table": {
    "namespace": "laminar_iceberg",
    "name": "laminar_shreyas_compaction"
  },
  "compaction": {
    "target_file_size_mb": 256,
    "min_files_per_partition": 2
  }
}
```

---

## Docker Testing

### Build Docker Image

```bash
docker build -t megaberg:latest .
# or
docker build -t gladium08/compaction:latest .
```

### Local Docker Testing (MinIO + REST)

**Step 1: Create Docker config**

```bash
cat > /tmp/docker_config.json << 'EOF'
{
  "s3": {
    "endpoint": "http://minio:9000",
    "bucket": "lakehouse",
    "region": "us-east-1",
    "access_key": "admin",
    "secret_key": "password",
    "use_default_credentials": false
  },
  "catalog": {
    "type": "rest",
    "uri": "http://rest:8181",
    "warehouse": "s3://lakehouse"
  },
  "table": {
    "namespace": "default",
    "name": "test_table"
  },
  "compaction": {
    "target_file_size_mb": 256,
    "min_files_per_partition": 2
  }
}
EOF
```

**Step 2: Reset test data**

```bash
./scripts/reset_and_generate_local.sh
```

**Step 3: Run container with resource limits**

```bash
# Start container
docker run -d \
  --name compaction-test \
  --cpus="1" \
  --memory="512m" \
  --network="megaberg_iceberg_net" \
  -v "/tmp/docker_config.json:/config/config.json:ro" \
  gladium08/compaction:latest

# Run compaction (single worker)
docker exec compaction-test megaberg --config /config/config.json

# Run compaction (2 workers)
docker exec compaction-test megaberg --config /config/config.json --workers 2

# Cleanup
docker rm -f compaction-test
```

### AWS Glue Docker Testing

**Step 1: Create Glue Docker config**

```bash
cat > /tmp/glue_config.json << 'EOF'
{
  "s3": {
    "endpoint": "https://s3.us-east-1.amazonaws.com",
    "bucket": "laminar-dev",
    "region": "us-east-1"
  },
  "catalog": {
    "type": "glue",
    "warehouse": "s3://laminar-dev/warehouse",
    "region": "us-east-1"
  },
  "table": {
    "namespace": "laminar_iceberg",
    "name": "laminar_shreyas_compaction"
  },
  "compaction": {
    "target_file_size_mb": 256,
    "min_files_per_partition": 2
  }
}
EOF
```

**Step 2: Run with AWS credentials**

```bash
docker run -d \
  --name compaction-glue \
  --cpus="1" \
  --memory="512m" \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_DEFAULT_REGION=us-east-1 \
  -v "/tmp/glue_config.json:/config/config.json:ro" \
  gladium08/compaction:latest

# Run compaction
docker exec compaction-glue megaberg --config /config/config.json --workers 2

# Cleanup
docker rm -f compaction-glue
```

### Monitor Container Memory

```bash
# Real-time monitoring (in separate terminal)
docker stats compaction-test

# Or one-liner
docker stats compaction-test --no-stream --format "MEM: {{.MemUsage}} ({{.MemPerc}})"
```

---

## CLI Options

```
megaberg --config <CONFIG> [OPTIONS]

Options:
  -c, --config <CONFIG>      Path to config JSON file
  -l, --lightweight          Use lightweight engine (no DataFusion, lower memory)
  -w, --workers <WORKERS>    Number of concurrent workers (default: 1)
  -h, --help                 Print help
```

---

## Resource Requirements

Tested with 580 MB input data (20 files × 29 MB):

| Config | vCPU | Memory | Time | Throughput |
|--------|------|--------|------|------------|
| 1 worker | 0.5 | 256 MB | 3.6s | 161 MB/s |
| 2 workers | 1.0 | 512 MB | 2.6s | 225 MB/s |

### EKS Resource Specs

**Single Worker:**
```yaml
resources:
  requests:
    cpu: "250m"
    memory: "200Mi"
  limits:
    cpu: "500m"
    memory: "256Mi"
```

**Two Workers:**
```yaml
resources:
  requests:
    cpu: "500m"
    memory: "350Mi"
  limits:
    cpu: "1000m"
    memory: "512Mi"
```

---

## Cost Estimates (AWS Fargate)

| Compactions/Month | Cost |
|-------------------|------|
| 1,000 | $0.71 |
| 10,000 | $7.12 |
| 100,000 | $71.20 |

**~0.12 cents per GB compacted**

---

## Performance Summary

| Engine | Workers | Time | Throughput | Memory |
|--------|---------|------|------------|--------|
| DataFusion | 1 | 3.6s | 161 MB/s | 235 MB |
| DataFusion | 2 | 2.6s | 225 MB/s | 333 MB |
| Lightweight | 1 | 4.2s | 136 MB/s | 206 MB |
