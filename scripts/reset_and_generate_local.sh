#!/bin/bash
set -e

# Configuration
NAMESPACE="${NAMESPACE:-default}"
TABLE="${TABLE:-test_table}"
CONFIG_FILE="${CONFIG_FILE:-local_config.json}"
REST_CATALOG="http://localhost:8181"

echo "=== Resetting table ${NAMESPACE}.${TABLE} on local setup ==="

# Step 1: Delete table from REST catalog
echo "Deleting table from REST catalog..."
curl -s -X DELETE "${REST_CATALOG}/v1/namespaces/${NAMESPACE}/tables/${TABLE}" || echo "Table doesn't exist or already deleted"

# Step 2: Delete data from MinIO
echo "Deleting data from MinIO..."
docker exec mc mc rm -r --force minio/lakehouse/${NAMESPACE}/${TABLE}/ 2>/dev/null || echo "No data to delete"

# Step 3: Create namespace if it doesn't exist
echo "Ensuring namespace exists..."
curl -s -X POST "${REST_CATALOG}/v1/namespaces" \
  -H "Content-Type: application/json" \
  -d "{\"namespace\": [\"${NAMESPACE}\"]}" 2>/dev/null || echo "Namespace may already exist"

# Step 4: Create table
echo "Creating table..."
curl -s -X POST "${REST_CATALOG}/v1/namespaces/${NAMESPACE}/tables" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "'"${TABLE}"'",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "id", "type": "long", "required": true},
        {"id": 2, "name": "timestamp", "type": "long", "required": true},
        {"id": 3, "name": "value", "type": "double", "required": true},
        {"id": 4, "name": "category", "type": "string", "required": true},
        {"id": 5, "name": "description", "type": "string", "required": true}
      ]
    },
    "properties": {
      "write.parquet.compression-codec": "uncompressed"
    }
  }'

# Step 5: Run data generation
echo -e "\nRunning data generation..."
cd "$(dirname "$0")/.."
cargo run --example generate_data -- --config "$CONFIG_FILE"

echo "=== Done! ==="
