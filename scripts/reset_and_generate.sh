#!/bin/bash
set -e

# Check AWS credentials
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "Error: AWS_ACCESS_KEY_ID not set"
    exit 1
fi

# Configuration
DATABASE="${DATABASE:-laminar_iceberg}"
TABLE="${TABLE:-laminar_shreyas_compaction}"
BUCKET="${BUCKET:-laminar-dev}"
WAREHOUSE_PATH="warehouse/${DATABASE}/${TABLE}"
CONFIG_FILE="${CONFIG_FILE:-generate_data_config.json}"

echo "=== Resetting table ${DATABASE}.${TABLE} ==="

# Step 1: Delete Glue table
echo "Deleting Glue table..."
aws glue delete-table --database-name "$DATABASE" --name "$TABLE" 2>/dev/null || echo "Table doesn't exist or already deleted"

# Step 2: Delete data from S3
echo "Deleting S3 data at s3://${BUCKET}/${WAREHOUSE_PATH}/..."
aws s3 rm "s3://${BUCKET}/${WAREHOUSE_PATH}/" --recursive 2>/dev/null || echo "No data to delete"

# Step 3: Run data generation
echo "Running data generation..."
cd "$(dirname "$0")/.."
cargo run --example generate_data -- --config "$CONFIG_FILE"

echo "=== Done! ==="
