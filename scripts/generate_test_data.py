#!/usr/bin/env python3
"""Generate test parquet files for megaberg E2E testing."""

import os
import random
import string
import tempfile
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

# Configuration
NUM_FILES = 20
ROWS_PER_FILE = 10000
OUTPUT_DIR = tempfile.mkdtemp(prefix="megaberg_test_")

def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters, k=length))

def generate_parquet_file(file_path: str, num_rows: int, file_idx: int):
    """Generate a single parquet file with test data."""

    # Generate data
    base_time = datetime(2024, 1, 15, 10, 0, 0)

    data = {
        "id": list(range(file_idx * num_rows, (file_idx + 1) * num_rows)),
        "event_time": [base_time + timedelta(seconds=i) for i in range(num_rows)],
        "user_id": [f"user_{random.randint(1, 1000)}" for _ in range(num_rows)],
        "event_type": [random.choice(["click", "view", "purchase", "signup"]) for _ in range(num_rows)],
        "value": [random.uniform(0, 1000) for _ in range(num_rows)],
        "metadata": [random_string(20) for _ in range(num_rows)],
    }

    # Create Arrow table
    table = pa.table(data)

    # Write parquet file
    pq.write_table(table, file_path, compression='snappy')

    return len(table)

def main():
    print(f"Generating {NUM_FILES} parquet files with {ROWS_PER_FILE} rows each...")
    print(f"Output directory: {OUTPUT_DIR}")

    total_rows = 0
    files = []

    for i in range(NUM_FILES):
        file_name = f"data_{i:04d}.parquet"
        file_path = os.path.join(OUTPUT_DIR, file_name)
        rows = generate_parquet_file(file_path, ROWS_PER_FILE, i)
        total_rows += rows
        files.append(file_path)
        print(f"  Generated: {file_name} ({rows} rows)")

    print(f"\nTotal: {total_rows} rows in {len(files)} files")
    print(f"\nUpload to MinIO with:")
    print(f"  mc cp --recursive {OUTPUT_DIR}/ local/lakehouse/compaction_test/")

    # Print schema
    print("\nSchema:")
    table = pq.read_table(files[0])
    print(table.schema)

    return OUTPUT_DIR

if __name__ == "__main__":
    main()
