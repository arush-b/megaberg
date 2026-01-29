-- Create namespace
CREATE NAMESPACE IF NOT EXISTS rest.db;

-- Create Iceberg table with 4 bucket partitions
CREATE TABLE IF NOT EXISTS rest.db.test_table (
  id BIGINT,
  name STRING
) USING iceberg
PARTITIONED BY (bucket(4, id))
TBLPROPERTIES (
  'write.target-file-size-bytes' = '24000'
);

-- Insert 40000 rows (4 partitions x ~10 files x 1000 rows)
INSERT INTO rest.db.test_table
SELECT
  id,
  concat('name_', cast(id as string)) as name
FROM range(0, 40000);

-- Verify
SELECT COUNT(*) as total_rows FROM rest.db.test_table;
SELECT partition, COUNT(*) as files
FROM rest.db.test_table.files
GROUP BY partition;
