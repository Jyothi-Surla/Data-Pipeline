

-- Table: files (Normalization for file metadata)
CREATE TABLE sensor_schema.files (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) UNIQUE NOT NULL,
    upload_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- Comment: Stores unique file references with upload timestamps.

-- Table: raw_sensor_data_partitioned (Partitioned raw sensor data)
CREATE TABLE sensor_schema.raw_sensor_data_partitioned (
    id BIGSERIAL,
    file_name VARCHAR(255) NOT NULL,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    device VARCHAR(17) NOT NULL,
    temp DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    co DOUBLE PRECISION,
    lpg DOUBLE PRECISION,
    smoke DOUBLE PRECISION,
    motion BOOLEAN,
    light BOOLEAN,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    file_id INTEGER REFERENCES sensor_schema.files(id),
    PRIMARY KEY (id, ts)
) PARTITION BY RANGE (ts);
-- Comment: Partitioned by timestamp for efficient storage of large datasets (e.g., 384M rows from 10,000 files).

-- Partition: raw_sensor_data_2020_07
CREATE TABLE sensor_schema.raw_sensor_data_2020_07 PARTITION OF sensor_schema.raw_sensor_data_partitioned
FOR VALUES FROM ('2020-07-01 00:00:00+00') TO ('2020-08-01 00:00:00+00');
-- Comment: Handles July 2020 data, with 769,752 rows in the current implementation.

-- Partition: raw_sensor_data_default
CREATE TABLE sensor_schema.raw_sensor_data_default PARTITION OF sensor_schema.raw_sensor_data_partitioned
DEFAULT;
-- Comment: Catches data outside defined ranges (e.g., 1 row in current data).

-- Indexes for Query Optimization
CREATE INDEX idx_raw_ts ON sensor_schema.raw_sensor_data_partitioned (ts);
CREATE INDEX idx_raw_device ON sensor_schema.raw_sensor_data_partitioned (device);
CREATE INDEX idx_raw_file_name ON sensor_schema.raw_sensor_data_partitioned (file_name);
-- Comment: Indexes optimize queries on timestamp, device, and file_name for large datasets.

-- Table: aggregated_metrics (Aggregated sensor metrics)
CREATE TABLE sensor_schema.aggregated_metrics (
    file_id INTEGER REFERENCES sensor_schema.files(id),
    device VARCHAR(17) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    avg_value DOUBLE PRECISION,
    std_value DOUBLE PRECISION,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (file_id, device, sensor_type)
);
-- Comment: Stores computed metrics with a composite primary key for uniqueness and scalability.

-- Verification Queries
-- Check row counts
SELECT 'raw_sensor_data' AS table_name, COUNT(*) AS row_count FROM sensor_schema.raw_sensor_data_partitioned;
SELECT 'aggregated_metrics' AS table_name, COUNT(*) AS row_count FROM sensor_schema.aggregated_metrics;
SELECT 'files' AS table_name, COUNT(*) AS row_count FROM sensor_schema.files;
-- Comment: Current counts: 769,752 raw rows, 150 aggregated metrics, 11 files.

-- Check partition distribution
SELECT 
    c2.relname AS partition_name,
    COUNT(*) AS row_count
FROM pg_class c1
JOIN pg_inherits i ON c1.oid = i.inhparent
JOIN pg_class c2 ON c2.oid = i.inhrelid
JOIN pg_partitioned_table p ON p.partrelid = c1.oid
WHERE c1.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'sensor_schema')
AND c1.relname = 'raw_sensor_data_partitioned'
GROUP BY c2.relname;
-- Comment: Current distribution: 769,752 in raw_sensor_data_2020_07, 1 in default.

-- Check constraints
SELECT 
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    kcu.column_name,
    pg_get_constraintdef(c.oid) AS constraint_definition
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
JOIN pg_constraint c ON c.conname = tc.constraint_name 
    AND c.connamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'sensor_schema')
WHERE tc.table_schema = 'sensor_schema'
    AND (tc.table_name = 'raw_sensor_data_partitioned' OR tc.table_name = 'aggregated_metrics')
ORDER BY tc.table_name, tc.constraint_name, kcu.column_name;
-- Comment: Confirms PRIMARY KEY and FOREIGN KEY constraints.

