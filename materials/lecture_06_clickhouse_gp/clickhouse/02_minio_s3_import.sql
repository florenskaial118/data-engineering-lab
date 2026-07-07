-- ClickHouse + MinIO demo.
-- Read CSV from MinIO through s3() and load it into MergeTree.
-- Run in default project database: lab.

DROP TABLE IF EXISTS orders_from_minio;

-- 1. Direct read from MinIO.
-- Inside Docker network use http://minio:9000, not localhost.
SELECT
    order_id,
    user_id,
    status,
    amount,
    created_at,
    dt
FROM s3(
    'http://minio:9000/datalake/raw/orders/dt=2026-06-23/orders.csv',
    'admin',
    'password123',
    'CSVWithNames',
    'order_id UInt64, user_id UInt64, status String, amount Float64, created_at String, dt Date'
)
ORDER BY order_id;

-- 2. Create a normal local ClickHouse table.
CREATE TABLE orders_from_minio
(
    order_id UInt64,
    user_id UInt64,
    status String,
    amount Float64,
    created_at DateTime,
    dt Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, user_id, order_id);

-- 3. Load data from MinIO into MergeTree.
INSERT INTO orders_from_minio
SELECT
    order_id,
    user_id,
    status,
    amount,
    parseDateTimeBestEffort(created_at) AS created_at,
    dt
FROM s3(
    'http://minio:9000/datalake/raw/orders/dt=2026-06-23/orders.csv',
    'admin',
    'password123',
    'CSVWithNames',
    'order_id UInt64, user_id UInt64, status String, amount Float64, created_at String, dt Date'
);

-- 4. Analytics from local MergeTree table.
SELECT
    dt,
    status,
    count() AS orders_count,
    round(sum(amount), 2) AS total_amount
FROM orders_from_minio
GROUP BY dt, status
ORDER BY dt, status;

EXPLAIN indexes = 1
SELECT
    user_id,
    round(sum(amount), 2) AS total_amount
FROM orders_from_minio
WHERE dt = '2026-06-23'
GROUP BY user_id
ORDER BY total_amount DESC;
