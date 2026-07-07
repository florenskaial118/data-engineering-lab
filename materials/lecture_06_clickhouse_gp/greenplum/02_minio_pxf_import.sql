-- Greenplum + MinIO through PXF protocol demo.
-- This lab uses a small local pxf-mock service that implements the minimal PXF HTTP API
-- needed by Greenplum's pxf protocol to stream CSV from MinIO.
-- Run in default database postgres, default schema public.

CREATE EXTENSION IF NOT EXISTS pxf;

DROP EXTERNAL TABLE IF EXISTS ext_orders_minio_pxf;
DROP TABLE IF EXISTS orders_from_minio_pxf;

-- pxf:// location is handled by Greenplum pxf protocol.
-- This old Greenplum/PXF protocol implementation connects to localhost:5888.
-- The lab image starts pxf-mock inside the Greenplum container on that port.
CREATE EXTERNAL TABLE ext_orders_minio_pxf
(
    order_id bigint,
    user_id bigint,
    status text,
    amount numeric(12, 2),
    created_at timestamp,
    dt date
)
LOCATION ('pxf://datalake/raw/orders/dt=2026-06-23/orders.csv?PROFILE=s3:text&SERVER=minio')
FORMAT 'CSV' (HEADER);

SELECT *
FROM ext_orders_minio_pxf
ORDER BY order_id;

CREATE TABLE orders_from_minio_pxf
(
    order_id bigint,
    user_id bigint,
    status text,
    amount numeric(12, 2),
    created_at timestamp,
    dt date
)
DISTRIBUTED BY (user_id);

INSERT INTO orders_from_minio_pxf
SELECT *
FROM ext_orders_minio_pxf;

SELECT
    dt,
    status,
    count(*) AS orders_count,
    sum(amount) AS total_amount
FROM orders_from_minio_pxf
GROUP BY dt, status
ORDER BY dt, status;

EXPLAIN
SELECT
    user_id,
    sum(amount) AS total_amount
FROM orders_from_minio_pxf
WHERE dt = DATE '2026-06-23'
GROUP BY user_id;
