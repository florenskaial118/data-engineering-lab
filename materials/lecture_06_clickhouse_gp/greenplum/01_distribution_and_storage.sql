-- Greenplum distribution and storage demo.
-- Run in default database postgres, default schema public.

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS orders_by_order_id;
DROP TABLE IF EXISTS staging_orders_random;
DROP TABLE IF EXISTS orders_heap;
DROP TABLE IF EXISTS orders_ao_row;
DROP TABLE IF EXISTS orders_ao_column;

-- 1. Good distribution key for joins by user_id.
CREATE TABLE orders
(
    order_id bigint,
    user_id bigint,
    status text,
    amount numeric(12, 2),
    created_at timestamp,
    dt date
)
DISTRIBUTED BY (user_id);

CREATE TABLE users
(
    user_id bigint,
    city text,
    registration_dt date
)
DISTRIBUTED BY (user_id);

INSERT INTO users VALUES
    (501, 'Moscow', '2026-01-10'),
    (502, 'Saint Petersburg', '2026-01-15'),
    (503, 'Kazan', '2026-02-01'),
    (504, 'Moscow', '2026-02-11'),
    (505, 'Novosibirsk', '2026-03-02'),
    (506, 'Moscow', '2026-03-20'),
    (507, 'Kazan', '2026-04-02'),
    (508, 'Moscow', '2026-04-12'),
    (509, 'Novosibirsk', '2026-05-01');

INSERT INTO orders VALUES
    (1001, 501, 'created', 1250.50, '2026-06-23 09:15:00', '2026-06-23'),
    (1002, 502, 'paid', 3200.00, '2026-06-23 09:20:00', '2026-06-23'),
    (1003, 503, 'paid', 790.99, '2026-06-23 09:45:00', '2026-06-23'),
    (1004, 501, 'shipped', 1250.50, '2026-06-23 10:05:00', '2026-06-23'),
    (1005, 504, 'cancelled', 450.00, '2026-06-23 10:20:00', '2026-06-23'),
    (1006, 505, 'created', 1999.90, '2026-06-23 10:35:00', '2026-06-23'),
    (1007, 506, 'paid', 8750.00, '2026-06-23 11:00:00', '2026-06-23'),
    (1008, 507, 'paid', 150.75, '2026-06-23 11:10:00', '2026-06-23'),
    (1009, 508, 'shipped', 4320.40, '2026-06-23 11:25:00', '2026-06-23'),
    (1010, 509, 'created', 999.00, '2026-06-23 11:50:00', '2026-06-23');

-- Good join: both tables are distributed by user_id.
EXPLAIN
SELECT
    u.city,
    count(*) AS orders_count,
    sum(o.amount) AS total_amount
FROM orders o
JOIN users u ON o.user_id = u.user_id
GROUP BY u.city
ORDER BY u.city;

SELECT
    u.city,
    count(*) AS orders_count,
    sum(o.amount) AS total_amount
FROM orders o
JOIN users u ON o.user_id = u.user_id
GROUP BY u.city
ORDER BY u.city;

-- 2. Random distribution: ok for staging, not ideal for large joins.
CREATE TABLE staging_orders_random
(
    order_id bigint,
    user_id bigint,
    status text,
    amount numeric(12, 2),
    created_at timestamp,
    dt date
)
DISTRIBUTED RANDOMLY;

INSERT INTO staging_orders_random
SELECT * FROM orders;

EXPLAIN
SELECT
    u.city,
    count(*) AS orders_count
FROM staging_orders_random o
JOIN users u ON o.user_id = u.user_id
GROUP BY u.city;

-- 3. Bad distribution for join by user_id.
CREATE TABLE orders_by_order_id
(
    order_id bigint,
    user_id bigint,
    status text,
    amount numeric(12, 2),
    created_at timestamp,
    dt date
)
DISTRIBUTED BY (order_id);

INSERT INTO orders_by_order_id
SELECT * FROM orders;

-- On larger data this design commonly causes Redistribute Motion for join by user_id.
EXPLAIN
SELECT
    u.city,
    count(*) AS orders_count,
    sum(o.amount) AS total_amount
FROM orders_by_order_id o
JOIN users u ON o.user_id = u.user_id
GROUP BY u.city;

-- 4. Heap table: default storage, useful for small/staging tables.
CREATE TABLE orders_heap
(
    order_id bigint,
    user_id bigint,
    amount numeric(12, 2),
    dt date
)
DISTRIBUTED BY (user_id);

INSERT INTO orders_heap
SELECT order_id, user_id, amount, dt FROM orders;

-- 5. Append-optimized row table.
CREATE TABLE orders_ao_row
(
    order_id bigint,
    user_id bigint,
    amount numeric(12, 2),
    dt date
)
WITH (
    appendoptimized = true,
    orientation = row
)
DISTRIBUTED BY (user_id);

INSERT INTO orders_ao_row
SELECT order_id, user_id, amount, dt FROM orders;

-- 6. Append-optimized column table.
-- Storage parameters depend on Greenplum version. This works on many Greenplum 6 setups.
CREATE TABLE orders_ao_column
(
    order_id bigint,
    user_id bigint,
    amount numeric(12, 2),
    dt date
)
WITH (
    appendoptimized = true,
    orientation = column,
    compresstype = zlib,
    compresslevel = 5
)
DISTRIBUTED BY (user_id);

INSERT INTO orders_ao_column
SELECT order_id, user_id, amount, dt FROM orders;

EXPLAIN
SELECT
    dt,
    sum(amount) AS total_amount
FROM orders_ao_column
GROUP BY dt;
