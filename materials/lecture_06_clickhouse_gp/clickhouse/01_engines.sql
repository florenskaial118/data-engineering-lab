
DROP TABLE IF EXISTS orders_mt;
DROP TABLE IF EXISTS orders_bad_order;
DROP TABLE IF EXISTS order_status_replacing;
DROP TABLE IF EXISTS order_amounts_summing;
DROP TABLE IF EXISTS orders_memory;

-- 1. MergeTree: основной движок для аналитических таблиц.
CREATE TABLE orders_mt
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

INSERT INTO orders_mt VALUES
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

SELECT
    dt,
    status,
    count() AS orders_count,
    round(sum(amount), 2) AS total_amount
FROM orders_mt
GROUP BY dt, status
ORDER BY dt, status;

-- План запроса. На маленьких данных ускорения не видно, но видно структуру чтения.
EXPLAIN indexes = 1
SELECT
    user_id,
    sum(amount) AS total_amount
FROM orders_mt
WHERE dt = '2026-06-23'
  AND user_id IN (501, 506)
GROUP BY user_id;

-- 2. Плохой ORDER BY: сортировка по order_id плохо помогает запросам по dt/user_id.
CREATE TABLE orders_bad_order
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
ORDER BY order_id;

INSERT INTO orders_bad_order
SELECT * FROM orders_mt;

EXPLAIN indexes = 1
SELECT
    user_id,
    sum(amount) AS total_amount
FROM orders_bad_order
WHERE dt = '2026-06-23'
  AND user_id IN (501, 506)
GROUP BY user_id;

-- 3. ReplacingMergeTree: хранит версии, но это не OLTP update.
-- Старые версии схлопываются во время merge. FINAL делает схлопывание на чтении и может быть дорогим.
CREATE TABLE order_status_replacing
(
    order_id UInt64,
    status String,
    version UInt64,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(version)
ORDER BY order_id;

INSERT INTO order_status_replacing VALUES
    (1001, 'created', 1, '2026-06-23 09:15:00'),
    (1001, 'paid', 2, '2026-06-23 09:20:00'),
    (1001, 'shipped', 3, '2026-06-23 10:05:00'),
    (1002, 'created', 1, '2026-06-23 09:18:00'),
    (1002, 'paid', 2, '2026-06-23 09:20:00');

SELECT 'ReplacingMergeTree without FINAL' AS demo;
SELECT * FROM order_status_replacing ORDER BY order_id, version;

SELECT 'ReplacingMergeTree with FINAL' AS demo;
SELECT * FROM order_status_replacing FINAL ORDER BY order_id;

-- 4. SummingMergeTree: пример предагрегированной таблицы.
CREATE TABLE order_amounts_summing
(
    dt Date,
    status String,
    orders_count UInt64,
    amount_sum Float64
)
ENGINE = SummingMergeTree
ORDER BY (dt, status);

INSERT INTO order_amounts_summing
SELECT
    dt,
    status,
    count() AS orders_count,
    sum(amount) AS amount_sum
FROM orders_mt
GROUP BY dt, status;

-- Повторная вставка показывает, что SummingMergeTree суммирует строки с одинаковым ключом при merge.
INSERT INTO order_amounts_summing
SELECT
    dt,
    status,
    count() AS orders_count,
    sum(amount) AS amount_sum
FROM orders_mt
GROUP BY dt, status;

SELECT
    dt,
    status,
    sum(orders_count) AS orders_count,
    round(sum(amount_sum), 2) AS amount_sum
FROM order_amounts_summing
GROUP BY dt, status
ORDER BY dt, status;

-- 5. Memory: временный in-memory engine. Данные пропадают после restart сервера.
CREATE TABLE orders_memory
(
    order_id UInt64,
    user_id UInt64,
    amount Float64
)
ENGINE = Memory;

INSERT INTO orders_memory VALUES
    (1, 501, 100.00),
    (2, 502, 200.00);

SELECT * FROM orders_memory ORDER BY order_id;

-- Быстрый просмотр созданных таблиц.
SHOW TABLES LIKE '%orders%';
