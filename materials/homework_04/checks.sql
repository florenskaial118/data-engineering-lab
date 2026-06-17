-- Postgres DWH checks
-- Run in database: dwh

SELECT count(*) AS orders_count
FROM stg.orders_raw;

SELECT
    order_id,
    user_id,
    city,
    created_at,
    status,
    payment_status,
    payment_amount,
    items_count
FROM stg.orders_raw
ORDER BY order_id
LIMIT 10;

SELECT
    status,
    count(*) AS orders_count,
    sum(payment_amount) AS total_payment_amount
FROM stg.orders_raw
GROUP BY status
ORDER BY status;

-- Hive checks
-- Run through HiveServer2 after switching to database ods.

USE ods;

SHOW TABLES;

SELECT *
FROM order_city_stats
ORDER BY city;

SELECT *
FROM order_status_stats
ORDER BY status;

SELECT *
FROM category_revenue
ORDER BY total_item_amount DESC;
