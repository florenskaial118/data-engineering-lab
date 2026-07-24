CREATE EXTENSION IF NOT EXISTS pxf;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS mart;

DROP EXTERNAL TABLE IF EXISTS raw.ext_ecommerce_orders_pxf;
DROP EXTERNAL TABLE IF EXISTS raw.ext_ecommerce_users_pxf;
DROP EXTERNAL TABLE IF EXISTS raw.ext_ecommerce_order_items_pxf;

CREATE EXTERNAL TABLE raw.ext_ecommerce_orders_pxf
(
    order_id bigint,
    user_id bigint,
    status text,
    amount numeric(12, 2),
    created_at timestamp,
    dt date
)
LOCATION ('pxf://datalake/raw/ecommerce/orders/dt=2026-07-01/orders.csv?PROFILE=s3:text&SERVER=minio')
FORMAT 'CSV' (HEADER);

CREATE EXTERNAL TABLE raw.ext_ecommerce_users_pxf
(
    user_id bigint,
    city text,
    segment text,
    registration_dt date
)
LOCATION ('pxf://datalake/raw/ecommerce/users/users.csv?PROFILE=s3:text&SERVER=minio')
FORMAT 'CSV' (HEADER);

CREATE EXTERNAL TABLE raw.ext_ecommerce_order_items_pxf
(
    order_item_id bigint,
    order_id bigint,
    product_id bigint,
    product_name text,
    quantity bigint,
    item_price numeric(12, 2),
    dt date


)
LOCATION ('pxf://datalake/raw/ecommerce/order_items/dt=2026-07-01/order_items.csv?PROFILE=s3:text&SERVER=minio')
FORMAT 'CSV' (HEADER);

CREATE TABLE stg.ecommerce_orders
(
    order_id bigint,
    user_id bigint,
    status text,
    amount numeric(12, 2),
    created_at timestamp,
    dt date
)
DISTRIBUTED BY (user_id);

CREATE TABLE stg.ecommerce_users
(
    user_id bigint,
    city text,
    segment text,
    registration_dt date
)
DISTRIBUTED BY (user_id);

CREATE TABLE stg.ecommerce_order_items
(
    order_item_id bigint,
    order_id bigint,
    product_id bigint,
    product_name text,
    quantity bigint,
    item_price numeric(12, 2),
    dt date
)
DISTRIBUTED BY (order_id);

INSERT INTO stg.ecommerce_orders
SELECT * FROM raw.ext_ecommerce_orders_pxf;

INSERT INTO stg.ecommerce_users
SELECT * FROM raw.ext_ecommerce_users_pxf;

INSERT INTO stg.ecommerce_order_items
SELECT * FROM raw.ext_ecommerce_order_items_pxf;

CREATE TABLE ods.ecommerce_orders
(
    order_id bigint,
    user_id bigint,
    status text,
    amount numeric(12, 2),
    created_at timestamp,
    dt date
)
DISTRIBUTED BY (user_id);

CREATE TABLE ods.ecommerce_users
(
    user_id bigint,
    city text,
    segment text,
    registration_dt date
)
DISTRIBUTED BY (user_id);

CREATE TABLE ods.ecommerce_order_items
(
    order_item_id bigint,
    order_id bigint,
    product_id bigint,
    product_name text,
    quantity bigint,
    item_price numeric(12, 2),
    dt date
)
DISTRIBUTED BY (order_id);

INSERT INTO ods.ecommerce_orders
SELECT * FROM stg.ecommerce_orders;

INSERT INTO ods.ecommerce_users
SELECT * FROM stg.ecommerce_users;

INSERT INTO ods.ecommerce_order_items
SELECT * FROM stg.ecommerce_order_items;

CREATE TABLE stg.ecommerce_orders_bad_dist
(
    order_id bigint,
    user_id bigint,
    status text,
    amount numeric(12, 2),
    created_at timestamp,
    dt date
)
DISTRIBUTED BY (order_id);

INSERT INTO stg.ecommerce_orders_bad_dist
SELECT * FROM raw.ext_ecommerce_orders_pxf;

EXPLAIN
SELECT u.city, count(*)
FROM ods.ecommerce_orders o
JOIN ods.ecommerce_users u ON o.user_id = u.user_id
GROUP BY u.city;

EXPLAIN
SELECT u.city, count(*)
FROM stg.ecommerce_orders_bad_dist o
JOIN ods.ecommerce_users u ON o.user_id = u.user_id
GROUP BY u.city;


CREATE TABLE mart.ecommerce_city_revenue (
    dt DATE, 
    city text,
    orders_count bigint,
    users_count bigint,
    items_count bigint, 
    total_revenue numeric(12, 2)
);

INSERT INTO mart.ecommerce_city_revenue
SELECT o.dt, city, 
count(DISTINCT oi.order_id) as orders_count,
count(DISTINCT u.user_id) as users_count,
sum(oi.quantity) as items_count,
sum(oi.item_price*oi.quantity) as total_revenue
from ods.ecommerce_orders o 
join ods.ecommerce_order_items oi on o.order_id = oi.order_id
join ods.ecommerce_users u on o.user_id = u.user_id
group by o.dt, city;

