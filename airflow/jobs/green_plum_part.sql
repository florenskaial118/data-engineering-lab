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
LOCATION ('pxf://datalake/raw/orders/dt=2026-07-01/orders.csv?PROFILE=s3:text&SERVER=minio')
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
LOCATION ('pxf://order_items/dt=2026-07-01/order_items.csv?PROFILE=s3:text&SERVER=minio')
FORMAT 'CSV' (HEADER);