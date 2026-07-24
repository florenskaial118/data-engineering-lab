CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS mart;

SELECT

    order_id,user_id,status,amount,created_at,dt
FROM s3(
    'http://minio:9000/datalake/raw/ecommerce/orders/dt=2026-07-01/orders.csv',
    'admin',
    'password123',
    'CSVWithNames',
    'order_id UInt64, user_id UInt64, status String, amount Float64, created_at DateTime, dt Date'
);

SELECT
    order_item_id,order_id,product_id,product_name,quantity,item_price,dt
FROM s3(
    'http://minio:9000/datalake/raw/ecommerce/order_items/dt=2026-07-01/order_items.csv',
    'admin',
    'password123',
    'CSVWithNames',
    'order_item_id UInt64, order_id UInt64, product_id UInt64, product_name String, quantity UInt64, item_price Float64, dt Date'
);

SELECT
    user_id,city,segment,registration_dt
FROM s3(
    'http://minio:9000/datalake/raw/ecommerce/users/users.csv',
    'admin',
    'password123',
    'CSVWithNames',
    'user_id UInt64, city String, segment String, registration_dt Date'
);

DROP TABLE IF EXISTS ods.ecommerce_orders;
DROP TABLE IF EXISTS ods.ecommerce_users;
DROP TABLE IF EXISTS ods.ecommerce_order_items;

CREATE TABLE ods.ecommerce_orders
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


CREATE TABLE ods.ecommerce_order_items
(
    order_item_id UInt64, 
    order_id UInt64, 
    product_id UInt64, 
    product_name String, 
    quantity UInt64, 
    item_price Float64, 
    dt Date

)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, order_id, product_id);

CREATE TABLE ods.ecommerce_users
(
    user_id UInt64, 
    city String, 
    segment String, 
    registration_dt Date

)
ENGINE = MergeTree
PARTITION BY toYYYYMM(registration_dt)
ORDER BY (user_id, city);





DROP TABLE IF EXISTS mart.ecommerce_city_revenue;
CREATE TABLE mart.ecommerce_city_revenue (
    dt DATE, 
    city String,
    orders_count UInt64,
    users_count UInt64,
    items_count UInt64, 
    total_revenue Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, city);

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

EXPLAIN indexes = 1
SELECT o.dt, city, 
count(DISTINCT oi.order_id) as orders_count,
count(DISTINCT u.user_id) as users_count,
sum(oi.quantity) as items_count,
sum(oi.item_price*oi.quantity) as total_revenue
from ods.ecommerce_orders o 
join ods.ecommerce_order_items oi on o.order_id = oi.order_id
join ods.ecommerce_users u on o.user_id = u.user_id
group by o.dt, city;



DROP TABLE IF EXISTS mart.ecommerce_product_revenue;
CREATE TABLE mart.ecommerce_product_revenue (
    dt DATE, 
    product_id UInt64,
    product_name String,
    items_count UInt64,
    total_quantity UInt64, 
    total_revenue Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, product_id, product_name);

INSERT INTO mart.ecommerce_product_revenue
SELECT o.dt, product_id, product_name,
count(DISTINCT oi.order_item_id) as items_count,
sum(oi.quantity) as total_quantity,
sum(oi.item_price*oi.quantity) as total_revenue
from ods.ecommerce_order_items oi join ods.ecommerce_orders o on oi.order_id = o.order_id
group by o.dt, product_id, product_name;

EXPLAIN indexes = 1
SELECT o.dt, product_id, product_name,
count(DISTINCT oi.order_item_id) as items_count,
sum(oi.quantity) as total_quantity,
sum(oi.item_price*oi.quantity) as total_revenue
from ods.ecommerce_order_items oi join ods.ecommerce_orders o on oi.order_id = o.order_id
group by o.dt, product_id, product_name