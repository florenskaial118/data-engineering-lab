from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from pathlib import Path
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def upload_raw_to_s3():
    conn = BaseHook.get_connection('minio_s3')

    # Defaults подходят для MinIO в docker-compose этого проекта.
    S3_ENDPOINT = os.getenv('S3_ENDPOINT') or os.getenv('S3_ENDPOINT_URL') or 'http://localhost:9000'
    S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'admin')
    S3_SECRET_KEY = os.getenv('S3_SECRET_KEY', 'password123')
    S3_BUCKET = os.getenv('S3_BUCKET', 'datalake')
    S3_REGION = os.getenv('S3_REGION', 'us-east-1')
    S3_PATH_STYLE_ACCESS = os.getenv('S3_PATH_STYLE_ACCESS', 'true')
    
    RAW_KEY_ORDERS = 'raw/ecommerce/orders/dt=2026-07-01/orders.csv'
    RAW_KEY_ORDER_ITEMS = 'raw/ecommerce/order_items/dt=2026-07-01/order_items.csv'
    RAW_KEY_USERS = 'raw/ecommerce/users/users.csv'
    LOCAL_ORDERS = Path('/opt/airflow/jobs/ecommerce/orders.csv')
    LOCAL_ORDER_ITEMS = Path('/opt/airflow/jobs/ecommerce/order_items.csv')
    LOCAL_USERS = Path('/opt/airflow/jobs/ecommerce/users.csv')

    
    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
    )

    s3.upload_file(str(LOCAL_ORDERS), S3_BUCKET, RAW_KEY_ORDERS)
    print('uploaded:', f's3://{S3_BUCKET}/{RAW_KEY_ORDERS}')

    s3.upload_file(str(LOCAL_ORDER_ITEMS), S3_BUCKET, RAW_KEY_ORDER_ITEMS)
    print('uploaded:', f's3://{S3_BUCKET}/{RAW_KEY_ORDER_ITEMS}')

    s3.upload_file(str(LOCAL_USERS), S3_BUCKET, RAW_KEY_USERS)
    print('uploaded:', f's3://{S3_BUCKET}/{RAW_KEY_USERS}')

def create_clickhouse_layers():
    return [
    "CREATE DATABASE IF NOT EXISTS raw;",
    "CREATE DATABASE IF NOT EXISTS ods;",
    "CREATE DATABASE IF NOT EXISTS mart;",

    "DROP TABLE IF EXISTS ods.ecommerce_orders;",
    "DROP TABLE IF EXISTS ods.ecommerce_users;",
    "DROP TABLE IF EXISTS ods.ecommerce_order_items;",

    """ CREATE TABLE ods.ecommerce_orders
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
    ORDER BY (dt, user_id, order_id);""",


    """CREATE TABLE ods.ecommerce_order_items
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
    ORDER BY (dt, order_id, product_id);""",

    """CREATE TABLE ods.ecommerce_users
    (
        user_id UInt64, 
        city String, 
        segment String, 
        registration_dt Date

    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(registration_dt)
    ORDER BY (user_id, city);"""
    ]
def load_clickhouse_ods():
    return [
    """INSERT INTO ods.ecommerce_orders
    SELECT
        order_id,user_id,status,amount,created_at,dt
    FROM s3(
        'http://minio:9000/datalake/raw/ecommerce/orders/dt=2026-07-01/orders.csv',
        'admin',
        'password123',
        'CSVWithNames',
        'order_id UInt64, user_id UInt64, status String, amount Float64, created_at DateTime, dt Date'
    );""",

    """INSERT INTO ods.ecommerce_order_items
    SELECT
        order_item_id,order_id,product_id,product_name,quantity,item_price,dt
    FROM s3(
        'http://minio:9000/datalake/raw/ecommerce/order_items/dt=2026-07-01/order_items.csv',
        'admin',
        'password123',
        'CSVWithNames',
        'order_item_id UInt64, order_id UInt64, product_id UInt64, product_name String, quantity UInt64, item_price Float64, dt Date'
    );""",

    """INSERT INTO ods.ecommerce_users
    SELECT
        user_id,city,segment,registration_dt
    FROM s3(
        'http://minio:9000/datalake/raw/ecommerce/users/users.csv',
        'admin',
        'password123',
        'CSVWithNames',
        'user_id UInt64, city String, segment String, registration_dt Date'
    );"""
]

def build_clickhouse_marts():
    return [
    """ DROP TABLE IF EXISTS mart.ecommerce_city_revenue;""",
    """CREATE TABLE mart.ecommerce_city_revenue (
        dt DATE, 
        city String,
        orders_count UInt64,
        users_count UInt64,
        items_count UInt64, 
        total_revenue Float64
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(dt)
    ORDER BY (dt, city);""",

    """INSERT INTO mart.ecommerce_city_revenue
    SELECT o.dt, city, 
    count(DISTINCT oi.order_id) as orders_count,
    count(DISTINCT u.user_id) as users_count,
    sum(oi.quantity) as items_count,
    sum(oi.item_price*oi.quantity) as total_revenue
    from ods.ecommerce_orders o 
    join ods.ecommerce_order_items oi on o.order_id = oi.order_id
    join ods.ecommerce_users u on o.user_id = u.user_id
    group by o.dt, city;""",

    """DROP TABLE IF EXISTS mart.ecommerce_product_revenue;""",
    """CREATE TABLE mart.ecommerce_product_revenue (
        dt DATE, 
        product_id UInt64,
        product_name String,
        items_count UInt64,
        total_quantity UInt64, 
        total_revenue Float64
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(dt)
    ORDER BY (dt, product_id, product_name);""",

    """INSERT INTO mart.ecommerce_product_revenue
    SELECT o.dt, product_id, product_name,
    count(DISTINCT oi.order_item_id) as items_count,
    sum(oi.quantity) as total_quantity,
    sum(oi.item_price*oi.quantity) as total_revenue
    from ods.ecommerce_order_items oi join ods.ecommerce_orders o on oi.order_id = o.order_id
    group by o.dt, product_id, product_name;"""

    ]

def create_greenplum_layers():
    return [
    "CREATE EXTENSION IF NOT EXISTS pxf;",

    "CREATE SCHEMA IF NOT EXISTS raw;",
    "CREATE SCHEMA IF NOT EXISTS stg;",
    "CREATE SCHEMA IF NOT EXISTS ods;",
    "CREATE SCHEMA IF NOT EXISTS mart;",
    "DROP EXTERNAL TABLE IF EXISTS raw.ext_ecommerce_orders_pxf;",
    "DROP EXTERNAL TABLE IF EXISTS raw.ext_ecommerce_users_pxf;",
    "DROP EXTERNAL TABLE IF EXISTS raw.ext_ecommerce_order_items_pxf;",
    "DROP TABLE IF EXISTS stg.ecommerce_orders;",
    "DROP TABLE IF EXISTS stg.ecommerce_users;",
    "DROP TABLE IF EXISTS stg.ecommerce_order_items;",
    "DROP TABLE IF EXISTS ods.ecommerce_orders;",
    "DROP TABLE IF EXISTS ods.ecommerce_users;",
    "DROP TABLE IF EXISTS ods.ecommerce_order_items;",

    """CREATE EXTERNAL TABLE raw.ext_ecommerce_orders_pxf
    (
        order_id bigint,
        user_id bigint,
        status text,
        amount numeric(12, 2),
        created_at timestamp,
        dt date
    )
    LOCATION ('pxf://datalake/raw/ecommerce/orders/dt=2026-07-01/orders.csv?PROFILE=s3:text&SERVER=minio')
    FORMAT 'CSV' (HEADER);""",

    """CREATE EXTERNAL TABLE raw.ext_ecommerce_users_pxf
    (
        user_id bigint,
        city text,
        segment text,
        registration_dt date
    )
    LOCATION ('pxf://datalake/raw/ecommerce/users/users.csv?PROFILE=s3:text&SERVER=minio')
    FORMAT 'CSV' (HEADER);""",

    """CREATE EXTERNAL TABLE raw.ext_ecommerce_order_items_pxf
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
    FORMAT 'CSV' (HEADER);""",


    """CREATE TABLE stg.ecommerce_orders
    (
        order_id bigint,
        user_id bigint,
        status text,
        amount numeric(12, 2),
        created_at timestamp,
        dt date
    )
    DISTRIBUTED BY (user_id);""",

    """CREATE TABLE stg.ecommerce_users
    (
        user_id bigint,
        city text,
        segment text,
        registration_dt date
    )
    DISTRIBUTED BY (user_id);""",

    """CREATE TABLE stg.ecommerce_order_items
    (
        order_item_id bigint,
        order_id bigint,
        product_id bigint,
        product_name text,
        quantity bigint,
        item_price numeric(12, 2),
        dt date
    )
    DISTRIBUTED BY (order_id);""",

    """CREATE TABLE ods.ecommerce_orders
    (
        order_id bigint,
        user_id bigint,
        status text,
        amount numeric(12, 2),
        created_at timestamp,
        dt date
    )
    DISTRIBUTED BY (user_id);""",

    """CREATE TABLE ods.ecommerce_users
    (
        user_id bigint,
        city text,
        segment text,
        registration_dt date
    )
    DISTRIBUTED BY (user_id);""",

    """CREATE TABLE ods.ecommerce_order_items
    (
        order_item_id bigint,
        order_id bigint,
        product_id bigint,
        product_name text,
        quantity bigint,
        item_price numeric(12, 2),
        dt date
    )
    DISTRIBUTED BY (order_id);"""
]

def load_greenplum_from_pxf():
    return """
    INSERT INTO stg.ecommerce_orders
    SELECT * FROM raw.ext_ecommerce_orders_pxf;

    INSERT INTO stg.ecommerce_users
    SELECT * FROM raw.ext_ecommerce_users_pxf;

    INSERT INTO stg.ecommerce_order_items
    SELECT * FROM raw.ext_ecommerce_order_items_pxf;

    INSERT INTO ods.ecommerce_orders
    SELECT * FROM stg.ecommerce_orders;

    INSERT INTO ods.ecommerce_users
    SELECT * FROM stg.ecommerce_users;

    INSERT INTO ods.ecommerce_order_items
    SELECT * FROM stg.ecommerce_order_items;

"""

def build_greenplum_mart():
    return """
    DROP TABLE IF EXISTS mart.ecommerce_city_revenue;
    
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
"""


with DAG(
    dag_id="ecommerce_s3_clickhouse_greenplum",
    schedule=None,
    catchup=False
) as dag:
    upload_raw_to_s3 = PythonOperator(
        task_id = "upload_raw_to_s3",
        python_callable = upload_raw_to_s3
        )
    create_clickhouse_layers = ClickHouseOperator(
        task_id = "create_clickhouse_layers",
        clickhouse_conn_id='clickhouse_default',
        sql=create_clickhouse_layers())
    load_clickhouse_ods = ClickHouseOperator(
        task_id = "load_clickhouse_ods",
        clickhouse_conn_id='clickhouse_default',
        sql=load_clickhouse_ods())
    build_clickhouse_marts = ClickHouseOperator(
        task_id = "build_clickhouse_marts",
        clickhouse_conn_id='clickhouse_default',
        sql=build_clickhouse_marts())
    create_greenplum_layers = PostgresOperator(
        task_id = "create_greenplum_layers",
        postgres_conn_id='greenplum_default',
        sql=create_greenplum_layers())
    load_greenplum_from_pxf = PostgresOperator(
        task_id = "load_greenplum_from_pxf",
        postgres_conn_id='greenplum_default',
        sql=load_greenplum_from_pxf())
    build_greenplum_mart = PostgresOperator(
        task_id = "build_greenplum_mart",
        postgres_conn_id='greenplum_default',
        sql=build_greenplum_mart())


    upload_raw_to_s3 >> create_clickhouse_layers >> load_clickhouse_ods >> build_clickhouse_marts >> create_greenplum_layers >> load_greenplum_from_pxf >> build_greenplum_mart