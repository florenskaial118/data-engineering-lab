#!/usr/bin/env python3

import os
from pathlib import Path
from textwrap import dedent

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
LOCAL_ORDERS = Path('airflow/jobs/ecommerce/orders.csv')
LOCAL_ORDER_ITEMS = Path('airflow/jobs/ecommerce/order_items.csv')
LOCAL_USERS = Path('airflow/jobs/ecommerce/users.csv')

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

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