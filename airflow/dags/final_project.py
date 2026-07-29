from airflow import DAG
import requests
from datetime import datetime
date = datetime.now().strftime('%Y-%m-%d')
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from clickhouse_driver import Client
from tabulate import tabulate
import pandas as pd


def extract_api():
    response_posts = requests.get("https://jsonplaceholder.typicode.com/posts",
        params={
            "limit":3,
            "skip":0
        },
        timeout = 10,
    )


    response_comments = requests.get("https://jsonplaceholder.typicode.com/comments",
        params={
            "limit":3,
            "skip":0
        },
        timeout = 10,
    )

    response_users = requests.get("https://jsonplaceholder.typicode.com/users",
        params={
            "limit":3,
            "skip":0
        },
        timeout = 10,
    )

    resp_posts = requests.put(
            f'http://namenode:9870/webhdfs/v1/warehouse/raw/JSONPlaceholder/posts/dt={date}/posts.json?op=CREATE&overwrite=true&createparent=true',
            allow_redirects=False
        )

    if resp_posts.status_code == 307:
        resp_upload = requests.put(resp_posts.headers['Location'], data=response_posts.content)

    resp_comments = requests.put(
            f'http://namenode:9870/webhdfs/v1/warehouse/raw/JSONPlaceholder/comments/dt={date}/comments.json?op=CREATE&overwrite=true&createparent=true',
            allow_redirects=False
        )

    if resp_comments.status_code == 307:
        resp_upload = requests.put(resp_comments.headers['Location'], data=response_comments.content)

    resp_users = requests.put(
            f'http://namenode:9870/webhdfs/v1/warehouse/raw/JSONPlaceholder/users/dt={date}/users.json?op=CREATE&overwrite=true&createparent=true',
            allow_redirects=False
        )

    if resp_users.status_code == 307:
        resp_upload = requests.put(resp_users.headers['Location'], data=response_users.content)

def load_mart():
    return [
        'CREATE DATABASE IF NOT EXISTS mart;',
        'DROP TABLE IF EXISTS mart.JSONPlaceholder_UsersActivity;',
        """CREATE TABLE IF NOT EXISTS mart.JSONPlaceholder_UsersActivity (
            userId UInt64,
            post_cnt UInt64,
            got_comment_cnt UInt64,
            address_city String,
            company_name String,
            userEmail String,
            website String
        ) ENGINE = MergeTree
        ORDER BY userId;""",
        """INSERT INTO mart.JSONPlaceholder_UsersActivity
        SELECT *
        FROM hdfs(
            'hdfs://namenode:8020/warehouse/mart/JSONPlaceholder_UsersActivity/UsersActivity/*.parquet',
            'Parquet',
            'userId UInt64, post_cnt UInt64, got_comment_cnt UInt64, address_city String, company_name String, userEmail String, website String'
        )"""
    ]

def check_result():
    client = Client(
        host='clickhouse',
        port=9000,
        user='admin',
        password='admin',
        database='default'
    )
    result = client.execute("SELECT * FROM mart.JSONPlaceholder_UsersActivity ORDER BY userId")
    
    for row in result:
        print(f"User {row[0]}: {row[1]} posts, {row[2]} comments | {row[3]} | {row[4]}")

    
with DAG (
    dag_id = 'final_project',
    schedule = None,
    catchup = False
) as dag:
    extract_api = PythonOperator(
        task_id = 'extract_api',
        python_callable = extract_api
    )
    load_raw = SparkSubmitOperator(
        task_id = 'load_raw',
        conn_id = "spark_default",
        application ="/opt/airflow/jobs/spark_raw_ods_part.py",
        name = "spark_job",
        verbose = True,
        env_vars={
             "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-arm64",
         },
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:8020",
            "spark.sql.warehouse.dir": "hdfs://namenode:8020/user/hive/warehouse",
            "spark.driver.extraJavaOptions": "-Duser.home=/tmp",
            "spark.executor.extraJavaOptions": "-Duser.home=/tmp",
            },
    )
    transform = SparkSubmitOperator(
        task_id = 'transform',
        conn_id = "spark_default",
        application ="/opt/airflow/jobs/spark_mart_part.py",
        name = "spark_job",
        verbose = True,
        env_vars={
             "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-arm64",
         },
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:8020",
            "spark.sql.warehouse.dir": "hdfs://namenode:8020/user/hive/warehouse",
            "spark.driver.extraJavaOptions": "-Duser.home=/tmp",
            "spark.executor.extraJavaOptions": "-Duser.home=/tmp",
            },
    )
    load_mart = ClickHouseOperator(
        task_id = 'load_mart',
        clickhouse_conn_id = 'clickhouse_default',
        sql=load_mart()
    )
    check_result = PythonOperator(
        task_id = 'check_result',
        python_callable=check_result
    )


extract_api >> load_raw >> transform >> load_mart >> check_result