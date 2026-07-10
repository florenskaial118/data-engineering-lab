from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import psycopg2
import pandas as pd
import json   
import requests



def load_orders_raw():
    with open("/opt/airflow/jobs/orders.json", "r", encoding="utf-8") as f:
        orders_data = json.load(f)

        rows_to_insert = [
            (
                order["order_id"],
                order["customer"]["user_id"],
                order["customer"]["city"],
                order["created_at"],
                order["status"],
                order["payment"]['payment_id'],
                order["payment"]['status'].lower(),
                float(order["payment"]['amount']),
                order["payment"]['currency'],
                len(order['items']),
                json.dumps(order)
            )
            for order in orders_data
        ]

        
        conn = psycopg2.connect(
            host="postgres-dwh",
            port=5432,
            database="dwh",
            user="admin",
            password="admin"
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS stg;

            CREATE TABLE IF NOT EXISTS stg.orders_raw (
                order_id BIGINT PRIMARY KEY,
                user_id BIGINT,
                city TEXT,
                created_at TIMESTAMPTZ,
                status TEXT,
                payment_id TEXT,
                payment_status TEXT,
                payment_amount NUMERIC(12, 2),
                payment_currency TEXT,
                items_count INT,
                raw_payload JSONB,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """)    
        cur.executemany(
                    """
                    INSERT INTO stg.orders_raw(
                        order_id,
                        user_id,
                        city,
                        created_at,
                        status,
                        payment_id,
                        payment_status,
                        payment_amount,
                        payment_currency,
                        items_count,
                        raw_payload
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict (order_id)
                    do update set
                        user_id = EXCLUDED.user_id,
                        city = EXCLUDED.city,
                        created_at = EXCLUDED.created_at,
                        payment_status = EXCLUDED.payment_status,
                        status = EXCLUDED.status,
                        payment_currency = EXCLUDED.payment_currency,
                        payment_amount = EXCLUDED.payment_amount,
                        items_count = EXCLUDED.items_count,
                        raw_payload = EXCLUDED.raw_payload
                    """,
                    rows_to_insert,
                )
        conn.commit()

def upload_to_hdfs():
    """Загружает файл в HDFS через WebHDFS API"""
    # Читаем файл
    with open('/opt/airflow/jobs/orders.json', 'rb') as f:
        data = f.read()
    
    # Создаём файл в HDFS (получаем редирект)
    resp = requests.put(
        'http://namenode:9870/webhdfs/v1/warehouse/orders.json?op=CREATE&overwrite=true',
        allow_redirects=False
    )
    
    # Загружаем по редиректу
    if resp.status_code == 307:
        requests.put(resp.headers['Location'], data=data)

with DAG (
    dag_id="orders_pipeline",
    schedule=None,
    catchup=False
) as dag:
    check_input = BashOperator(
        task_id = "check_input_file", 
        bash_command = """
        if [ -s /opt/airflow/jobs/orders.json ]; then
        echo "File exists and is not empty"
        else
        echo "Problem with file. Quitting" exit 1
        fi
        """)
    load_orders = PythonOperator(task_id = 'load_orders_raw', python_callable = load_orders_raw)
    load_orders_hdfs = PythonOperator(
        task_id="upload_to_hdfs",
        python_callable=upload_to_hdfs
    )
    run_spark_job = SparkSubmitOperator(
        task_id = "spark_job",
        conn_id = "spark_default",
        application ="/opt/airflow/jobs/orders_to_hive.py",
        name = "spark_job",
        verbose = True,
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:8020",
            "spark.sql.warehouse.dir": "hdfs://namenode:8020/user/hive/warehouse",
            "spark.driver.extraJavaOptions": "-Duser.home=/tmp",
            "spark.executor.extraJavaOptions": "-Duser.home=/tmp",
            },
    )
    print = BashOperator(
        task_id="print",
        bash_command="""
            cat "/opt/airflow/jobs/checks.sql"
            """
    )

check_input >> load_orders >> load_orders_hdfs >> run_spark_job >> print