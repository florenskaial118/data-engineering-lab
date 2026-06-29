from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id = "dag_spark_submit_operator",
    description = "Пример SparkOperator",
    start_date = datetime(2026,1,1),
    schedule = None,
    catchup = False,
    tags = ["spark"]
) as dag:
    run_spark_job = SparkSubmitOperator(
        task_id = "spark_job",
        conn_id = "spark_default",
        application ="/opt/airflow/jobs/simple_spark_job.py",
        name = "spark_job",
        verbose = True,
        conf={
            "spark.hadoop.fs.defaultFS": "hdfs://namenode:8020",
            "spark.driver.extraJavaOptions": "-Duser.home=/tmp",
            "spark.executor.extraJavaOptions": "-Duser.home=/tmp",
        },
    )
