from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id = "minimal_dag",
    description = "Самый простой даг",
    start_date = datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    tags = ["airflow", "minimal"]
) as dag:
    start = EmptyOperator(task_id = "start")