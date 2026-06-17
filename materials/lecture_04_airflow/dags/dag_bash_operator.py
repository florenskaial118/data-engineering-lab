from datetime import datetime 
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "dag_bash_operator",
    description = "Пример BashOperator",
    start_date = datetime(2026,1,1),
    schedule = None,
    catchup = False,
    tags = ["bash"]
)  as dag: 
    hello_from_bash = BashOperator(
        task_id = "hello_from_bash",
        bash_command = """
        set -e
        mkdir -p /tmp/airflow_lecture
        echo "Hello from BashOperator"
        echo "DAG: {{ dag.dag_id }}"
        echo "Task: {{ task.task_id }}"
        echo "Run id: {{ run_id }}"
        date
        echo "Date: {{ ds }}"
        echo "created by {{ task.task_id }} at $(date -Iseconds)" > /tmp/airflow_lecture/bash_operator.txt
        ls -l /tmp/airflow_lecture/bash_operator.txt
        """
    )