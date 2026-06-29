from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator

def calculate_order_total(**context) -> dict[str, int | str]:
    orders = [1200, 850, 430, 2100]
    total = sum(orders)
    print(f"DAG Id: {context['dag'].dag_id}")
    print(f"Task Id: {context['task'].task_id}")
    print(f"Total: {total}")
    print(f"Orders: {orders}")

    return {"order_count": len(orders), "total": total, "currency": "RUB"}

with DAG(
    dag_id = "dag_python_operator",
    description = "Пример PythonOperator",
    start_date = datetime(2026,1,1),
    schedule = None,
    catchup = False,
    tags = ["python"]
)  as dag:

    calculate = PythonOperator(
        task_id = "calculate_orders_total",
        python_callable = calculate_order_total
    )