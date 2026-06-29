from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator

def extract_orders() -> list[dict[str, int | str]]:
    orders = [
        {"order_id": 1, "status": "paid", "amount": 1200},
        {"order_id": 2, "status": "paid", "amount": 850},
        {"order_id": 3, "status": "cancelled", "amount": 430},
        {"order_id": 4, "status": "paid", "amount": 2100},
    ]
    print(f"Extracted {len(orders)} orders")
    return orders


def paid_revenue(ti) -> dict[str, int]:
    orders = ti.xcom_pull(task_ids="extract_orders")
    paid_orders = [order for order in orders if order["status"] == "paid"]
    revenue = sum(order["amount"] for order in paid_orders)

    result = {"paid_order_count": len(paid_orders), "paid_revenue": revenue}
    print(result)
    return result

with DAG(
    dag_id = "dag_xcom_operator",
    description = "Пример Xcom",
    start_date = datetime(2026,1,1),
    schedule = None,
    catchup = False,
    tags = ["xcom"]
)  as dag:
    extract = PythonOperator(task_id = "extract_orders", python_callable = extract_orders)
    calculate = PythonOperator(task_id = "calculate_revenue", python_callable = paid_revenue)

    extract >> calculate
