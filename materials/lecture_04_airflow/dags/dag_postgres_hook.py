from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "dwh_postgres"
RAW_SCHEMA = "raw"

def create_raw_layer() -> None:
    hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
    hook.run(f"""
    CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};
    create table if not exists {RAW_SCHEMA}.events (
    event_id integer primary key,
    user_id integer not null,
    event_type text not null,
    event_date date not null,
    amount numeric(10, 2) not null
    );

    truncate table {RAW_SCHEMA}.events;

    insert into {RAW_SCHEMA}.events
        (event_id, user_id, event_type, event_date, amount)
    values
        (1, 101, 'view',     date '2026-01-01', 0.00),
        (2, 101, 'cart',     date '2026-01-01', 0.00),
        (3, 101, 'purchase', date '2026-01-01', 1200.00),
        (4, 102, 'view',     date '2026-01-01', 0.00),
        (5, 102, 'purchase', date '2026-01-02', 850.00),
        (6, 103, 'view',     date '2026-01-02', 0.00),
        (7, 104, 'purchase', date '2026-01-02', 2100.00);
    """)

with DAG(
    dag_id = "dag_postgres_operator",
    description = "Пример PostgresOperator",
    start_date = datetime(2026,1,1),
    schedule = None,
    catchup = False,
    tags = ["postgres"]
)  as dag:
    create_raw = PythonOperator(
        task_id = "create_raw_layer",
        python_callable = create_raw_layer
    )
