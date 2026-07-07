# Airflow

Создайте свой DAG в рабочей папке:

```text
airflow/dags/ecommerce_s3_clickhouse_greenplum.py
```

Скопируйте CSV-файлы в:

```text
airflow/jobs/ecommerce/orders.csv
airflow/jobs/ecommerce/users.csv
airflow/jobs/ecommerce/order_items.csv
```

DAG должен быть простым и последовательным: загрузка CSV в S3, выполнение SQL для ClickHouse, выполнение SQL для Greenplum, построение витрин.
