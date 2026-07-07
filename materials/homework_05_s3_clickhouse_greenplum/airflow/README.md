# Airflow template

Скопируйте шаблон DAG в рабочую папку:

```text
airflow/dags/ecommerce_s3_clickhouse_greenplum.py
```

Скопируйте CSV-файлы в:

```text
airflow/jobs/ecommerce/orders.csv
airflow/jobs/ecommerce/users.csv
airflow/jobs/ecommerce/order_items.csv
```

Шаблон специально неполный: в нём отмечены места, куда нужно добавить SQL для ClickHouse и Greenplum.
