# Домашнее задание 04: Airflow

Нужно собрать простой Airflow-пайплайн для файла `orders.json`: загрузить заказы в Postgres, затем запустить Spark-задачу и сохранить агрегаты в Hive.

Исходный файл:

```text
/materials/seminar_02_raw/data/orders.json
```

Рабочие файлы нужно создать здесь:

```text
airflow/dags/orders_pipeline.py
airflow/jobs/orders_to_hive.py
airflow/jobs/orders.json
```

Файл `orders.json` нужно скопировать из материалов в `airflow/jobs/orders.json`. Внутри контейнера Airflow он будет доступен как:

```text
/opt/airflow/jobs/orders.json
```

## 1. DAG `orders_pipeline`

Создайте DAG:

```text
dag_id="orders_pipeline"
schedule=None
catchup=False
```

В DAG должны быть task-и в таком порядке:

```text
check_input_file >> load_orders_raw >> run_spark_to_hive >> print_check_queries
```

## 2. Проверить входной файл

Task `check_input_file` должен проверить, что файл `/opt/airflow/jobs/orders.json` существует и не пустой.

Если файла нет или он пустой, task должен упасть с понятной ошибкой.

## 3. Загрузить raw-заказы в Postgres

Task `load_orders_raw` должен прочитать `orders.json` через `pandas` и загрузить данные в Postgres через `psycopg2` или `psycopg`.

Подключение к Postgres DWH внутри Airflow:

```text
host=postgres-dwh
port=5432
database=dwh
user=admin
password=admin
```

Нужно создать схему и таблицу:

```sql
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
);
```

Что загрузить:

- `order_id`, `user_id`, `city`, `created_at`, `status` взять из заказа;
- `payment_id`, `payment_status`, `payment_amount`, `payment_currency` взять из объекта `payment`;
- `items_count` посчитать как длину массива `items`;
- `raw_payload` сохранить как исходный JSON заказа.

Загрузка должна быть идемпотентной: при повторном запуске DAG строки должны обновляться через `ON CONFLICT (order_id) DO UPDATE`.

## 4. Spark job `orders_to_hive.py`

Создайте Spark job, который читает `/opt/airflow/jobs/orders.json`, считает агрегаты и записывает их в Hive.

Spark job должен создать Hive database:

```sql
CREATE DATABASE IF NOT EXISTS ods;
```

Нужно создать таблицы:

```text
ods.order_city_stats
ods.order_status_stats
ods.category_revenue
```

### `ods.order_city_stats`

Колонки:

```text
city
orders_count
users_count
total_payment_amount
avg_payment_amount
```

### `ods.order_status_stats`

Колонки:

```text
status
orders_count
users_count
total_payment_amount
```

### `ods.category_revenue`

Разверните массив `items` и посчитайте выручку по категориям.

Колонки:

```text
category
items_count
orders_count
total_quantity
total_item_amount
```

Категорию можно взять как первый элемент `category_path`. Если категории нет, используйте `unknown`.

Таблицы должны перезаписываться при повторном запуске.

## 5. Запустить Spark из Airflow

Task `run_spark_to_hive` должен запускать Spark job через `SparkSubmitOperator`:

```text
/opt/airflow/jobs/orders_to_hive.py
```

Connection:

```text
spark_default
```

Минимальный `conf`:

```python
conf={
    "spark.hadoop.fs.defaultFS": "hdfs://namenode:8020",
    "spark.sql.warehouse.dir": "hdfs://namenode:8020/user/hive/warehouse",
    "spark.driver.extraJavaOptions": "-Duser.home=/tmp",
    "spark.executor.extraJavaOptions": "-Duser.home=/tmp",
}
```

## 6. Вывести запросы для проверки

Task `print_check_queries` должен вывести в лог SQL-запросы для проверки результата.

Проверки также лежат в файле:

```text
materials/homework_04/checks.sql
```

## Что приложить в ответ

Приложите:

1. код `airflow/dags/orders_pipeline.py`;
2. код `airflow/jobs/orders_to_hive.py`;
3. скриншот успешного запуска DAG;
4. результат проверки `stg.orders_raw`;
5. результат проверки таблиц в `ods`.

## Подсказки

Запуск Airflow:

```bash
make airflow
```

Airflow UI:

```text
http://localhost:8088
admin / admin
```

HiveServer2:

```text
jdbc:hive2://localhost:10000/default
```
