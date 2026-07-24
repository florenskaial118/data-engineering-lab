# Домашнее задание 05: S3, ClickHouse и Greenplum

В этом задании нужно построить небольшой ecommerce-пайплайн: загрузить CSV в S3/MinIO, прочитать данные в ClickHouse и Greenplum, сделать витрины и автоматизировать процесс через Airflow.

Данные лежат в папке:

```text
materials/homework_05_s3_clickhouse_greenplum/data
```

Файлы:

- `orders.csv` - заказы;
- `users.csv` - пользователи;
- `order_items.csv` - товары внутри заказов.

Используйте стандартные окружения стенда:

- MinIO bucket: `datalake`;
- ClickHouse: host `localhost`, port `8123`, user `admin`, password `admin`;
- Greenplum: host `localhost`, port `5434`, database `postgres`, user `gpadmin`.

## 1. Подготовка окружения

Поднимите сервисы:

```bash
make airflow
```

Airflow UI:

```text
http://localhost:8088
admin / admin
```

MinIO Console:

```text
http://localhost:9001
admin / password123
```

## 2. Raw-слой в S3

Загрузите файлы в MinIO в такие пути:

```text
s3://datalake/raw/ecommerce/orders/dt=2026-07-01/orders.csv
s3://datalake/raw/ecommerce/users/users.csv
s3://datalake/raw/ecommerce/order_items/dt=2026-07-01/order_items.csv
```

Можно загрузить через MinIO Console, `mc` или Python `boto3`.

Проверьте, что объекты появились в bucket `datalake`.

## 3. ClickHouse

SQL можно выполнять через DataGrip, DBeaver или `clickhouse-client`.

Создайте слои:

```sql
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS mart;
```

### 3.1. Прочитать CSV напрямую из S3

Напишите запросы, которые читают каждый файл через `s3()`.

Для доступа из Docker-сети используйте URL вида:

```text
http://minio:9000/datalake/raw/ecommerce/orders/dt=2026-07-01/orders.csv
```

Для доступа с хоста используйте:

```text
http://localhost:9000/datalake/raw/ecommerce/orders/dt=2026-07-01/orders.csv
```

### 3.2. Создать ODS-таблицы

Создайте таблицы:

```text
ods.ecommerce_orders
ods.ecommerce_users
ods.ecommerce_order_items
```

Рекомендуемые движки и ключи:

```sql
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, user_id, order_id)
```

Для `ods.ecommerce_order_items` используйте:

```sql
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, order_id, product_id)
```

Для `ods.ecommerce_users` выберите ключ, удобный для join по `user_id`.

### 3.3. Загрузить данные из S3

Загрузите данные в ODS-таблицы через `INSERT INTO ... SELECT FROM s3(...)`.

Проверьте количество строк в каждой таблице.

### 3.4. Сделать витрины

Создайте витрины:

```text
mart.ecommerce_city_revenue
mart.ecommerce_product_revenue
```

`mart.ecommerce_city_revenue` должна содержать:

```text
dt, city, orders_count, users_count, items_count, total_revenue
```

`mart.ecommerce_product_revenue` должна содержать:

```text
dt, product_id, product_name, items_count, total_quantity, total_revenue
```

Для проверки планов используйте:

```sql
EXPLAIN indexes = 1
SELECT ...
```

Кратко ответьте: почему для `orders` ключ `ORDER BY (dt, user_id, order_id)` лучше, чем `ORDER BY order_id`, если основные запросы фильтруют по дате и агрегируют по пользователю или городу?

потому что orders by определяет физический порядок данных на диске если сделать только ключ по заказу, агрегации по пользователю будут долго идти

## 4. Greenplum

В Greenplum данные из S3 нужно читать только через PXF.

Подключитесь к Greenplum:

```bash
docker compose exec -T -u gpadmin greenplum bash -lc \
  'source /opt/greenplum-db-6.8.1/greenplum_path.sh && psql -U gpadmin -d postgres'
```

Создайте слои:

```sql
CREATE EXTENSION IF NOT EXISTS pxf;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS mart;
```

Если при создании external table появляется ошибка `ERROR: protocol "pxf" does not exist`, значит extension `pxf` не подключена в текущей базе.

### 4.1. External tables через PXF

Создайте external tables:

```text
raw.ext_ecommerce_orders_pxf
raw.ext_ecommerce_users_pxf
raw.ext_ecommerce_order_items_pxf
```

Пример location для заказов:

```sql
LOCATION ('pxf://datalake/raw/ecommerce/orders/dt=2026-07-01/orders.csv?PROFILE=s3:text&SERVER=minio')
FORMAT 'CSV' (HEADER);
```

Аналогично создайте external tables для `users.csv` и `order_items.csv`.

### 4.2. STG-таблицы с distribution key

Создайте внутренние таблицы:

```text
stg.ecommerce_orders
stg.ecommerce_users
stg.ecommerce_order_items
```

Рекомендуемые distribution keys:

```text
stg.ecommerce_orders DISTRIBUTED BY (user_id)
stg.ecommerce_users DISTRIBUTED BY (user_id)
stg.ecommerce_order_items DISTRIBUTED BY (order_id)
```

Загрузите данные из external tables в STG.

### 4.3. ODS и оптимальность join

Создайте ODS-таблицы:

```text
ods.ecommerce_orders
ods.ecommerce_users
ods.ecommerce_order_items
```

Переложите туда данные из STG. Для `orders` и `users` сохраните одинаковый distribution key по `user_id`.

Создайте дополнительную таблицу для сравнения:

```text
stg.ecommerce_orders_bad_dist DISTRIBUTED BY (order_id)
```

Сравните планы:

```sql
EXPLAIN
SELECT u.city, count(*)
FROM ods.ecommerce_orders o
JOIN ods.ecommerce_users u ON o.user_id = u.user_id
GROUP BY u.city;
```

```sql
EXPLAIN
SELECT u.city, count(*)
FROM stg.ecommerce_orders_bad_dist o
JOIN ods.ecommerce_users u ON o.user_id = u.user_id
GROUP BY u.city;
```
    
Кратко ответьте: где появляется `Redistribute Motion` и почему?

в первом случае он появился только 1 раз перед group by, а во втором случае 2 раза - перед join (потому данные из orders_bad_dist перераспределяются по user_id) и перед group by

### 4.4. Витрина Greenplum

Создайте витрину:

```text
mart.ecommerce_city_revenue
```

Колонки:

```text
dt, city, orders_count, users_count, items_count, total_revenue
```

## 5. Airflow

Автоматизируйте пайплайн через Airflow. DAG нужно написать самостоятельно.

Рабочие файлы создайте здесь:

```text
airflow/dags/ecommerce_s3_clickhouse_greenplum.py
airflow/jobs/ecommerce/orders.csv
airflow/jobs/ecommerce/users.csv
airflow/jobs/ecommerce/order_items.csv
```

DAG:

```text
dag_id="ecommerce_s3_clickhouse_greenplum"
schedule=None
catchup=False
```

Сделайте простой последовательный DAG без дополнительных проверочных task-ов.

Минимальная цепочка task-ов:

```text
upload_raw_to_s3
>> create_clickhouse_layers
>> load_clickhouse_ods
>> build_clickhouse_marts
>> create_greenplum_layers
>> load_greenplum_from_pxf
>> build_greenplum_mart
```

Требования:

- S3 загрузка через `S3Hook`, `boto3` или другой понятный способ из DAG;
- ClickHouse SQL выполняется из DAG любым удобным способом;
- Greenplum SQL выполняется из DAG через `PostgresOperator`, `SQLExecuteQueryOperator`, `PostgresHook` или аналогичный инструмент;
- Greenplum должен читать raw-файлы только через PXF external tables;
- DAG должен быть линейным и понятным: несколько последовательных операторов без ветвлений и отдельных проверок.

Connections в Airflow уже добавлены в окружение:

```text
minio_s3
clickhouse_default
greenplum_default
```

## 6. Что приложить в ответ

Приложите:

1. SQL для ClickHouse;
2. SQL для Greenplum;
3. код DAG;
4. скриншот успешного запуска DAG;
5. короткий ответ про `ORDER BY` в ClickHouse;
6. короткий ответ про `DISTRIBUTED BY` и `Redistribute Motion` в Greenplum.
