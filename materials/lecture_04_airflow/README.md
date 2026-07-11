# Lecture 04: Airflow

Примеры DAG-ов лежат в `materials/lecture_04_airflow/dags`. Это справочная копия для лекции и студентов.

Рабочая директория Airflow в проекте: `airflow/`.

Активные DAG-и, которые видит Airflow UI, лежат в `airflow/dags` и монтируются как `/opt/airflow/dags`.

Активные Spark jobs лежат в `airflow/jobs` и монтируются как `/opt/airflow/jobs`.

Spark job для примера `SparkSubmitOperator` также сохранен в `materials/lecture_04_airflow/jobs` как справочная копия.

## Порядок показа

1. `lecture_04_00_minimal_dag` - самый простой DAG с одним `EmptyOperator`.
2. `lecture_04_01_bash_operator` - запуск shell-команды через `BashOperator`.
3. `lecture_04_02_python_operator` - запуск Python-функции через `PythonOperator`.
4. `lecture_04_03_xcom` - передача данных между task-ами через XCom.
5. `lecture_04_04_postgres_hook` - работа с Postgres DWH через `PostgresHook`.
6. `lecture_04_05_spark_submit` - запуск PySpark job через `SparkSubmitOperator`.

## PostgresHook

DAG `lecture_04_04_postgres_hook` использует connection `dwh_postgres`.

Connection уже задан в `docker-compose.yml` через переменную:

```bash
AIRFLOW_CONN_DWH_POSTGRES=postgresql://admin:admin@postgres-dwh:5432/dwh
```

DAG создает в базе `dwh` схему `lecture_04_airflow` и две таблицы:

```sql
lecture_04_airflow.raw_events
lecture_04_airflow.event_stats
```

После запуска можно проверить результат так:

```bash
docker compose exec postgres-dwh psql -U admin -d dwh
```

```sql
select * from lecture_04_airflow.raw_events order by event_id;
select * from lecture_04_airflow.event_stats order by event_date, event_type;
```

## SparkSubmitOperator

DAG `lecture_04_05_spark_submit` запускает файл:

```bash
/opt/airflow/jobs/simple_spark_job.py
```

Результат пишется в HDFS:

```bash
hdfs://namenode:8020/tmp/airflow_lecture/spark_output
```

Проверить результат можно через HDFS UI: `http://localhost:9870`.

## Запуск Airflow

```bash
make airflow
docker compose up -d airflow-webserver
```

Airflow UI: `http://localhost:8088`

Логин и пароль: `admin / admin`

Spark UI: `http://localhost:8080`

Postgres DWH: `localhost:5432`, база `dwh`, пользователь `admin`, пароль `admin`
