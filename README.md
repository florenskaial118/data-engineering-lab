## PostgreSQL

Для практики используется PostgreSQL в Docker-контейнере.

### Запуск PostgreSQL

```bash
make postgres
```
### Подключиться к консоли 

```bash
make psql
```

### Остановить Posgtres

```bash
make down
```

## HDFS и Hive

Для практики доступен небольшой Docker-кластер: HDFS NameNode, HDFS DataNode, Hive Metastore, HiveServer2 и внутренняя PostgreSQL-база для metastore. Общий Hive/Spark warehouse находится в HDFS по пути `/warehouse`.

### Запуск Hive-кластера

```bash
make hive
```

### Проверка Hive-кластера

```bash
make hive-check
```

Параметры подключения:

- HDFS NameNode UI: `http://localhost:9870`
- HDFS DataNode UI: `http://localhost:9864`
- HDFS внутри Docker: `hdfs://namenode:8020`
- HDFS с хоста: `hdfs://localhost:8020`
- Hive Metastore внутри Docker: `thrift://hive-metastore:9083`
- Hive Metastore с хоста: `thrift://localhost:9083`
- PostgreSQL-база Hive Metastore с хоста: `localhost:15432`, database `metastore`, user `hive`, password `hive`
- HiveServer2 с хоста: `jdbc:hive2://localhost:10000`

PostgreSQL для Hive Metastore опубликован на `15432`, поэтому не конфликтует с `postgres-dwh` на `5432`.

### Работа из Jupyter

Jupyter-контейнер получает настройки HDFS и Hive автоматически.

HDFS через WebHDFS:

```python
import os
from hdfs import InsecureClient

client = InsecureClient(os.environ["HDFS_WEBHDFS_URL"], user="spark")
client.makedirs("/warehouse/from_jupyter")
client.list("/warehouse")
```

Hive через PySpark

```python
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master(os.environ["SPARK_MASTER_URL"])
    .appName("jupyter-hive")
    .config("hive.metastore.uris", os.environ["HIVE_METASTORE_URI"])
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("SHOW DATABASES").show()
spark.range(3).write.mode("overwrite").parquet("hdfs://namenode:8020/warehouse/from_jupyter_spark")
```

Локальные файлы из окружения Jupyter доступны Spark через `file:///materials/...`.:

Относительные пути и пути без схемы Spark будет интерпретировать через HDFS, потому что `fs.defaultFS` настроен на `hdfs://namenode:8020`. Для локального чтения указывайте префикс `file://`.

## Jupyter

Запуск JupyterLab вместе с PostgreSQL, MinIO, HDFS, Hive и Spark:

```bash
make jupyter
```

Открыть: `http://localhost:8888`.

Логин, пароль и token не требуются.

Spark application UI доступен на `http://localhost:4040`, пока в notebook запущен `SparkSession`.

## ClickHouse + MinIO

Запуск ClickHouse вместе с MinIO/S3:

```bash
make clickhouse
```

Параметры подключения:

- ClickHouse HTTP: `http://localhost:8123`
- ClickHouse native: `localhost:9002`
- Database: `lab`
- User: `admin`
- Password: `admin`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- MinIO user/password: `admin` / `password123`
- S3 bucket: `datalake`

ClickHouse native port опубликован на `9002`, чтобы не конфликтовать с MinIO API на `9000`.

## Greenplum + MinIO

Запуск Greenplum вместе с MinIO/S3:

```bash
make greenplum
```

Параметры подключения:

- Greenplum: `localhost:5434`
- Database: `postgres`
- User: `gpadmin`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- S3 bucket: `datalake`

Greenplum опубликован на `5434`, чтобы не конфликтовать с основным PostgreSQL на `5432` и Airflow PostgreSQL на `5433`.

## Kafka

Запуск однонодовой Kafka в KRaft-режиме, без ZooKeeper:

```bash
make kafka
```

Параметры подключения:

- Bootstrap server с хоста: `localhost:9092`
- Bootstrap server внутри Docker: `kafka:29092`

Проверка доступности Kafka из Spark и Spark из Kafka:

```bash
make check-kafka-spark
```

Проверка создаёт Kafka topics `spark_check` и `spark_check_out`, пишет сообщение из Spark в Kafka, читает его обратно Spark Structured Streaming Kafka connector и проверяет чтение результата через Kafka CLI.

## Проверки S3

Проверка чтения и записи ClickHouse в MinIO/S3:

```bash
make check-clickhouse-s3
```

Проверка создаёт CSV-объект в `s3://datalake/clickhouse/check.csv`, читает его через ClickHouse `s3()` table function, затем пишет результат в `s3://datalake/clickhouse/out.csv`.

Проверка доступности MinIO/S3 из Greenplum:

```bash
make check-greenplum-s3
```

Проверка валидирует TCP-доступ Greenplum к `minio:9000`, читает тестовый объект MinIO через SQL `COPY FROM PROGRAM`, если в образе есть `curl` или `wget`, и отдельно сообщает, есть ли в образе native S3 tooling (`gpcheckcloud` или `pxf`).

Все интеграционные проверки одной командой:

```bash
make check-integrations
```

## Ресурсы

Тяжёлые сервисы вынесены в Docker Compose profiles. Поэтому `docker compose up -d` поднимает только базовый PostgreSQL, а Spark, Hive, Airflow, ClickHouse, Greenplum, Kafka и MinIO запускаются через соответствующие `make` targets.
