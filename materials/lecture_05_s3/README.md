# Lecture 05: S3, MinIO и Yandex Object Storage в дата-платформе

Цель лекции - объяснить, что такое S3/Object Storage, зачем он нужен в data platform, чем отличается от HDFS и как с ним работают из Python, AWS CLI и Spark.

Основное демо теперь находится в notebook:

```text
materials/lecture_05_s3/notebooks/s3_boto3_aws_spark_demo.ipynb
```

## Что такое S3/Object Storage

S3/Object Storage - это объектное хранилище. Оно хранит не файлы в привычной файловой системе, а объекты. Каждый объект имеет key, содержимое и метаданные.

Базовые сущности:

- `bucket` - контейнер верхнего уровня, например `datalake`;
- `object` - конкретный объект, например CSV или Parquet-файл;
- `key` - полный путь объекта внутри bucket, например `raw/orders/dt=2026-06-23/orders.csv`;
- `prefix` - логическая группа ключей, например `raw/orders/`;
- `endpoint` - адрес S3-compatible API, например `http://minio:9000` или `https://storage.yandexcloud.net`;
- `access key` - идентификатор ключа доступа;
- `secret key` - секретная часть ключа доступа.

Важно: в S3 нет настоящих директорий. Запись `raw/orders/dt=2026-06-23/orders.csv` - это один key. UI просто показывает key как дерево по символу `/`.

## Зачем S3 нужен в data platform

S3 обычно используется как слой Data Lake. Он хранит сырые данные, очищенные данные и промежуточные результаты пайплайнов.

Типовая архитектура:

```text
Источники данных
     ↓
S3/Data Lake raw
     ↓
Spark / Airflow ETL
     ↓
S3/Data Lake ods в Parquet
     ↓
ClickHouse / Greenplum / BI / ML
```

Почему это удобно:

- storage отделен от compute;
- данные можно читать разными инструментами;
- Parquet/ORC хорошо подходят для аналитики;
- можно хранить историю дешево;
- один и тот же Data Lake может использовать Spark, ClickHouse, Greenplum, Trino, Airflow и ML-пайплайны.

## Почему в учебном проекте MinIO

MinIO - S3-compatible object storage, который удобно запускать локально в Docker.

В проекте MinIO нужен, чтобы показать production-подход без облачного аккаунта:

- API внутри Docker: `http://minio:9000`;
- API с хоста: `http://localhost:9000`;
- Console UI: `http://localhost:9001`;
- login/password: `admin` / `password123`;
- bucket: `datalake`.

Запуск стенда:

```bash
make jupyter
```

Jupyter UI:

```text
http://localhost:8888
```

MinIO UI:

```text
http://localhost:9001
```

## HDFS vs S3

HDFS - распределенная файловая система из Hadoop-экосистемы. Она ближе к обычной файловой системе: есть NameNode, DataNode, директории, блоки, replication factor.

S3 - объектное хранилище. В нем нет настоящих директорий и дешевого атомарного rename как в HDFS. Есть bucket и object key.

Сравнение:

| Критерий | HDFS | S3/Object Storage |
|---|---|---|
| Модель | Файловая система | Объектное API |
| Структура | Директории и файлы | Bucket, object, key, prefix |
| Compute/storage | Часто вместе в Hadoop-кластере | Обычно разделены |
| Rename | Дешевая файловая операция | Обычно copy + delete |
| Масштабирование | Управляется кластером HDFS | Управляется object storage сервисом |
| Типичный кейс | Hadoop on-prem, HDFS warehouse | Cloud/Data Lake, S3-compatible storage |

Главная идея: Spark может читать и HDFS, и S3, но физика операций разная. То, что дешево в HDFS, может быть дорого в S3.

## Data lake layout

Пример аккуратной структуры bucket:

```text
s3://datalake/
  raw/
    orders/dt=2026-06-23/orders.csv
  ods/
    orders/dt=2026-06-23/part-*.parquet
  marts/
    daily_revenue/dt=2026-06-23/part-*.parquet
  checkpoints/
    streaming/orders/
  staging/
    tmp/job_name/run_id=.../
```

Смысл слоев:

- `raw` - исходные данные как пришли из источника;
- `ods` - очищенные и типизированные данные;
- `marts` - витрины под аналитику;
- `checkpoints` - checkpoint-и jobs;
- `staging` - временные данные.

Не пишите все данные в корень bucket. Разделяйте данные по слоям, сущностям и датам.

## Notebook demo

Открыть notebook:

```text
materials/lecture_05_s3/notebooks/s3_boto3_aws_spark_demo.ipynb
```

Внутри показаны три способа работы с S3:

- `boto3` как Python SDK;
- AWS CLI через notebook-команды `!aws ...`, аналогично `!hdfs dfs ...` из HDFS-лекции;
- Spark через `s3a://`.

Notebook показывает основные методы `boto3`:

- `list_buckets()`;
- `create_bucket()`;
- `head_bucket()`;
- `put_object()`;
- `upload_file()`;
- `list_objects_v2()`;
- paginator для listing;
- `head_object()`;
- `get_object()`;
- `download_file()`;
- `copy_object()`;
- `put_object_tagging()`;
- `get_object_tagging()`;
- `generate_presigned_url()`;
- `delete_object()` / `delete_objects()`;
- обработка `ClientError`.

## AWS CLI в notebook

Команды показываются в стиле shell-команд Jupyter. В Docker-образ Jupyter/Spark добавлен `awscli`, поэтому после пересборки стенда команды работают как обычные `!aws ...`.

```python
!aws --version
!aws --endpoint-url $S3_ENDPOINT s3 ls
!aws --endpoint-url $S3_ENDPOINT s3 mb s3://$S3_BUCKET
!aws --endpoint-url $S3_ENDPOINT s3 cp $LOCAL_ORDERS s3://$S3_BUCKET/$RAW_KEY
!aws --endpoint-url $S3_ENDPOINT s3 ls s3://$S3_BUCKET/raw/orders/dt=2026-06-23/
```

Если `aws` не найден, значит Jupyter-контейнер запущен из старого образа. Пересоберите стенд:

```bash
make jupyter
```

## Spark и S3A

Spark работает с S3-compatible storage через Hadoop connector `s3a://`.

Пример путей:

```text
s3a://datalake/raw/orders/dt=2026-06-23/orders.csv
s3a://datalake/ods/orders/dt=2026-06-23/
```

Ключевые настройки:

- `spark.hadoop.fs.s3a.endpoint`;
- `spark.hadoop.fs.s3a.access.key`;
- `spark.hadoop.fs.s3a.secret.key`;
- `spark.hadoop.fs.s3a.path.style.access=true` для MinIO;
- `spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`;
- зависимости `hadoop-aws` и AWS SDK bundle.

Notebook читает CSV из `raw`, приводит типы, добавляет `loaded_at`, пишет Parquet в `ods` и читает результат обратно.

## Spark committers и почему S3 не HDFS

Это важный production-блок.

Когда Spark пишет результат, каждая task обычно пишет временные part-файлы. В HDFS классический commit часто опирается на rename: task записала временный файл, потом job commit быстро переименовал временный путь в финальный.

В S3/Object Storage rename не является дешевой атомарной операцией. Обычно это copy + delete. Поэтому старые file output commit protocols, которые нормально работают на HDFS, могут быть медленными или проблемными на S3.

Что нужно объяснить студентам:

- S3 - не файловая система, даже если путь выглядит как `s3a://bucket/prefix/file.parquet`;
- `mode("overwrite")` может удалить весь указанный prefix;
- много мелких файлов ухудшает и запись, и чтение;
- при сбоях важно понимать, какие временные файлы уже появились в prefix;
- для production используют S3A committers, cloud-optimized committers или table formats вроде Iceberg/Delta/Hudi;
- конкретные настройки зависят от версии Spark, Hadoop AWS connector и облачного провайдера.

Для учебного demo в notebook используется обычная запись Spark, потому что цель - показать базовую работу `s3a://`. Для production этого недостаточно: нужно отдельно проектировать commit protocol, layout и размер файлов.

## Переключение на Yandex Object Storage

Для Yandex Object Storage меняются endpoint, bucket и credentials:

```bash
S3_ENDPOINT=https://storage.yandexcloud.net
S3_ACCESS_KEY=<YANDEX_STATIC_KEY_ID>
S3_SECRET_KEY=<YANDEX_STATIC_SECRET>
S3_BUCKET=<YANDEX_BUCKET_NAME>
S3_REGION=ru-central1
S3_PATH_STYLE_ACCESS=true
```

Не храните реальные ключи в репозитории и не показывайте secret key на записи. После демо удалите тестовые объекты, bucket и static key, если они создавались только для занятия.

## Типовые ошибки

- `AccessDenied` - неверные credentials или недостаточно прав.
- `NoSuchBucket` - bucket не создан или указан не тот `S3_BUCKET`.
- `NoSuchKey` - key объекта указан неверно.
- `SignatureDoesNotMatch` - неверный secret, endpoint, region или способ подписи.
- `wrong endpoint` - из Docker нужен `http://minio:9000`, с хоста `http://localhost:9000`.
- `path-style access` - для MinIO обычно нужен path-style access.
- `ClassNotFoundException: S3AFileSystem` - Spark запущен без `hadoop-aws` зависимостей.
- `small files problem` - слишком много маленьких файлов в prefix.

## Чеклист перед записью

- `make jupyter` выполнен.
- Jupyter открывается на `http://localhost:8888`.
- MinIO UI открывается на `http://localhost:9001`.
- Bucket `datalake` создан.
- Notebook открывается и стартовые ячейки выполняются.
- `boto3` подключается к MinIO.
- AWS CLI-блок работает: `aws --version` выполняется в Jupyter-контейнере.
- SparkSession создается до любых других Spark-ячеек.
- Реальные секреты не попадают в notebook и запись экрана.
