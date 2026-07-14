.PHONY: help up down postgres psql clickhouse greenplum kafka check-kafka-spark check-clickhouse-s3 check-greenplum-s3 check-integrations hive hive-check jupyter airflow seminar logs

help:
	@printf "Targets: up, down, postgres, psql, clickhouse, greenplum, kafka, check-kafka-spark, check-clickhouse-s3, check-greenplum-s3, check-integrations, hive, hive-check, jupyter, airflow, seminar, logs\n"

up:
	docker compose up -d

down:
	docker compose --profile s3 --profile clickhouse --profile greenplum --profile kafka --profile hive --profile spark --profile airflow down

postgres:
	docker compose up -d postgres-dwh
	@until docker compose exec postgres-dwh pg_isready -U admin -d dwh >/dev/null 2>&1; do sleep 1; done
	docker compose exec postgres-dwh psql -U admin -d dwh -f /docker-entrypoint-initdb.d/01_lecture_01.sql

psql:
	docker compose exec postgres-dwh psql -U admin -d dwh

hive:
	docker compose --profile hive up -d namenode datanode hdfs-init hive-metastore-postgresql hive-metastore hive-server
	@until docker compose exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -ls / >/dev/null 2>&1; do sleep 2; done
	docker compose exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /tmp /user/spark /warehouse
	docker compose exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -chown -R spark:supergroup /user/spark /warehouse
	docker compose exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -chmod 777 /tmp /user/spark
	docker compose exec namenode /opt/hadoop-3.2.1/bin/hdfs dfs -chmod 1777 /warehouse
	@printf "HDFS UI: http://localhost:9870\n"
	@printf "HiveServer2: jdbc:hive2://localhost:10000\n"

hive-check:
	docker compose exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "CREATE DATABASE IF NOT EXISTS lab_check; SHOW DATABASES;"

clickhouse:
	docker compose --profile s3 --profile clickhouse up -d minio minio-init clickhouse
	@until docker compose exec clickhouse clickhouse-client --user admin --password admin --query "SELECT 1" >/dev/null 2>&1; do sleep 1; done
	@printf "ClickHouse HTTP: http://localhost:8123\n"
	@printf "ClickHouse native: localhost:9002\n"
	@printf "MinIO API: http://localhost:9000\n"
	@printf "MinIO Console: http://localhost:9001\n"

greenplum:
	docker compose --profile s3 --profile greenplum up -d minio minio-init greenplum
	@until docker compose exec greenplum bash -lc "source /opt/greenplum-db-6.8.1/greenplum_path.sh && pg_isready -U gpadmin -d postgres" >/dev/null 2>&1; do sleep 2; done
	@printf "Greenplum: localhost:5434, database postgres, user gpadmin\n"
	@printf "MinIO API: http://localhost:9000\n"
	@printf "MinIO Console: http://localhost:9001\n"

kafka:
	docker compose --profile kafka up -d kafka
	@until docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 2; done
	@printf "Kafka bootstrap server: localhost:9092\n"

check-kafka-spark:
	docker compose --profile kafka --profile spark up -d --build kafka spark-master spark-worker
	@until docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 2; done
	@until docker compose exec spark-master bash -lc "timeout 1 bash -c '</dev/tcp/kafka/29092'" >/dev/null 2>&1; do sleep 2; done
	docker compose exec kafka bash -lc "timeout 1 bash -c '</dev/tcp/spark-master/7077'"
	docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic spark_check --partitions 1 --replication-factor 1
	docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic spark_check_out --partitions 1 --replication-factor 1
	docker compose exec spark-master bash -lc "timeout 1 bash -c '</dev/tcp/spark-master/7077'"
	docker compose exec spark-master python /app/scripts/checks/spark_kafka_check.py
	docker compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic spark_check_out --from-beginning --max-messages 1 --timeout-ms 10000
	@printf "Kafka <-> Spark check passed\n"

check-clickhouse-s3:
	docker compose --profile s3 --profile clickhouse up -d minio minio-init clickhouse
	@until docker compose exec clickhouse clickhouse-client --user admin --password admin --query "SELECT 1" >/dev/null 2>&1; do sleep 1; done
	docker compose --profile s3 run --rm --entrypoint /bin/sh minio-init -c "mc alias set local http://minio:9000 admin password123 && printf 'id,name\n1,alice\n' >/tmp/clickhouse_s3_check.csv && mc cp /tmp/clickhouse_s3_check.csv local/datalake/clickhouse/check.csv"
	docker compose exec clickhouse clickhouse-client --user admin --password admin --query "SELECT count() FROM s3('http://minio:9000/datalake/clickhouse/check.csv', 'admin', 'password123', 'CSVWithNames', 'id UInt32, name String')"
	docker compose exec clickhouse clickhouse-client --user admin --password admin --query "INSERT INTO FUNCTION s3('http://minio:9000/datalake/clickhouse/out.csv', 'admin', 'password123', 'CSV', 'id UInt32, name String') SELECT 2, 'bob'"
	docker compose --profile s3 run --rm --entrypoint /bin/sh minio-init -c "mc alias set local http://minio:9000 admin password123 && mc stat local/datalake/clickhouse/out.csv"
	@printf "ClickHouse <-> MinIO S3 check passed\n"

check-greenplum-s3:
	docker compose --profile s3 --profile greenplum up -d minio minio-init greenplum
	@until docker compose exec greenplum bash -lc "source /opt/greenplum-db-6.8.1/greenplum_path.sh && pg_isready -U gpadmin -d postgres" >/dev/null 2>&1; do sleep 2; done
	docker compose --profile s3 run --rm --entrypoint /bin/sh minio-init -c "mc alias set local http://minio:9000 admin password123 && printf 'id,name\n1,greenplum\n' >/tmp/greenplum_s3_check.csv && mc cp /tmp/greenplum_s3_check.csv local/datalake/greenplum/check.csv && mc anonymous set download local/datalake"
	docker compose exec greenplum bash -lc "timeout 3 bash -c '</dev/tcp/minio/9000'"
	docker compose exec greenplum psql -U gpadmin -d postgres -c "SELECT version();"
	docker compose exec greenplum bash -lc "if command -v curl >/dev/null 2>&1; then psql -U gpadmin -d postgres -c \"DROP TABLE IF EXISTS minio_http_check; CREATE TABLE minio_http_check (id int, name text); COPY minio_http_check FROM PROGRAM 'curl -fsS http://minio:9000/datalake/greenplum/check.csv' WITH (FORMAT csv, HEADER true); SELECT count(*) FROM minio_http_check;\"; elif command -v wget >/dev/null 2>&1; then psql -U gpadmin -d postgres -c \"DROP TABLE IF EXISTS minio_http_check; CREATE TABLE minio_http_check (id int, name text); COPY minio_http_check FROM PROGRAM 'wget -qO- http://minio:9000/datalake/greenplum/check.csv' WITH (FORMAT csv, HEADER true); SELECT count(*) FROM minio_http_check;\"; else printf 'Greenplum reaches MinIO, but curl/wget is absent for SQL HTTP smoke test\n'; fi"
	docker compose exec greenplum bash -lc "if command -v gpcheckcloud >/dev/null 2>&1 || command -v pxf >/dev/null 2>&1; then printf 'Greenplum native S3 tooling is present\n'; else printf 'Greenplum native S3 tooling is absent in this image; MinIO network/HTTP access was checked above\n'; fi"

check-integrations: check-kafka-spark check-clickhouse-s3 check-greenplum-s3

jupyter:
	docker compose --profile spark --profile s3 up -d --build jupyter
	docker compose exec -u root jupyter chmod 777 /spark-local
	@test "$$(docker compose ps --status running --services jupyter)" = "jupyter"
	@printf "JupyterLab: http://localhost:8888\n"
	@printf "Spark UI: http://localhost:4040 while SparkSession is running\n"

airflow:
	docker compose --profile airflow --profile s3 --profile clickhouse --profile greenplum up -d --build postgres-dwh minio minio-init clickhouse greenplum namenode datanode hdfs-init spark-master spark-worker postgres-airflow airflow-init airflow-webserver airflow-scheduler
	@printf "Airflow UI: http://localhost:8088\n"
	@printf "Login: admin / admin\n"
	@printf "Spark UI: http://localhost:8080\n"
	@printf "HDFS UI: http://localhost:9870\n"
	@printf "ClickHouse HTTP: http://localhost:8123\n"
	@printf "Greenplum: localhost:5434, database postgres, user gpadmin\n"

seminar: jupyter

logs:
	docker compose --profile s3 --profile clickhouse --profile greenplum --profile kafka --profile hive --profile spark --profile airflow logs -f jupyter spark-master spark-worker namenode datanode hive-metastore hive-server airflow-webserver airflow-scheduler clickhouse greenplum kafka minio
