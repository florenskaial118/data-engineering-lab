from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = (
        SparkSession.builder.appName("lecture-04-simple-spark-job")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    output_path = os.getenv(
        "SPARK_OUTPUT_PATH",
        "hdfs://namenode:8020/tmp/airflow_lecture/spark_output",
    )

    orders = spark.createDataFrame(
        [
            (1, "books", 1200.0),
            (2, "books", 850.0),
            (3, "electronics", 2100.0),
            (4, "electronics", 430.0),
            (5, "home", 990.0),
        ],
        ["order_id", "category", "amount"],
    )

    result = (
        orders.groupBy("category")
        .agg(
            F.count("*").alias("orders_count"),
            F.round(F.sum("amount"), 2).alias("revenue"),
        )
        .orderBy("category")
    )

    result.show(truncate=False)
    result.coalesce(1).write.mode("overwrite").parquet(output_path)
    print(f"Spark result written to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
