#!/usr/bin/env python3

#прочитайте raw JSON;
#разберите вложенные поля;
#приведите типы данных;
#уберите очевидные дубли;
#обработайте пропуски там, где это необходимо;
#сформируйте минимум две таблицы в `stg` или `ods`.

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


date = datetime.now().strftime('%Y-%m-%d')

spark = SparkSession.builder\
                    .appName("final_project")\
                    .master("spark://spark-master:7077") \
                    .getOrCreate()

HDFS_RAW_POSTS_DIR = f"/warehouse/raw/JSONPlaceholder/posts/dt={date}"
HDFS_RAW_COMMENTS_DIR = f"/warehouse/raw/JSONPlaceholder/comments/dt={date}"
HDFS_RAW_USERS_DIR = f"/warehouse/raw/JSONPlaceholder/users/dt={date}"

posts = spark.read.option("multiLine", "true").json(f"{HDFS_RAW_POSTS_DIR}/posts.json")


comments = spark.read.option("multiLine", "true").json(f"{HDFS_RAW_COMMENTS_DIR}/comments.json")


users = spark.read.option("multiLine", "true").json(f"{HDFS_RAW_USERS_DIR}/users.json")


users_flattened = users.select(
    col('address.city').alias('address_city'),
    col('address.geo.lat').alias('address_geo_lat'),
    col('address.geo.lng').alias('address_geo_lng'),
    col('address.street').alias('address_street'),
    col('address.suite').alias('address_suite'),
    col('address.zipcode').alias('address_zipcode'),
    col('company.bs').alias('company_bs'),
    col('company.catchPhrase').alias('company_catchPhrase'),
    col('company.name').alias('company_name'),
    col('email'),
    col('id'),
    col('name'),
    col('phone'),
    col('username'),
    col('website')
)

posts = (posts
    .withColumn('id', col('id').cast('int'))
    .withColumn('userId', col('userId').cast('int'))
        )

comments = (comments
    .withColumn('id', col('id').cast('int'))
    .withColumn('postId', col('postId').cast('int'))
        )

users_flattened = (users_flattened
    .withColumn('address_geo_lat', col('address_geo_lat').cast('Double'))
    .withColumn('address_geo_lng', col('address_geo_lng').cast('Double'))
    .withColumn('id', col('id').cast('int'))
        )

posts.dropDuplicates(['id','userId'])
comments.dropDuplicates(['id','postId'])
users.dropDuplicates(['id'])

clean_posts = posts.dropna()
clean_comments = comments.dropna()
clean_users = users_flattened.dropna()

HDFS_ODS_POSTS_DIR = f"/warehouse/ods/JSONPlaceholder_posts"
HDFS_ODS_COMMENTS_DIR = f"/warehouse/ods/JSONPlaceholder_comments"
HDFS_ODS_USERS_DIR = f"/warehouse/ods/JSONPlaceholder_users"

clean_posts.write.mode("overwrite").parquet(f"{HDFS_ODS_POSTS_DIR}/posts")
clean_comments.write.mode("overwrite").parquet(f"{HDFS_ODS_COMMENTS_DIR}/comments")
clean_users.write.mode("overwrite").parquet(f"{HDFS_ODS_USERS_DIR}/users")