#!/usr/bin/env python3
#Создайте минимум одну витрину в слое `mart`.

#Витрина должна содержать:

#- join между таблицами или разбор вложенной структуры;
#- агрегацию;
#- понятные бизнес-поля


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col



spark = SparkSession.builder\
                    .appName("final_project")\
                    .master("spark://spark-master:7077") \
                    .getOrCreate()

HDFS_ODS_POSTS_DIR = f"/warehouse/ods/JSONPlaceholder_posts"
HDFS_ODS_COMMENTS_DIR = f"/warehouse/ods/JSONPlaceholder_comments"
HDFS_ODS_USERS_DIR = f"/warehouse/ods/JSONPlaceholder_users"
HDFS_MART_DIR = f"/warehouse/mart/JSONPlaceholder_UsersActivity"



posts = spark.read.parquet(f"{HDFS_ODS_POSTS_DIR}/posts").select(
    col('id').alias('postId'),
    col('userId')
)
comments = spark.read.parquet(f"{HDFS_ODS_COMMENTS_DIR}/comments").select(
    col('email').alias('commentEmail'),
    col('id').alias('commentId'),
    col('postId')
)
users = spark.read.parquet(f"{HDFS_ODS_USERS_DIR}/users").select(
    col('address_city'),
    col('company_name'),
    col('email').alias('userEmail'),
    col('id').alias('userId'),
    col('website')
)



posts_comments = posts.join(
    comments, on = 'postId')

posts_comments_stat = posts_comments.groupBy('userId').agg(
    F.countDistinct('postId').alias('post_cnt'),
    F.countDistinct('commentId').alias('got_comment_cnt')
)

users_activity = posts_comments_stat.join(
    users, on = 'userId', how = 'right').orderBy('userId')

users_activity.show()

users_activity.write.mode("overwrite").parquet(f"{HDFS_MART_DIR}/UsersActivity")

