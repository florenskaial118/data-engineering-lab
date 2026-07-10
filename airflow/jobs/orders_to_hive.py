from pyspark.sql import SparkSession


spark = (
        SparkSession.builder.appName("orders_to_hive_sql")
        .config("spark.sql.shuffle.partitions", "2")
        .config("hive.metastore.uris", "thrift://hive-metastore:9083")
        .enableHiveSupport()
        .getOrCreate()
    )
spark.sparkContext.setLogLevel("WARN")

df_orders = spark.read \
    .format("json") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .load("hdfs://namenode:8020/warehouse/orders.json")

df_orders.createOrReplaceTempView("orders_temp")
df_orders.printSchema()
df_orders.show(2, truncate=False)

spark.sql("CREATE DATABASE IF NOT EXISTS ods")

spark.sql("DROP TABLE IF EXISTS ods.order_city_stats")
spark.sql("""
    CREATE TABLE ods.order_city_stats AS
    SELECT 
        customer.city,
        COUNT(order_id) as orders_count,
        COUNT(DISTINCT customer.user_id) as users_count,
        SUM(payment.amount) as total_payment_amount,
        AVG(payment.amount) as avg_payment_amount
    FROM orders_temp
    GROUP BY customer.city
""")

# 2. order_status_stats
spark.sql("DROP TABLE IF EXISTS ods.order_status_stats")
spark.sql("""
    CREATE TABLE ods.order_status_stats AS
    SELECT 
        status,
        COUNT(order_id) as orders_count,
        COUNT(DISTINCT customer.user_id) as users_count,
        SUM(payment.amount) as total_payment_amount
    FROM orders_temp
    GROUP BY status
""")

# 3. category_revenue
spark.sql("DROP TABLE IF EXISTS ods.category_revenue")
spark.sql("""
    CREATE TABLE ods.category_revenue AS
    SELECT
        COALESCE(item.category_path[0], 'unknown') as category,
        COUNT(*) as items_count,
        COUNT(DISTINCT order_id) as orders_count,
        SUM(item.quantity) as total_quantity,
        SUM(item.quantity * item.unit_price) as total_item_amount
    FROM orders_temp
    LATERAL VIEW EXPLODE(items) AS item
    GROUP BY COALESCE(item.category_path[0], 'unknown')
""")

spark.stop()
