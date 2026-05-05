from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SilverLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 👉 lấy schema từ batch
static_df = spark.read.format("parquet").load("data/bronze")
schema = static_df.schema

# 👉 đọc stream với schema
df = spark.readStream \
    .format("parquet") \
    .schema(schema) \
    .load("data/bronze")

# CLEAN
clean_df = df \
    .filter(col("user_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .filter(col("order_id").isNotNull()) \
    .filter(col("event_type") == "order_item")

# FEATURE
feature_df = clean_df \
    .withColumn("is_reorder", col("reordered")) \
    .withColumn("cart_position", col("add_to_cart_order"))

# WRITE
query = feature_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "data/silver") \
    .option("checkpointLocation", "checkpoint/silver") \
    .start()

query.awaitTermination()