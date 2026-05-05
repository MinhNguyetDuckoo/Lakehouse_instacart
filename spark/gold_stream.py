from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

spark = SparkSession.builder \
    .appName("GoldLayer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load schema
static_df = spark.read.format("parquet").load("data/silver")
schema = static_df.schema

# Read stream
df = spark.readStream \
    .format("parquet") \
    .schema(schema) \
    .load("data/silver")


# =========================
# FUNCTION XỬ LÝ BATCH
# =========================
def process_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id}")

    # USER
    user_agg = batch_df.groupBy("user_id") \
        .agg(
            count("order_id").alias("total_items"),
            avg("cart_position").alias("avg_cart_position")
        )

    user_agg.write.mode("append").parquet("data/gold/user")

    # PRODUCT
    product_agg = batch_df.groupBy("product_id") \
        .agg(
            count("*").alias("total_sales"),
            avg("is_reorder").alias("reorder_rate")
        )

    product_agg.write.mode("append").parquet("data/gold/product")

    # TIME
    time_agg = batch_df.groupBy("order_hour") \
        .agg(
            count("*").alias("num_orders")
        )

    time_agg.write.mode("append").parquet("data/gold/time")


# =========================
# START STREAM
# =========================
query = df.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "checkpoint/gold") \
    .start()

query.awaitTermination()