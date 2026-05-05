from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

# 1. Tạo Spark session
spark = SparkSession.builder \
    .appName("StreamingLakehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Schema (phải giống generator)
schema = StructType([
    StructField("event_type", StringType()),
    StructField("user_id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("add_to_cart_order", IntegerType()),
    StructField("reordered", IntegerType()),
    StructField("order_hour", IntegerType()),
    StructField("timestamp", LongType())
])

# 3. Đọc Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Convert JSON
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 5. Debug (in ra console)
query_console = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

# 6. Ghi vào Bronze (Parquet)
query_file = parsed_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", r"D:\Hoc_hanh\BigData\lakehourse\data\bronze") \
    .option("checkpointLocation", r"checkpoint\bronze") \
    .start()

# 7. giữ job chạy
spark.streams.awaitAnyTermination()