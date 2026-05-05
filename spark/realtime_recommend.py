# =========================
# FIX ENCODING WINDOWS
# =========================
import sys
sys.stdout.reconfigure(encoding='utf-8')

# =========================
# IMPORT
# =========================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, IntegerType
from pyspark.ml.recommendation import ALSModel

# =========================
# INIT SPARK
# =========================
spark = SparkSession.builder \
    .appName("RealtimeRecommendation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=== Spark started ===")

# =========================
# LOAD MODEL
# =========================
model_path = "models/als_model"

model = ALSModel.load(model_path)
print("Model loaded successfully!")

# =========================
# DEFINE SCHEMA
# =========================
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("product_id", IntegerType())

# =========================
# READ FROM KAFKA
# =========================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .load()

print("Reading stream from Kafka...")

# =========================
# PARSE JSON
# =========================
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# =========================
# REALTIME PREDICTION
# =========================
prediction_df = model.transform(parsed_df)

# =========================
# SELECT OUTPUT
# =========================
output_df = prediction_df.select(
    "user_id",
    "product_id",
    "prediction"
)

# =========================
# WRITE TO CONSOLE
# =========================
query = output_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

print("=== Streaming started ===")

query.awaitTermination()