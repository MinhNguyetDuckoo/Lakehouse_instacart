from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, count

# =========================
# 1. INIT SPARK
# =========================
spark = SparkSession.builder \
    .appName("ALSRecommendation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================
# 2. LOAD DATA (Silver)
# =========================
df = spark.read.format("parquet").load("data/silver")

# =========================
# 3. CREATE RATING (FIX QUAN TRỌNG)
# =========================
# dùng số lần mua làm rating
df = df.groupBy("user_id", "product_id") \
       .agg(count("*").alias("rating"))

# convert type cho chắc
df = df.withColumn("rating", col("rating").cast("float"))

# drop null (an toàn)
df = df.dropna()

print("Sample data:")
df.show(5)

# =========================
# 4. SPLIT DATA
# =========================
(train, test) = df.randomSplit([0.8, 0.2], seed=42)

# =========================
# 5. TRAIN ALS MODEL
# =========================
als = ALS(
    userCol="user_id",
    itemCol="product_id",
    ratingCol="rating",
    rank=10,
    maxIter=10,
    regParam=0.1,
    coldStartStrategy="drop"
)

model = als.fit(train)

# =========================
# 6. PREDICT
# =========================
predictions = model.transform(test)

print("\nPrediction sample:")
predictions.show(10)

# =========================
# 7. GENERATE RECOMMENDATION
# =========================
user_recs = model.recommendForAllUsers(5)

print("\nRecommendation sample:")
user_recs.show(5, truncate=False)

# =========================
# 8. SAVE MODEL
# =========================
import os

os.makedirs("models", exist_ok=True)

model.write().overwrite().save("models/als_model")

print("\nModel trained & saved successfully!")