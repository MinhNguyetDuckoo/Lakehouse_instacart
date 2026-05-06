from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Debug-Silver") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ) \
    .getOrCreate()

df = spark.read.format("delta") \
    .load("data/silver/orders")

df.show()