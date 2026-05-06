from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv

load_dotenv()

# =========================================================
# SPARK SESSION
# =========================================================

spark = SparkSession.builder \
    .appName("Lakehouse Silver Streaming") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =========================================================
# BASE PATH
# =========================================================

base_path = "data"

# =========================================================
# READ BRONZE STREAMS
# =========================================================

orders_bronze = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/bronze/orders")

order_items_bronze = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/bronze/order_items")

payments_bronze = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/bronze/payments")

reviews_bronze = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/bronze/reviews")

shipments_bronze = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/bronze/shipments")

returns_bronze = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/bronze/returns")

inventory_bronze = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/bronze/inventory")

traffic_bronze = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/bronze/traffic")

# =========================================================
# ORDERS SILVER
# =========================================================

orders_silver = orders_bronze \
    .withColumn(
        "order_id",
        col("order_id").cast("int")
    ) \
    .withColumn(
        "customer_id",
        col("customer_id").cast("int")
    ) \
    .withColumn(
        "zip",
        col("zip").cast("int")
    ) \
    .withColumn(
        "order_date",
        to_date(col("order_date"))
    ) \
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    ) \
    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    ) \
    .filter(
        col("order_id").isNotNull()
    ) \
    .dropDuplicates([
        "order_id",
        "event_timestamp"
    ]) \
    .withColumn(
        "processing_time",
        current_timestamp()
    ) \
    .withColumn(
        "silver_layer",
        lit("silver")
    )

# =========================================================
# ORDER ITEMS SILVER
# =========================================================

order_items_silver = order_items_bronze \
    .withColumn(
        "order_id",
        col("order_id").cast("int")
    ) \
    .withColumn(
        "product_id",
        col("product_id").cast("int")
    ) \
    .withColumn(
        "seller_id",
        col("seller_id").cast("int")
    ) \
    .withColumn(
        "price",
        col("price").cast("double")
    ) \
    .withColumn(
        "freight_value",
        col("freight_value").cast("double")
    ) \
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    ) \
    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    ) \
    .filter(
        col("order_id").isNotNull()
    ) \
    .filter(
        col("product_id").isNotNull()
    ) \
    .dropDuplicates([
        "order_id",
        "product_id",
        "event_timestamp"
    ]) \
    .withColumn(
        "item_total",
        col("price") + col("freight_value")
    ) \
    .withColumn(
        "silver_layer",
        lit("silver")
    )

# =========================================================
# PAYMENTS SILVER
# =========================================================

payments_silver = payments_bronze \
    .withColumn(
        "order_id",
        col("order_id").cast("int")
    ) \
    .withColumn(
        "payment_value",
        col("payment_value").cast("double")
    ) \
    .withColumn(
        "installments",
        col("installments").cast("int")
    ) \
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    ) \
    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    ) \
    .filter(
        col("payment_value") > 0
    ) \
    .dropDuplicates([
        "order_id",
        "event_timestamp"
    ]) \
    .withColumn(
        "payment_category",
        when(
            col("payment_value") >= 500,
            "HIGH_VALUE"
        ).otherwise("NORMAL")
    ) \
    .withColumn(
        "fraud_flag",
        when(
            col("payment_value") >= 2000,
            1
        ).otherwise(0)
    ) \
    .withColumn(
        "silver_layer",
        lit("silver")
    )

# =========================================================
# REVIEWS SILVER
# =========================================================

reviews_silver = reviews_bronze \
    .withColumn(
        "review_id",
        col("review_id").cast("string")
    ) \
    .withColumn(
        "order_id",
        col("order_id").cast("int")
    ) \
    .withColumn(
        "product_id",
        col("product_id").cast("int")
    ) \
    .withColumn(
        "customer_id",
        col("customer_id").cast("int")
    ) \
    .withColumn(
        "rating",
        col("rating").cast("int")
    ) \
    .withColumn(
        "review_date",
        to_date(col("review_date"))
    ) \
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    ) \
    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    ) \
    .filter(
        col("rating").between(1, 5)
    ) \
    .dropDuplicates([
        "review_id",
        "event_timestamp"
    ]) \
    .withColumn(
        "review_sentiment",
        when(
            col("rating") >= 4,
            "POSITIVE"
        ).when(
            col("rating") == 3,
            "NEUTRAL"
        ).otherwise(
            "NEGATIVE"
        )
    ) \
    .withColumn(
        "silver_layer",
        lit("silver")
    )

# =========================================================
# SHIPMENTS SILVER
# =========================================================

shipments_silver = shipments_bronze \
    .withColumn(
        "order_id",
        col("order_id").cast("int")
    ) \
    .withColumn(
        "shipping_fee",
        col("shipping_fee").cast("double")
    ) \
    .withColumn(
        "ship_date",
        to_date(col("ship_date"))
    ) \
    .withColumn(
        "delivery_date",
        to_date(col("delivery_date"))
    ) \
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    ) \
    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    ) \
    .dropDuplicates([
        "order_id",
        "event_timestamp"
    ]) \
    .withColumn(
        "delivery_days",
        datediff(
            col("delivery_date"),
            col("ship_date")
        )
    ) \
    .withColumn(
        "shipping_status",
        when(
            col("delivery_days") <= 2,
            "FAST"
        ).when(
            col("delivery_days") <= 5,
            "NORMAL"
        ).otherwise(
            "DELAYED"
        )
    ) \
    .withColumn(
        "silver_layer",
        lit("silver")
    )

# =========================================================
# RETURNS SILVER
# =========================================================

returns_silver = returns_bronze \
    .withColumn(
        "return_id",
        col("return_id").cast("string")
    ) \
    .withColumn(
        "order_id",
        col("order_id").cast("int")
    ) \
    .withColumn(
        "product_id",
        col("product_id").cast("int")
    ) \
    .withColumn(
        "return_date",
        to_date(col("return_date"))
    ) \
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    ) \
    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    ) \
    .dropDuplicates([
        "return_id",
        "event_timestamp"
    ]) \
    .withColumn(
        "return_flag",
        lit(1)
    ) \
    .withColumn(
        "silver_layer",
        lit("silver")
    )

# =========================================================
# INVENTORY SILVER
# =========================================================

inventory_silver = inventory_bronze \
    .withColumn(
        "product_id",
        col("product_id").cast("int")
    ) \
    .withColumn(
        "stock_on_hand",
        col("stock_on_hand").cast("int")
    ) \
    .withColumn(
        "units_received",
        col("units_received").cast("int")
    ) \
    .withColumn(
        "units_sold",
        col("units_sold").cast("int")
    ) \
    .withColumn(
        "snapshot_date",
        to_date(col("snapshot_date"))
    ) \
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    ) \
    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    ) \
    .dropDuplicates([
        "product_id",
        "event_timestamp"
    ]) \
    .withColumn(
        "inventory_status",
        when(
            col("stock_on_hand") <= 0,
            "STOCKOUT"
        ).when(
            col("stock_on_hand") <= 20,
            "LOW_STOCK"
        ).when(
            col("stock_on_hand") >= 500,
            "OVERSTOCK"
        ).otherwise(
            "NORMAL"
        )
    ) \
    .withColumn(
        "silver_layer",
        lit("silver")
    )

# =========================================================
# TRAFFIC SILVER
# =========================================================

traffic_silver = traffic_bronze \
    .withColumn(
        "sessions",
        col("sessions").cast("int")
    ) \
    .withColumn(
        "unique_visitors",
        col("unique_visitors").cast("int")
    ) \
    .withColumn(
        "page_views",
        col("page_views").cast("int")
    ) \
    .withColumn(
        "date",
        to_date(col("date"))
    ) \
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    ) \
    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    ) \
    .dropDuplicates([
        "date",
        "traffic_source",
        "event_timestamp"
    ]) \
    .withColumn(
        "pages_per_session",
        round(
            col("page_views") / col("sessions"),
            2
        )
    ) \
    .withColumn(
        "traffic_quality",
        when(
            col("sessions") >= 1000,
            "HIGH"
        ).otherwise(
            "NORMAL"
        )
    ) \
    .withColumn(
        "silver_layer",
        lit("silver")
    )

# =========================================================
# WRITE FUNCTION
# =========================================================

def write_silver_stream(df, table_name):

    return df.writeStream \
        .queryName(
            f"silver_{table_name}_stream"
        ) \
        .format("delta") \
        .outputMode("append") \
        .partitionBy("event_date") \
        .option(
            "checkpointLocation",
            f"{base_path}/silver/checkpoints/{table_name}"
        ) \
        .option(
            "mergeSchema",
            "true"
        ) \
        .trigger(
            processingTime="5 seconds"
        ) \
        .start(
            f"{base_path}/silver/{table_name}"
        )

# =========================================================
# START STREAMS
# =========================================================

queries = []

queries.append(write_silver_stream(orders_silver, "orders"))
queries.append(write_silver_stream(order_items_silver, "order_items"))
queries.append(write_silver_stream(payments_silver, "payments"))
queries.append(write_silver_stream(reviews_silver, "reviews"))
queries.append(write_silver_stream(shipments_silver, "shipments"))
queries.append(write_silver_stream(returns_silver, "returns"))
queries.append(write_silver_stream(inventory_silver, "inventory"))
queries.append(write_silver_stream(traffic_silver, "traffic"))


spark.streams.awaitAnyTermination()