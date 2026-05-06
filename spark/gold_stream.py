from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv

load_dotenv()

# Spark Session

spark = SparkSession.builder \
    .appName("Lakehouse Gold Streaming") \
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

# Base Path

base_path = "data"

# Read Silver Streams

orders_silver = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/silver/orders")

order_items_silver = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/silver/order_items")

payments_silver = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/silver/payments")

reviews_silver = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/silver/reviews")

shipments_silver = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/silver/shipments")

returns_silver = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/silver/returns")

inventory_silver = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/silver/inventory")

traffic_silver = spark.readStream \
    .format("delta") \
    .load(f"{base_path}/silver/traffic")

# Daily Sales KPI

daily_sales_gold = order_items_silver \
    .groupBy("event_date") \
    .agg(
        round(
            sum("price"),
            2
        ).alias("gross_revenue"),

        round(
            sum("freight_value"),
            2
        ).alias("shipping_revenue"),

        round(
            sum("item_total"),
            2
        ).alias("total_revenue"),

        approx_count_distinct("order_id")
        .alias("total_orders"),

        approx_count_distinct("product_id")
        .alias("total_products"),

        round(
            avg("item_total"),
            2
        ).alias("avg_order_value")
    ) \
    .withColumn(
        "gold_layer",
        lit("gold")
    )

# Top Products KPI

top_products_gold = order_items_silver \
    .groupBy(
        "event_date",
        "product_id"
    ) \
    .agg(
        count("*")
        .alias("items_sold"),

        round(
            sum("item_total"),
            2
        ).alias("product_revenue"),

        round(
            avg("price"),
            2
        ).alias("avg_price")
    ) \
    .withColumn(
        "gold_layer",
        lit("gold")
    )

# Seller Performance KPI

seller_performance_gold = order_items_silver \
    .groupBy(
        "event_date",
        "seller_id"
    ) \
    .agg(
        approx_count_distinct("order_id")
        .alias("total_orders"),

        round(
            sum("item_total"),
            2
        ).alias("seller_revenue")
    ) \
    .withColumn(
        "gold_layer",
        lit("gold")
    )

# Payment Analytics KPI

payment_analytics_gold = payments_silver \
    .groupBy(
        "event_date",
        "payment_method"
    ) \
    .agg(
        count("*")
        .alias("total_transactions"),

        round(
            sum("payment_value"),
            2
        ).alias("payment_total"),

        round(
            avg("payment_value"),
            2
        ).alias("avg_payment"),

        sum("fraud_flag")
        .alias("fraud_transactions")
    ) \
    .withColumn(
        "gold_layer",
        lit("gold")
    )

# Review Analytics KPI

review_analytics_gold = reviews_silver \
    .groupBy(
        "event_date",
        "review_sentiment"
    ) \
    .agg(
        count("*")
        .alias("total_reviews"),

        round(
            avg("rating"),
            2
        ).alias("avg_rating")
    ) \
    .withColumn(
        "gold_layer",
        lit("gold")
    )

# Shipping KPI

shipping_kpi_gold = shipments_silver \
    .groupBy(
        "event_date",
        "shipping_status"
    ) \
    .agg(
        count("*")
        .alias("total_shipments"),

        round(
            avg("delivery_days"),
            2
        ).alias("avg_delivery_days"),

        round(
            avg("shipping_fee"),
            2
        ).alias("avg_shipping_fee")
    ) \
    .withColumn(
        "gold_layer",
        lit("gold")
    )

# Inventory KPI

inventory_kpi_gold = inventory_silver \
    .groupBy(
        "event_date",
        "inventory_status"
    ) \
    .agg(
        count("*")
        .alias("total_products"),

        round(
            avg("stock_on_hand"),
            2
        ).alias("avg_stock"),

        sum("units_sold")
        .alias("units_sold")
    ) \
    .withColumn(
        "gold_layer",
        lit("gold")
    )

# Traffic KPI

traffic_kpi_gold = traffic_silver \
    .groupBy(
        "event_date",
        "traffic_source"
    ) \
    .agg(
        sum("sessions")
        .alias("total_sessions"),

        sum("unique_visitors")
        .alias("unique_visitors"),

        sum("page_views")
        .alias("page_views"),

        round(
            avg("pages_per_session"),
            2
        ).alias("avg_pages_per_session")
    ) \
    .withColumn(
        "gold_layer",
        lit("gold")
    )

# Returns KPI

returns_kpi_gold = returns_silver \
    .groupBy("event_date") \
    .agg(
        count("*")
        .alias("total_returns"),

        approx_count_distinct("order_id")
        .alias("returned_orders"),

        approx_count_distinct("product_id")
        .alias("returned_products")
    ) \
    .withColumn(
        "gold_layer",
        lit("gold")
    )

# Write Stream Function

def write_gold_stream(df, table_name):

    return df.writeStream \
        .queryName(
            f"gold_{table_name}_stream"
        ) \
        .format("delta") \
        .outputMode("complete") \
        .option(
            "checkpointLocation",
            f"{base_path}/gold/checkpoints/{table_name}"
        ) \
        .trigger(
            processingTime="10 seconds"
        ) \
        .start(
            f"{base_path}/gold/{table_name}"
        )

# Start Streaming Queries

queries = []

queries.append(
    write_gold_stream(
        daily_sales_gold,
        "daily_sales"
    )
)

queries.append(
    write_gold_stream(
        top_products_gold,
        "top_products"
    )
)

queries.append(
    write_gold_stream(
        seller_performance_gold,
        "seller_performance"
    )
)

queries.append(
    write_gold_stream(
        payment_analytics_gold,
        "payment_analytics"
    )
)

queries.append(
    write_gold_stream(
        review_analytics_gold,
        "review_analytics"
    )
)

queries.append(
    write_gold_stream(
        shipping_kpi_gold,
        "shipping_kpi"
    )
)

queries.append(
    write_gold_stream(
        inventory_kpi_gold,
        "inventory_kpi"
    )
)

queries.append(
    write_gold_stream(
        traffic_kpi_gold,
        "traffic_kpi"
    )
)

queries.append(
    write_gold_stream(
        returns_kpi_gold,
        "returns_kpi"
    )
)

print("GOLD STREAMING STARTED")
print("SILVER -> GOLD RUNNING")

spark.streams.awaitAnyTermination()