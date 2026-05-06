from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("Lakehouse Bronze Streaming") \
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


orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("order_date", StringType()),
    StructField("customer_id", StringType()),
    StructField("zip", StringType()),
    StructField("order_status", StringType()),
    StructField("payment_method", StringType()),
    StructField("device_type", StringType()),
    StructField("order_source", StringType())
])


order_items_schema = StructType([
    StructField("order_id", StringType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("price", StringType()),
    StructField("freight_value", StringType())
])


payments_schema = StructType([
    StructField("order_id", StringType()),
    StructField("payment_method", StringType()),
    StructField("payment_value", StringType()),
    StructField("installments", StringType())
])


reviews_schema = StructType([
    StructField("review_id", StringType()),
    StructField("order_id", StringType()),
    StructField("product_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("review_date", StringType()),
    StructField("rating", StringType()),
    StructField("review_title", StringType())
])


shipments_schema = StructType([
    StructField("order_id", StringType()),
    StructField("ship_date", StringType()),
    StructField("delivery_date", StringType()),
    StructField("shipping_fee", StringType())
])


returns_schema = StructType([
    StructField("return_id", StringType()),
    StructField("order_id", StringType()),
    StructField("product_id", StringType()),
    StructField("return_reason", StringType()),
    StructField("return_date", StringType())
])


inventory_schema = StructType([
    StructField("snapshot_date", StringType()),
    StructField("product_id", StringType()),
    StructField("stock_on_hand", StringType()),
    StructField("units_received", StringType()),
    StructField("units_sold", StringType())
])


traffic_schema = StructType([
    StructField("date", StringType()),
    StructField("sessions", StringType()),
    StructField("unique_visitors", StringType()),
    StructField("page_views", StringType()),
    StructField("traffic_source", StringType())
])


def create_stream(topic, schema, event_column):

    kafka_df = spark.readStream \
        .format("kafka") \
        .option(
            "kafka.bootstrap.servers",
            "localhost:9092"
        ) \
        .option(
            "subscribe",
            topic
        ) \
        .option(
            "startingOffsets",
            "earliest"
        ) \
        .load()

    parsed_df = kafka_df \
        .selectExpr(
            "CAST(value AS STRING) as raw_json",
            "topic",
            "partition",
            "offset",
            "timestamp as kafka_timestamp"
        ) \
        .withColumn(
            "data",
            from_json(
                col("raw_json"),
                schema
            )
        ) \
        .select(
            "raw_json",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "data.*"
        ) \
        .withColumn(
            "event_timestamp",
            to_timestamp(col(event_column))
        ) \
        .withColumn(
            "event_date",
            to_date(col("event_timestamp"))
        ) \
        .withColumn(
            "bronze_ingest_time",
            current_timestamp()
        ) \
        .withColumn(
            "bronze_layer",
            lit("bronze")
        ) \
        .withColumn(
            "data_source",
            lit(topic)
        )

    return parsed_df


def write_bronze_stream(df, table_name):

    return df.writeStream \
        .queryName(
            f"bronze_{table_name}_stream"
        ) \
        .format("delta") \
        .outputMode("append") \
        .partitionBy("event_date") \
        .trigger(
            processingTime="5 seconds"
        ) \
        .option(
            "checkpointLocation",
            f"data/bronze/checkpoints/{table_name}"
        ) \
        .option(
            "mergeSchema",
            "true"
        ) \
        .start(
            f"data/bronze/{table_name}"
        )


orders_stream = create_stream(
    "orders_topic",
    orders_schema,
    "order_date"
)

order_items_stream = create_stream(
    "order_items_topic",
    order_items_schema,
    "order_id"
)

payments_stream = create_stream(
    "payments_topic",
    payments_schema,
    "order_id"
)

reviews_stream = create_stream(
    "reviews_topic",
    reviews_schema,
    "review_date"
)

shipments_stream = create_stream(
    "shipments_topic",
    shipments_schema,
    "ship_date"
)

returns_stream = create_stream(
    "returns_topic",
    returns_schema,
    "return_date"
)

inventory_stream = create_stream(
    "inventory_topic",
    inventory_schema,
    "snapshot_date"
)

traffic_stream = create_stream(
    "traffic_topic",
    traffic_schema,
    "date"
)


orders_query = write_bronze_stream(
    orders_stream,
    "orders"
)

order_items_query = write_bronze_stream(
    order_items_stream,
    "order_items"
)

payments_query = write_bronze_stream(
    payments_stream,
    "payments"
)

reviews_query = write_bronze_stream(
    reviews_stream,
    "reviews"
)

shipments_query = write_bronze_stream(
    shipments_stream,
    "shipments"
)

returns_query = write_bronze_stream(
    returns_stream,
    "returns"
)

inventory_query = write_bronze_stream(
    inventory_stream,
    "inventory"
)

traffic_query = write_bronze_stream(
    traffic_stream,
    "traffic"
)


debug_query = orders_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()


print("Bronze Streaming Started")
print("Kafka -> Delta Bronze Running")


spark.streams.awaitAnyTermination()