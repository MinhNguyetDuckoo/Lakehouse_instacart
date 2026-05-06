import os
import streamlit as st
import pandas as pd
import plotly.express as px

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from streamlit_autorefresh import st_autorefresh


# =========================================================
# TIMEZONE
# =========================================================

os.environ["TZ"] = "Asia/Ho_Chi_Minh"


# =========================================================
# PAGE CONFIG
# =========================================================

st.set_page_config(
    page_title="Realtime Ecommerce Lakehouse",
    page_icon="🚀",
    layout="wide"
)

st.title("🚀 Realtime Ecommerce Lakehouse Dashboard")

st.markdown("---")


# =========================================================
# AUTO REFRESH
# =========================================================

st_autorefresh(
    interval=5000,
    key="dashboard_refresh"
)


# =========================================================
# SPARK SESSION
# =========================================================

@st.cache_resource
def create_spark_session():

    spark = SparkSession.builder \
        .appName("Lakehouse Dashboard") \
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        ) \
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ) \
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.conf.set(
        "spark.sql.session.timeZone",
        "Asia/Ho_Chi_Minh"
    )

    return spark


spark = create_spark_session()


# =========================================================
# LOAD DELTA TABLE
# =========================================================

def load_delta_table(path):

    try:

        df = spark.read.format("delta").load(path)

        # convert timestamp -> string
        for field in df.schema.fields:

            if field.dataType.simpleString() == "timestamp":

                df = df.withColumn(
                    field.name,
                    col(field.name).cast("string")
                )

        return df.toPandas()

    except Exception as e:

        st.warning(f"Cannot load table: {path}")
        st.error(str(e))

        return pd.DataFrame()


# =========================================================
# LOAD GOLD TABLES
# =========================================================

sales_df = load_delta_table(
    "data/gold/sales_kpi"
)

customer_df = load_delta_table(
    "data/gold/customer_metrics"
)

review_df = load_delta_table(
    "data/gold/review_analytics"
)

inventory_df = load_delta_table(
    "data/gold/inventory_alert"
)

traffic_df = load_delta_table(
    "data/gold/traffic_kpi"
)

delivery_df = load_delta_table(
    "data/gold/delivery_kpi"
)

fraud_df = load_delta_table(
    "data/gold/fraud_alert"
)


# =========================================================
# KPI METRICS
# =========================================================

st.subheader("📊 Business KPIs")

total_revenue = 0
total_orders = 0
avg_order_value = 0

if not sales_df.empty:

    if "total_revenue" in sales_df.columns:

        total_revenue = round(
            sales_df["total_revenue"].sum(),
            2
        )

    if "total_orders" in sales_df.columns:

        total_orders = int(
            sales_df["total_orders"].sum()
        )

    if "avg_order_value" in sales_df.columns:

        avg_order_value = round(
            sales_df["avg_order_value"].mean(),
            2
        )

col1, col2, col3 = st.columns(3)

col1.metric(
    "💰 Total Revenue",
    f"${total_revenue}"
)

col2.metric(
    "📦 Total Orders",
    total_orders
)

col3.metric(
    "🛒 Avg Order Value",
    f"${avg_order_value}"
)

st.markdown("---")


# =========================================================
# REVENUE ANALYTICS
# =========================================================

st.subheader("📈 Revenue Analytics")

if not sales_df.empty:

    fig = px.line(
        sales_df,
        x="window_start",
        y="total_revenue",
        markers=True,
        title="Revenue Per Window"
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

else:

    st.warning("sales_kpi is empty")

st.markdown("---")


# =========================================================
# REVIEW ANALYTICS
# =========================================================

st.subheader("⭐ Review Sentiment")

if not review_df.empty:

    fig2 = px.pie(
        review_df,
        names="review_sentiment",
        values="total_reviews",
        title="Review Sentiment Distribution"
    )

    st.plotly_chart(
        fig2,
        use_container_width=True
    )

else:

    st.warning("review_analytics is empty")

st.markdown("---")


# =========================================================
# INVENTORY MONITORING
# =========================================================

st.subheader("📦 Inventory Monitoring")

if not inventory_df.empty:

    fig3 = px.pie(
        inventory_df,
        names="inventory_status",
        values="total_products",
        title="Inventory Status"
    )

    st.plotly_chart(
        fig3,
        use_container_width=True
    )

else:

    st.warning("inventory_alert is empty")

st.markdown("---")


# =========================================================
# TRAFFIC ANALYTICS
# =========================================================

st.subheader("🌐 Traffic Analytics")

if not traffic_df.empty:

    fig4 = px.bar(
        traffic_df,
        x="traffic_source",
        y="total_sessions",
        title="Traffic Sources"
    )

    st.plotly_chart(
        fig4,
        use_container_width=True
    )

else:

    st.warning("traffic_kpi is empty")

st.markdown("---")


# =========================================================
# DELIVERY KPI
# =========================================================

st.subheader("🚚 Delivery KPI")

if not delivery_df.empty:

    st.dataframe(
        delivery_df,
        use_container_width=True
    )

else:

    st.warning("delivery_kpi is empty")

st.markdown("---")


# =========================================================
# TOP CUSTOMERS
# =========================================================

st.subheader("👑 Top Customers")

if not customer_df.empty:

    top_customers = customer_df.sort_values(
        by="total_orders",
        ascending=False
    ).head(10)

    st.dataframe(
        top_customers,
        use_container_width=True
    )

else:

    st.warning("customer_metrics is empty")

st.markdown("---")


# =========================================================
# FRAUD ALERTS
# =========================================================

st.subheader("🚨 Fraud Alerts")

if not fraud_df.empty:

    st.dataframe(
        fraud_df,
        use_container_width=True
    )

else:

    st.success("No fraud alerts detected")

st.markdown("---")


# =========================================================
# FOOTER
# =========================================================

st.caption(
    "Realtime dashboard auto refresh every 5 seconds"
)