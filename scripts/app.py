import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import col

# =========================
# INIT SPARK
# =========================
@st.cache_resource
def load_spark():
    return SparkSession.builder \
        .appName("StreamlitRecommendation") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

spark = load_spark()

# =========================
# LOAD MODEL
# =========================
@st.cache_resource
def load_model():
    return ALSModel.load("models/als_model")

model = load_model()

# =========================
# LOAD PRODUCT NAME
# =========================
@st.cache_data
def load_products():
    df = pd.read_csv("data/raw/products.csv")
    return dict(zip(df.product_id, df.product_name))

product_dict = load_products()

# =========================
# UI
# =========================
st.title("🛒 Instacart Recommendation System")

user_id = st.number_input("Enter User ID:", min_value=1, step=1)

if st.button("Recommend"):

    # =========================
    # CREATE USER DF
    # =========================
    user_df = spark.createDataFrame([(int(user_id),)], ["user_id"])

    # =========================
    # USER HISTORY (SILVER)
    # =========================
    st.subheader("🛒 User Purchased:")

    try:
        user_history = spark.read.parquet("data/silver") \
            .filter(col("user_id") == int(user_id)) \
            .select("product_id") \
            .distinct() \
            .limit(10)

        history_list = user_history.collect()

        if len(history_list) == 0:
            st.write("❌ No purchase history")
        else:
            for h in history_list:
                name = product_dict.get(h["product_id"], "Unknown")
                st.write(f"• {name}")

    except Exception as e:
        st.error("❌ Error loading user history (check Silver layer)")
        st.text(str(e))

    # =========================
    # RECOMMENDATION (ALS)
    # =========================
    st.subheader("🎯 Recommended for you:")

    try:
        recs = model.recommendForUserSubset(user_df, 5)
        result = recs.collect()

        if len(result) == 0:
            st.warning("⚠️ User mới → fallback popular products")

            # =========================
            # POPULAR (GOLD)
            # =========================
            popular = spark.read.parquet("data/gold/user") \
                .orderBy(col("total_items").desc()) \
                .limit(5)

            popular_list = popular.collect()

            for p in popular_list:
                st.write(f"🔥 User {p['user_id']} (popular profile)")

        else:
            rec_list = result[0]["recommendations"]

            rows = []
            for r in rec_list:
                if r["rating"] > 0:
                    name = product_dict.get(r["product_id"], "Unknown")
                    rows.append((name, round(r["rating"], 3)))

            if len(rows) == 0:
                st.write("No strong recommendation found")
            else:
                df = pd.DataFrame(rows, columns=["Product", "Score"])
                st.dataframe(df)

    except Exception as e:
        st.error("❌ Error during recommendation")
        st.text(str(e))

# =========================
# FOOTER (GIẢI THÍCH)
# =========================
st.markdown("---")
st.markdown("""
### 🧠 How it works:
- **Silver layer** → chứa lịch sử mua hàng chi tiết (user_id, product_id)
- **Gold layer** → chứa dữ liệu tổng hợp phục vụ training
- Model sử dụng **ALS (Collaborative Filtering)**
- Gợi ý dựa trên hành vi của các user tương tự
""")