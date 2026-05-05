import json
import time
import random
import pandas as pd
from kafka import KafkaProducer

# Load data
orders = pd.read_csv(r"D:\Hoc_hanh\BigData\lakehourse\data\raw\orders.csv")
order_products = pd.read_csv(r"D:\Hoc_hanh\BigData\lakehourse\data\raw\order_products__train.csv")

df = orders.merge(order_products, on="order_id")

# Group theo user (simulate session)
user_groups = df.groupby("user_id")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def simulate_delay(hour):
    """giả lập traffic theo giờ"""
    if 18 <= hour <= 22:
        return random.uniform(0.05, 0.2)
    elif 8 <= hour <= 11:
        return random.uniform(0.1, 0.3)
    else:
        return random.uniform(0.3, 0.8)

while True:
    # chọn user random
    user_id = random.choice(list(user_groups.groups.keys()))
    user_data = user_groups.get_group(user_id)

    # lấy 1 order random của user
    order_id = random.choice(user_data["order_id"].unique())
    order_data = user_data[user_data["order_id"] == order_id]

    hour = int(order_data.iloc[0]["order_hour_of_day"])

    for _, row in order_data.iterrows():
        event = {
            "event_type": "order_item",
            "user_id": int(row["user_id"]),
            "order_id": int(row["order_id"]),
            "product_id": int(row["product_id"]),
            "add_to_cart_order": int(row["add_to_cart_order"]),
            "reordered": int(row["reordered"]),
            "order_hour": hour,
            "timestamp": int(time.time())
        }

        producer.send("user_events", event)
        print("Sent:", event)

        time.sleep(simulate_delay(hour))

    # nghỉ giữa 2 order (simulate real life)
    time.sleep(random.uniform(3, 3))