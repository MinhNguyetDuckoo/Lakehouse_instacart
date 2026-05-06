import os
import time
import random
import pandas as pd

from datetime import datetime
from kafka_producer import send_to_kafka


# ======================================================
# CONFIG
# ======================================================

BASE_PATH = r'D:\Hoc_hanh\BigData\Lakehouse_instacart\data\raw'

DAY_SLEEP = 5
EVENT_SLEEP_MIN = 0.3
EVENT_SLEEP_MAX = 1.5


# ======================================================
# LOAD CSV
# ======================================================

print("\nLOADING DATASETS...\n")

orders_df = pd.read_csv(
    os.path.join(BASE_PATH, 'orders.csv'),
    low_memory=False
)

order_items_df = pd.read_csv(
    os.path.join(BASE_PATH, 'order_items.csv'),
    dtype={
        'promo_id_2': 'string'
    },
    low_memory=False
)

payments_df = pd.read_csv(
    os.path.join(BASE_PATH, 'payments.csv'),
    low_memory=False
)

shipments_df = pd.read_csv(
    os.path.join(BASE_PATH, 'shipments.csv'),
    low_memory=False
)

returns_df = pd.read_csv(
    os.path.join(BASE_PATH, 'returns.csv'),
    low_memory=False
)

reviews_df = pd.read_csv(
    os.path.join(BASE_PATH, 'reviews.csv'),
    low_memory=False
)

inventory_df = pd.read_csv(
    os.path.join(BASE_PATH, 'inventory.csv'),
    low_memory=False
)

traffic_df = pd.read_csv(
    os.path.join(BASE_PATH, 'web_traffic.csv'),
    low_memory=False
)

print("ALL DATASETS LOADED SUCCESSFULLY\n")


# ======================================================
# CONVERT DATE COLUMNS
# ======================================================

print("CONVERTING DATE COLUMNS...\n")


def safe_to_datetime(df, column_name):

    if column_name in df.columns:

        df[column_name] = pd.to_datetime(
            df[column_name],
            errors='coerce'
        )


safe_to_datetime(orders_df, 'order_date')

safe_to_datetime(payments_df, 'payment_date')

safe_to_datetime(shipments_df, 'shipment_date')

safe_to_datetime(returns_df, 'return_date')

safe_to_datetime(reviews_df, 'review_date')

safe_to_datetime(inventory_df, 'inventory_date')

safe_to_datetime(traffic_df, 'event_date')

print("DATE CONVERSION COMPLETED\n")


# ======================================================
# HELPERS
# ======================================================

def clean_record(record):

    """
    Convert pandas/numpy types to JSON-safe types
    """

    cleaned = {}

    for key, value in record.items():

        # timestamp
        if isinstance(value, pd.Timestamp):

            cleaned[key] = value.isoformat()

        # nan
        elif pd.isna(value):

            cleaned[key] = None

        else:

            cleaned[key] = value

    return cleaned


def enrich_event(record, event_type, sequence):

    record['event_time'] = datetime.now().isoformat()

    record['event_type'] = event_type

    record['event_sequence'] = sequence

    return record


def publish(topic, record, event_type, sequence):

    try:

        record = clean_record(record)

        record = enrich_event(
            record,
            event_type,
            sequence
        )

        send_to_kafka(topic, record)

        print("\n" + "=" * 60)
        print(f"TOPIC: {topic}")
        print(f"EVENT: {event_type}")
        print(record)

        time.sleep(
            random.uniform(
                EVENT_SLEEP_MIN,
                EVENT_SLEEP_MAX
            )
        )

    except Exception as e:

        print(f"\nERROR PUBLISHING TO {topic}")
        print(e)


# ======================================================
# ORDER JOURNEY STREAM
# ======================================================

def stream_order_journey(order_row):

    order_id = order_row['order_id']

    print("\n")
    print("#" * 60)
    print(f"PROCESSING ORDER: {order_id}")
    print("#" * 60)

    # ==================================================
    # 1. ORDER CREATED
    # ==================================================

    publish(
        topic='orders_topic',
        record=order_row.to_dict(),
        event_type='ORDER_CREATED',
        sequence=1
    )

    # ==================================================
    # 2. ORDER ITEMS
    # ==================================================

    items = order_items_df[
        order_items_df['order_id'] == order_id
    ]

    for _, item in items.iterrows():

        publish(
            topic='order_items_topic',
            record=item.to_dict(),
            event_type='ITEM_ADDED',
            sequence=2
        )

    # ==================================================
    # 3. PAYMENTS
    # ==================================================

    payments = payments_df[
        payments_df['order_id'] == order_id
    ]

    for _, payment in payments.iterrows():

        publish(
            topic='payments_topic',
            record=payment.to_dict(),
            event_type='PAYMENT_COMPLETED',
            sequence=3
        )

    # ==================================================
    # 4. SHIPMENTS
    # ==================================================

    shipments = shipments_df[
        shipments_df['order_id'] == order_id
    ]

    for _, shipment in shipments.iterrows():

        publish(
            topic='shipments_topic',
            record=shipment.to_dict(),
            event_type='SHIPMENT_CREATED',
            sequence=4
        )

    # ==================================================
    # 5. REVIEWS
    # ==================================================

    reviews = reviews_df[
        reviews_df['order_id'] == order_id
    ]

    for _, review in reviews.iterrows():

        publish(
            topic='reviews_topic',
            record=review.to_dict(),
            event_type='REVIEW_SUBMITTED',
            sequence=5
        )

    # ==================================================
    # 6. RETURNS
    # ==================================================

    returns = returns_df[
        returns_df['order_id'] == order_id
    ]

    for _, ret in returns.iterrows():

        publish(
            topic='returns_topic',
            record=ret.to_dict(),
            event_type='RETURN_REQUESTED',
            sequence=6
        )


# ======================================================
# INVENTORY EVENTS
# ======================================================

def stream_inventory_events(current_day):

    if 'inventory_date' not in inventory_df.columns:
        return

    daily_inventory = inventory_df[
        inventory_df['inventory_date'].dt.date == current_day
    ]

    if len(daily_inventory) == 0:
        return

    print("\nSTREAMING INVENTORY EVENTS...\n")

    for _, row in daily_inventory.iterrows():

        publish(
            topic='inventory_topic',
            record=row.to_dict(),
            event_type='INVENTORY_UPDATED',
            sequence=100
        )


# ======================================================
# TRAFFIC EVENTS
# ======================================================

def stream_traffic_events(current_day):

    if 'event_date' not in traffic_df.columns:
        return

    daily_traffic = traffic_df[
        traffic_df['event_date'].dt.date == current_day
    ]

    if len(daily_traffic) == 0:
        return

    print("\nSTREAMING TRAFFIC EVENTS...\n")

    for _, row in daily_traffic.iterrows():

        publish(
            topic='traffic_topic',
            record=row.to_dict(),
            event_type='WEB_TRAFFIC',
            sequence=200
        )


# ======================================================
# TIMELINE ENGINE
# ======================================================

def start_streaming():

    print("\nSTARTING EVENT STREAMING ENGINE...\n")

    # ==================================================
    # GET ALL DAYS
    # ==================================================

    unique_days = sorted(
        orders_df['order_date']
        .dropna()
        .dt.date
        .unique()
    )

    print(f"TOTAL DAYS: {len(unique_days)}")

    # ==================================================
    # LOOP THROUGH DAYS
    # ==================================================

    for current_day in unique_days:

        print("\n")
        print("=" * 80)
        print(f"STREAMING DAY: {current_day}")
        print("=" * 80)

        # ==================================================
        # DAILY ORDERS
        # ==================================================

        daily_orders = orders_df[
            orders_df['order_date'].dt.date == current_day
        ]

        # shuffle order inside same day
        daily_orders = daily_orders.sample(
            frac=1,
            random_state=random.randint(1, 999999)
        )

        print(
            f"\nTOTAL ORDERS TODAY: {len(daily_orders)}\n"
        )

        # ==================================================
        # PROCESS ORDERS
        # ==================================================

        for _, order in daily_orders.iterrows():

            stream_order_journey(order)

        # ==================================================
        # INDEPENDENT EVENTS
        # ==================================================

        stream_inventory_events(current_day)

        stream_traffic_events(current_day)

        print("\n")
        print("=" * 80)
        print(f"DAY {current_day} FINISHED")
        print("=" * 80)

        # simulate next business day
        time.sleep(DAY_SLEEP)


# ======================================================
# MAIN
# ======================================================

if __name__ == '__main__':

    try:

        start_streaming()

    except KeyboardInterrupt:

        print("\nSTREAMING STOPPED MANUALLY")

    except Exception as e:

        print("\nFATAL ERROR")
        print(e)