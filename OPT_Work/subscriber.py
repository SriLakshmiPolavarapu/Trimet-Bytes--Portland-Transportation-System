import time
import os
import json
import pandas as pd
import psycopg2
from google.cloud import pubsub_v1
from datetime import datetime, timedelta

# === Config ===
project_id = "dataengineeringproject-456307"
subscription_id = "MyTopic1-sub"
DBname = "trimet_data"
DBuser = "srilakshmi"
DBpwd = "srilu2001"

# === Pub/Sub Setup ===
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
json_list = []

# === Start time for runtime measurement ===
start_time = time.time()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        json_message = json.loads(message.data.decode('utf-8'))
        json_list.append(json_message)
        # Debug print every 50,000 messages
        if len(json_list) % 50000 == 0:
           print(f"Received {len(json_list)} messages...")

    except Exception as e:
        print(f"[callback] error decoding message: {e}")
    finally:
        message.ack()

# Subscribe to messages
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...\n")

try:
    # Keep listening until the queue is empty
    while True:
        if streaming_pull_future.done():
            break
        time.sleep(5)  # small delay to prevent busy loop
except KeyboardInterrupt:
    streaming_pull_future.cancel()
    print("Stopped subscriber by user.")

# Convert messages to DataFrame
df = pd.DataFrame(json_list)
if df.empty:
    print("No messages received.")
    exit()

# === Transformation ===
df['NEW_OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S', errors='coerce')
df['DAY_OF_WEEK'] = df['NEW_OPD_DATE'].dt.dayofweek
df['DAY_NAME'] = df['DAY_OF_WEEK'].map({
    0: 'Weekday', 1: 'Weekday', 2: 'Weekday',
    3: 'Weekday', 4: 'Weekday', 5: 'Saturday', 6: 'Sunday'
})

def create_timestamp(row):
    try:
        opd_date = datetime.strptime(row['OPD_DATE'], '%d%b%Y:%H:%M:%S')
        act_time = timedelta(seconds=min(row.get('ACT_TIME', 0), 86399))
        return pd.Timestamp(opd_date + act_time)
    except Exception as e:
        return pd.NaT

df['TIMESTAMP'] = df.apply(create_timestamp, axis=1)
df['SPEED'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff() / df.groupby('EVENT_NO_TRIP')['ACT_TIME'].diff()
df['SPEED'] = df['SPEED'].bfill().clip(lower=0)
df['GPS_LATITUDE'] = df['GPS_LATITUDE'].fillna(0.0)
df['GPS_LONGITUDE'] = df['GPS_LONGITUDE'].fillna(0.0)

# === Filter valid rows ===
valid_rows = df[
    (df['VEHICLE_ID'] > 0) &
    (df['ACT_TIME'].between(0, 86399)) &
    (df['GPS_LATITUDE'].between(-90, 90)) &
    (df['GPS_LONGITUDE'].between(-180, 180)) &
    (df['EVENT_NO_TRIP'] > 0) &
    (df['METERS'] >= 0) &
    (df['SPEED'] >= 0) &
    (~df['TIMESTAMP'].isna()) &
    (df['DAY_OF_WEEK'].between(0, 6))
].copy()

# Prepare trip and breadcrumb DataFrames
result_df = valid_rows.drop_duplicates(subset=['EVENT_NO_TRIP'], keep='first')
result_df.loc[:, 'ROUTE_ID'] = 0
result_df.loc[:, 'DIRECTION'] = 'Out'

df_trip = result_df[[
    'EVENT_NO_TRIP', 'ROUTE_ID', 'VEHICLE_ID', 'DAY_NAME', 'DIRECTION'
]].rename(columns={
    'EVENT_NO_TRIP': 'trip_id',
    'ROUTE_ID': 'route_id',
    'VEHICLE_ID': 'vehicle_id',
    'DAY_NAME': 'service_key',
    'DIRECTION': 'direction'
})

df_breadcrumb = valid_rows[[
    'TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP'
]].rename(columns={
    'TIMESTAMP': 'tstamp',
    'GPS_LATITUDE': 'latitude',
    'GPS_LONGITUDE': 'longitude',
    'SPEED': 'speed',
    'EVENT_NO_TRIP': 'trip_id'
})

# === Insert into PostgreSQL ===
conn = psycopg2.connect(
    host="localhost",
    database=DBname,
    user=DBuser,
    password=DBpwd
)
cursor = conn.cursor()

def insert_with_conflict(df, table, columns, pk):
    if df.empty:
        return
    values = [tuple(x) for x in df[columns].to_numpy()]
    placeholders = ", ".join(["%s"] * len(columns))
    columns_str = ", ".join(columns)
    query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders}) ON CONFLICT ({pk}) DO NOTHING"
    try:
        cursor.executemany(query, values)
        conn.commit()
        print(f"[DB] Inserted {len(df)} rows into {table} (duplicates skipped)")
    except Exception as e:
        conn.rollback()
        print(f"[DB] Error inserting into {table}: {e}")

insert_with_conflict(df_trip, "trip", ['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction'], 'trip_id')
insert_with_conflict(df_breadcrumb, "breadcrumb", ['tstamp', 'latitude', 'longitude', 'speed', 'trip_id'], 'tstamp')

cursor.close()
conn.close()

# === End time and runtime print ===
end_time = time.time()
elapsed = end_time - start_time
print(f"[VM2] subscriber.py runtime: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")

print(f"Total messages received: {len(json_list)}")
print(f"Valid trips to insert: {len(df_trip)}")
print(f"Valid breadcrumbs to insert: {len(df_breadcrumb)}")
