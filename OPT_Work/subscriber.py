import time
import json
import pandas as pd
import psycopg2
import subprocess
import threading
from google.cloud import pubsub_v1
from datetime import datetime, timedelta


project_id = "dataengineeringproject-456307"
subscription_id = "MyTopic1-sub"

DBname = "trimet_data"
DBuser = "srilakshmi"
DBpwd = "srilu2001"

BATCH_SIZE = 5000
batch = []
batch_lock = threading.Lock()   # <<< FIX 1: Thread safety

total_received = 0
first_message_received = False

start_time = time.time()
last_message_time = time.time()

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)


conn = psycopg2.connect(
    host="localhost",
    database=DBname,
    user=DBuser,
    password=DBpwd
)
cursor = conn.cursor()


def insert_batch(df):
    if df.empty:
        return

    trip_rows = df[['trip_id', 'route_id', 'vehicle_id', 'service_key', 'direction']].astype(object)
    cursor.executemany("""
        INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction)
        VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
    """, trip_rows.values.tolist())

    bc_rows = df[['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']].astype(object)
    cursor.executemany("""
        INSERT INTO breadcrumb (tstamp, latitude, longitude, speed, trip_id)
        VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
    """, bc_rows.values.tolist())

    conn.commit()


def transform(df):
    df = df.copy()

    df['NEW_OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], format='%d%b%Y:%H:%M:%S', errors='coerce')
    df['DAY_NAME'] = df['NEW_OPD_DATE'].dt.day_name()

    df['TIMESTAMP'] = df['NEW_OPD_DATE'] + df['ACT_TIME'].clip(0, 86399).apply(lambda x: timedelta(seconds=x))

    df['SPEED'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff().div(
        df.groupby('EVENT_NO_TRIP')['ACT_TIME'].diff()).clip(lower=0)

    valid = df.dropna(subset=['TIMESTAMP'])

    df_trip = valid.drop_duplicates('EVENT_NO_TRIP')[['EVENT_NO_TRIP', 'VEHICLE_ID', 'DAY_NAME']]
    df_trip['route_id'] = 0
    df_trip['direction'] = 'Out'
    df_trip = df_trip.rename(columns={
        'EVENT_NO_TRIP': 'trip_id',
        'VEHICLE_ID': 'vehicle_id',
        'DAY_NAME': 'service_key'
    })

    df_bc = valid.rename(columns={
        'TIMESTAMP': 'tstamp',
        'GPS_LATITUDE': 'latitude',
        'GPS_LONGITUDE': 'longitude',
        'EVENT_NO_TRIP': 'trip_id'
    })[['tstamp', 'latitude', 'longitude', 'SPEED', 'trip_id']].rename(columns={'SPEED': 'speed'})

    return df_trip.merge(df_bc, on='trip_id', how='outer')


def get_backlog():
    try:
        result = subprocess.check_output([
            "gcloud", "pubsub", "subscriptions", "describe", subscription_id,
            "--format=value(backlogSize)"
        ]).decode().strip()

        return int(result) if result else 0
    except:
        return -1

def callback(message):
    global last_message_time, total_received, first_message_received

    last_message_time = time.time()
    total_received += 1
    first_message_received = True

    if total_received % 100000 == 0:
        print(f"Received {total_received} messages...")

    with batch_lock:
        batch.append(json.loads(message.data))

        if len(batch) >= BATCH_SIZE:
            df = pd.DataFrame(batch.copy())
            batch.clear()

            df = transform(df)
            insert_batch(df)

    message.ack()

print(f"Listening on {subscription_path}...\n")
stream = subscriber.subscribe(subscription_path, callback=callback)

try:
    while True:
        time.sleep(10)

        idle = time.time() - last_message_time
        backlog = get_backlog()

        print(f"[DEBUG] Backlog: {backlog} | Idle: {idle:.1f}")

        # Stop only AFTER receiving first message
        if first_message_received and backlog == 0 and idle > 180:
            print("\nNo backlog and no new messages — stopping subscriber.")
            stream.cancel()
            break

except KeyboardInterrupt:
    print("Stopped manually.")
    stream.cancel()

with batch_lock:
    if batch:
        df = pd.DataFrame(batch)
        df = transform(df)
        insert_batch(df)

cursor.close()
conn.close()

end_time = time.time()

print("\n======== FINAL SUMMARY ========")
print(f"Total messages received: {total_received}")
print(f"Total subscriber time: {(end_time - start_time) / 60:.2f} minutes")
print("DONE.")
