import os
import json
import requests
import zipfile
import shutil
import logging
from datetime import date
from google.cloud import pubsub_v1
import pandas as pd
import concurrent. futures
import time

start_time = time.time()

# === LOAD SERVICE ACCOUNT KEY ===
script_dir = os.path.dirname(os.path.abspath(__file__))
KEY_PATH = os.path.join(script_dir, "dataengineeringproject-456307-2dca2bb9e633.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

# === CONFIGURATION ===
today_str = date.today().isoformat()
output_folder = "bus_data"
processed_data_folder = "processed_data"
extract_folder = os.path.join("extracted_json", today_str)

project_id = "dataengineeringproject-456307"
topic_id = "MyTopic1"

# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# set up basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s:%(message)s")
logger = logging.getLogger(__name__)

def gather_bus_data():
    os.makedirs(output_folder, exist_ok=True)
    vehicle_ids_path = os.path.join(script_dir, "vehicles_300.csv")

    try:
        df = pd.read_csv(vehicle_ids_path, header=None)
        vehicle_ids = df[0].astype(str).str.strip().tolist()
    except Exception as e:
        logger.error(f"Failed to read vehicle_ids.csv: {e}")
        return 0

    total_records = 0
    for vid in vehicle_ids:
        try:
            url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vid}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                total_records += len(data)
                file_path = os.path.join(output_folder, f"bus_{vid}_{today_str}.json")
                with open(file_path, "w") as out:
                    out.write(response.text)
            else:
                logger.debug(f"Non-200 for {vid}: {response.status_code}")
        except Exception as e:
            logger.error(f"Error gathering data for vehicle {vid}: {e}")

    os.makedirs(processed_data_folder, exist_ok=True)
    zip_base = os.path.join(processed_data_folder, f"bus_data_{today_str}")
    try:
        shutil.make_archive(zip_base, 'zip', output_folder)
    except Exception as e:
        logger.error(f"Error creating zip archive: {e}")

    try:
        shutil.rmtree(output_folder)
    except Exception as e:
        logger.error(f"Error removing temporary folder {output_folder}: {e}")

    return total_records

def unzip_data(zip_path, extract_to):
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
    except Exception as e:
        logger.error(f"Error unzipping {zip_path} to {extract_to}: {e}")

def futures_callback(future):
    try:
        # will raise if publish failed
        message_id = future.result()
    except Exception as e:
        logger.error(f"[publish] failed: {e}")

def publish_data(folder):
    count = 0
    futures_list = []

    for filename in os.listdir(folder):
        if not filename.endswith(".json"):
            continue
        file_path = os.path.join(folder, filename)
        try:
            with open(file_path, "r") as f:
                records = json.load(f)
        except Exception as e:
            logger.error(f"Error reading JSON {file_path}: {e}")
            continue

        # schedule each record for publish
        for record in records:
            count += 1
            data = json.dumps(record).encode("utf-8")
            try:
                future = publisher.publish(topic_path, data)
                future.add_done_callback(futures_callback)
                futures_list.append(future)
            except Exception as e:
                logger.error(f"Error publishing record {record.get('vehicle_id', '')}: {e}")

        # remove the file once its records are scheduled
        try:
            os.remove(file_path)
        except Exception as e:
            logger.error(f"Error removing processed file {file_path}: {e}")

    # wait for all publishes to finish, using continue instead of pass
    for future in concurrent.futures.as_completed(futures_list):
        continue

    return count

def main():
    start_time = time.time()
    gathered = gather_bus_data()
    print(f"Total breadcrumbs saved: {gathered}")

    zip_path = os.path.join(processed_data_folder, f"bus_data_{today_str}.zip")
    if os.path.exists(zip_path):
        os.makedirs(extract_folder, exist_ok=True)
        unzip_data(zip_path, extract_folder)
        published = publish_data(extract_folder)
        print(f"Total records published: {published}")

        try:
            shutil.rmtree(extract_folder)
        except Exception as e:
            logger.error(f"Error cleaning up {extract_folder}: {e}")


    end_time = time.time()
    elapsed = end_time - start_time
    print(f"[VM1] data_gather.py runtime: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
if __name__ == "__main__":
    main()
