import os
import json
import requests
import zipfile
import shutil
import logging
from datetime import date
from google.cloud import pubsub_v1
import pandas as pd
import time

# Start timer
start_time = time.time()

# === Load service account ===
script_dir = os.path.dirname(os.path.abspath(__file__))
KEY_PATH = os.path.join(script_dir, "dataengineeringproject-456307-2dca2bb9e633.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

# === Configuration ===
today_str = date.today().isoformat()
output_folder = "bus_data"
processed_data_folder = "processed_data"
extract_folder = os.path.join("extracted_json", today_str)

project_id = "dataengineeringproject-456307"
topic_id = "MyTopic1"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s"
)
logger = logging.getLogger(__name__)


def clean_directories():
    """Ensure all directories are clean before running gather."""
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    os.makedirs(output_folder, exist_ok=True)

    if os.path.exists(extract_folder):
        shutil.rmtree(extract_folder)

    os.makedirs(processed_data_folder, exist_ok=True)


def gather_bus_data():
    vehicle_ids_path = os.path.join(script_dir, "vehicles_676.csv")

    try:
        df = pd.read_csv(vehicle_ids_path, header=None)
        vehicle_ids = df[0].astype(str).str.strip().tolist()
    except Exception as e:
        logger.error(f"Failed to read vehicle IDs file: {e}")
        return 0

    total_records = 0

    for vid in vehicle_ids:
        try:
            url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vid}"
            response = requests.get(url, timeout=30)

            if response.status_code == 200:
                data = response.json()
                total_records += len(data)

                file_path = os.path.join(output_folder, f"bus_{vid}_{today_str}.json")
                with open(file_path, "w") as out:
                    out.write(json.dumps(data))

            else:
                logger.warning(f"Non-200 for vehicle {vid}: {response.status_code}")

        except Exception as e:
            logger.error(f"Error gathering data for vehicle {vid}: {e}")

    logger.info(f"Finished data gather phase. Total breadcrumbs downloaded: {total_records}")
    print(f"Total breadcrumbs downloaded: {total_records}")
    return total_records


def zip_data():
    zip_base = os.path.join(processed_data_folder, f"bus_data_{today_str}")

    # Remove old zip file if exists
    zip_path = f"{zip_base}.zip"
    if os.path.exists(zip_path):
        os.remove(zip_path)

    try:
        shutil.make_archive(zip_base, 'zip', output_folder)
        logger.info(f"Created zip archive at {zip_base}.zip")
        return zip_path
    except Exception as e:
        logger.error(f"Error creating zip archive: {e}")
        return None


def unzip_data(zip_path):
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_folder)
        logger.info(f"Unzipped {zip_path} to {extract_folder}")
    except Exception as e:
        logger.error(f"Error unzipping {zip_path}: {e}")


def publish_data(folder):
    count = 0

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

        for record in records:
            count += 1
            data = json.dumps(record).encode("utf-8")

            future = publisher.publish(topic_path, data=data)
            future.add_done_callback(lambda _: None)

            # ⭐ CRITICAL FIX: slow down publishing slightly to avoid Pub/Sub throttle
            time.sleep(0.0005)

            if count % 50000 == 0:
                logger.info(f"Queued {count} messages...")

        os.remove(file_path)

    logger.info("Stopping Pub/Sub publisher to flush outstanding messages...")

    try:
        publisher.stop()
    except AttributeError:
        pass

    logger.info(f"Finished publish phase. Total records published: {count}")
    print(f"Total records published: {count}")

    return count


def main():
    clean_directories()

    total_downloaded = gather_bus_data()
    zip_path = zip_data()

    if zip_path and os.path.exists(zip_path):
        unzip_data(zip_path)
        total_published = publish_data(extract_folder)

        shutil.rmtree(extract_folder)

        elapsed = time.time() - start_time
        print(f"[VM1] data_gather.py runtime: {elapsed:.2f} sec ({elapsed/60:.2f} min)")
        print(f"Summary: downloaded={total_downloaded}, published={total_published}")


if __name__ == "__main__":
    main()
