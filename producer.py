from google.cloud import pubsub_v1
import csv
import json
import glob
import os
import time

# Load service account
files = glob.glob("*.json")
if not files:
    raise FileNotFoundError("Service account JSON key not found.")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

project_id = "my-project-1528075008286"
topic_name = "Design"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Publishing messages to {topic_path}\n")

with open("Labels.csv", newline='', encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        try:
            # Normalize column names
            record = {
                "time": float(row["time"]) if row["time"] else None,
                "profile_name": row["profileName"],
                "temperature": float(row["temperature"]) if row["temperature"] else None,
                "humidity": float(row["humidity"]) if row["humidity"] else None,
                "pressure": float(row["pressure"]) if row["pressure"] else None,
            }

            message_data = json.dumps(record).encode("utf-8")

            future = publisher.publish(topic_path, message_data)
            future.result()

            print("Published:", record)
            time.sleep(0.2)

        except Exception as e:
            print("Failed to publish record:", e)
