from confluent_kafka import Producer
import json
import os
import uuid
from datetime import datetime, timezone

producer_config = {
    "bootstrap.servers": "kafka:9092"
}

producer = Producer(producer_config)

input_path = "/app/input"


def callback(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(
            f"Delivered: "
            f"{msg.topic()} "
            f"[{msg.partition()}] "
            f"offset={msg.offset()}"
        )


for filename in os.listdir(input_path):
    if not filename.endswith(".json"):
        continue
    file_path = os.path.join(input_path, filename)
    topic = filename.replace(".json", "")
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for row in data:
        event = {
            "event_id": str(uuid.uuid4()),
            "source": filename,
            "source_timestamp": row.get("date"),
            "ingestion_timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "data": row
        }

        producer.produce(
            topic=topic,
            value=json.dumps(event),
            callback=callback
        )

producer.flush()