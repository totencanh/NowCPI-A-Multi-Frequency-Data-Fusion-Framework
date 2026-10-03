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


for source in os.listdir(input_path):

    source_path = os.path.join(input_path, source)

    if not os.path.isdir(source_path):
        continue

    topic = source

    for filename in os.listdir(source_path):

        if not filename.endswith(".json"):
            continue

        file_path = os.path.join(source_path, filename)

        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        batch_id = filename.replace(".json", "")

        for row in data:

            event = {
                "event_id": str(uuid.uuid4()),
                "source": source,
                "batch_id": batch_id,
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