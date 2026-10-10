"""Wait until Bronze Delta has consumed the current Kafka end offsets."""

from __future__ import annotations

import os
import time
import uuid

from confluent_kafka import Consumer, TopicPartition
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.silver.common import TOPIC_TABLES, create_spark


def kafka_end_offsets(consumer: Consumer) -> dict[tuple[str, int], int]:
    """Return the last currently available offset for each configured topic."""
    metadata = consumer.list_topics(timeout=15)
    targets: dict[tuple[str, int], int] = {}
    for topic in TOPIC_TABLES:
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Kafka topic is unavailable: {topic}")
        for partition in topic_metadata.partitions:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=15, cached=False
            )
            if high > 0:
                targets[(topic, partition)] = high - 1
    return targets


def bronze_has_offsets(spark: SparkSession, bucket: str, targets) -> bool:
    """Check whether Bronze has persisted every targeted Kafka offset."""
    path = f"s3a://{bucket}/bronze/kafka_events"
    log = spark._jvm.org.apache.hadoop.fs.Path(f"{path}/_delta_log")
    if not log.getFileSystem(spark._jsc.hadoopConfiguration()).exists(log):
        return not targets

    expected = set(TOPIC_TABLES)
    actual = {
        (row.kafka_topic, row.kafka_partition): row.max_offset
        for row in (
            spark.read.format("delta")
            .load(path)
            .filter(F.col("kafka_topic").isin(*expected))
            .groupBy("kafka_topic", "kafka_partition")
            .max("kafka_offset")
            .withColumnRenamed("max(kafka_offset)", "max_offset")
            .collect()
        )
        if row.kafka_topic in expected
    }
    return all(actual.get(key, -1) >= offset for key, offset in targets.items())


def main() -> None:
    timeout_seconds = int(os.getenv("BRONZE_WAIT_TIMEOUT_SECONDS", "1800"))
    poll_seconds = int(os.getenv("BRONZE_WAIT_POLL_SECONDS", "10"))
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BROKER", "kafka:9092"),
            "group.id": f"nowcpi-bronze-barrier-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    spark, bucket = create_spark("NowCPI-Bronze-Barrier")
    try:
        targets = kafka_end_offsets(consumer)
        if not targets:
            print("Kafka has no available source offsets; Bronze barrier is clear.", flush=True)
            return

        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if bronze_has_offsets(spark, bucket, targets):
                print(
                    f"Bronze has caught up to {len(targets)} Kafka partition end offset(s).",
                    flush=True,
                )
                return
            print(
                f"Bronze has not caught up yet; checking again in {poll_seconds}s.",
                flush=True,
            )
            time.sleep(poll_seconds)
        raise TimeoutError(
            f"Bronze did not reach Kafka's current end offsets within {timeout_seconds}s."
        )
    finally:
        consumer.close()
        spark.stop()


if __name__ == "__main__":
    main()
