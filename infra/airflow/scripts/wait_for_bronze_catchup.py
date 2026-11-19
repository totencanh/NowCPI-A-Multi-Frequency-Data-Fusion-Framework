"""Wait for Bronze to persist Kafka end offsets without consuming Spark slots."""

from __future__ import annotations

import os
import time

from confluent_kafka import Consumer, TopicPartition
from trino.dbapi import connect


TOPICS = (
    "nowcpi.cpi",
    "nowcpi.cpi_components",
    "nowcpi.ppi_iip",
    "nowcpi.brent_oil",
    "nowcpi.usd_vnd",
    "nowcpi.vn_fuel",
)


def kafka_end_offsets(consumer: Consumer) -> dict[tuple[str, int], int]:
    """Read the inclusive last offset currently available per topic partition."""
    metadata = consumer.list_topics(timeout=15)
    targets: dict[tuple[str, int], int] = {}
    for topic in TOPICS:
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


def bronze_max_offsets() -> dict[tuple[str, int], int]:
    """Read accepted and quarantined Bronze offsets through Trino."""
    connection = connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "nowcpi"),
        catalog="delta",
        schema="bronze",
        http_scheme=os.getenv("TRINO_HTTP_SCHEME", "http"),
    )
    try:
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES FROM delta.bronze")
        bronze_tables = {str(row[0]) for row in cursor.fetchall()}
        if "kafka_events" not in bronze_tables:
            raise RuntimeError("Bronze receipt table delta.bronze.kafka_events is unavailable")

        persisted_tables = ["kafka_events"]
        if "kafka_events_quarantine" in bronze_tables:
            persisted_tables.append("kafka_events_quarantine")
        offset_selects = "\nUNION ALL\n".join(
            f"""SELECT kafka_topic, kafka_partition, kafka_offset
                FROM delta.bronze.{table}
                WHERE kafka_topic IN (
                    'nowcpi.cpi', 'nowcpi.cpi_components', 'nowcpi.ppi_iip',
                    'nowcpi.brent_oil', 'nowcpi.usd_vnd', 'nowcpi.vn_fuel'
                )"""
            for table in persisted_tables
        )
        cursor.execute(
            f"""SELECT kafka_topic, kafka_partition, max(kafka_offset) AS max_offset
                FROM ({offset_selects}) AS persisted_bronze_offsets
                GROUP BY kafka_topic, kafka_partition"""
        )
        return {
            (str(topic), int(partition)): int(offset)
            for topic, partition, offset in cursor.fetchall()
            if topic is not None and partition is not None and offset is not None
        }
    finally:
        connection.close()


def main() -> None:
    timeout_seconds = int(os.getenv("BRONZE_WAIT_TIMEOUT_SECONDS", "1800"))
    poll_seconds = max(1, int(os.getenv("BRONZE_WAIT_POLL_SECONDS", "10")))
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BROKER", "kafka:9092"),
            "group.id": "nowcpi-bronze-barrier-metadata-check",
            "enable.auto.commit": False,
        }
    )
    started = time.monotonic()
    deadline = started + timeout_seconds
    try:
        targets = kafka_end_offsets(consumer)
        if not targets:
            print("Kafka has no available source offsets; Bronze barrier is clear.", flush=True)
            return
        print(
            f"Waiting for Bronze offsets in {len(targets)} Kafka partition(s) via Trino; "
            f"timeout={timeout_seconds}s.",
            flush=True,
        )
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Bronze did not reach {len(targets)} Kafka partition end offsets "
                    f"within {timeout_seconds}s."
                )
            try:
                actual = bronze_max_offsets()
                pending = {
                    key: {"bronze": actual.get(key, -1), "kafka_target": target}
                    for key, target in targets.items()
                    if actual.get(key, -1) < target
                }
                if not pending:
                    print(
                        f"Bronze caught up to all {len(targets)} Kafka partition end "
                        f"offsets in {time.monotonic() - started:.1f}s.",
                        flush=True,
                    )
                    return
                print(
                    f"Persisted Bronze (accepted + quarantined) is behind in "
                    f"{len(pending)}/{len(targets)} partition(s): "
                    f"{pending}; retrying in {min(poll_seconds, max(1, int(remaining)))}s.",
                    flush=True,
                )
            except Exception as exc:
                print(
                    f"Could not read Bronze offsets from Trino yet: "
                    f"{type(exc).__name__}: {exc}; retrying.",
                    flush=True,
                )
            time.sleep(min(poll_seconds, max(0.1, deadline - time.monotonic())))
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
