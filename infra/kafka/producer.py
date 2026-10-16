"""Publish newly-created NowCPI JSON batches to their Kafka topics."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, Producer, TopicPartition


SOURCE_TOPICS = {
    "imf_cpi_vietnam": "nowcpi.cpi",
    "imf_cpi_components_vietnam": "nowcpi.cpi_components",
    "worldbank_iip_vietnam": "nowcpi.ppi_iip",
    "brent_oil_daily": "nowcpi.brent_oil",
    "usd_vnd_daily": "nowcpi.usd_vnd",
    "vn_fuel_e10_ron95": "nowcpi.vn_fuel",
}

RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "/app/input"))
STATE_FILE = Path(
    os.getenv("KAFKA_PUBLISHED_STATE", "/app/state/published_batches.json")
)
BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        choices=sorted(SOURCE_TOPICS),
        help="Publish batches for one source only.",
    )
    parser.add_argument(
        "--force-replay",
        action="store_true",
        help="Republish selected batches even when their checksum is in the state file.",
    )
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_state() -> dict[str, dict[str, str]]:
    try:
        value = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except FileNotFoundError:
        return {}


def save_state(state: dict[str, dict[str, str]]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=STATE_FILE.parent,
        prefix=f"{STATE_FILE.name}.",
        suffix=".tmp",
        delete=False,
    ) as temporary:
        json.dump(state, temporary, ensure_ascii=False, indent=2, sort_keys=True)
        temporary_path = Path(temporary.name)
    temporary_path.replace(STATE_FILE)


def discover_batches(source_filter: str | None = None) -> list[tuple[Path, str]]:
    """Find JSON snapshots/batches only for the known NowCPI source folders."""
    batches: list[tuple[Path, str]] = []
    for source_name, topic in SOURCE_TOPICS.items():
        if source_filter and source_name != source_filter:
            continue
        source_dir = RAW_DATA_DIR / source_name
        if source_dir.is_dir():
            batches.extend(
                (path, topic)
                for path in sorted(source_dir.glob("*.json"))
                if path.is_file() and path.stat().st_size > 2
            )

        # Keep compatibility with legacy flat JSON files during migration.
        legacy_file = RAW_DATA_DIR / f"{source_name}.json"
        if legacy_file.is_file() and legacy_file.stat().st_size > 2:
            batches.append((legacy_file, topic))

    return sorted(batches, key=lambda item: str(item[0]).lower())


def read_events(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    events = payload if isinstance(payload, list) else [payload]
    if not all(isinstance(event, dict) for event in events):
        raise ValueError(f"Expected a JSON object or list of objects in {path}")
    return events


def publish_batch(
    producer: Producer,
    path: Path,
    topic: str,
) -> int:
    events = read_events(path)
    delivery_errors: list[str] = []

    def delivered(error: Any, message: Any) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    for event in events:
        event_id = event.get("event_id")
        if not event_id:
            raise ValueError(f"Missing stable event_id in {path}")

        # Send the source event envelope unchanged; event_id is also the Kafka key.
        value = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        key = str(event_id).encode("utf-8")

        while True:
            try:
                producer.produce(topic, key=key, value=value, callback=delivered)
                producer.poll(0)
                break
            except BufferError:
                producer.poll(1)

    outstanding = producer.flush(120)
    if outstanding:
        delivery_errors.append(f"{outstanding} message(s) were not delivered before timeout")
    if delivery_errors:
        raise RuntimeError(
            f"Kafka delivery failed for {path}: " + "; ".join(delivery_errors[:5])
        )
    return len(events)


def topic_offsets(broker: str, topics: set[str]) -> dict[str, tuple[int, int]]:
    """Read topic offset ranges to detect broker data loss or empty topics.

    A topic may retain a few recent messages after older records expire. Merely
    checking whether a topic is empty would then miss the lost historical
    batches, even though the raw JSON and publisher manifest still exist.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "nowcpi-publisher-topic-probe",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    offsets: dict[str, tuple[int, int]] = {}
    try:
        metadata = consumer.list_topics(timeout=15)
        for topic in topics:
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None or topic_metadata.error is not None:
                offsets[topic] = (0, 0)
                continue

            topic_ranges: list[tuple[int, int]] = []
            for partition_id, partition_metadata in topic_metadata.partitions.items():
                if partition_metadata.error is not None:
                    continue
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition_id), timeout=15, cached=False
                )
                topic_ranges.append((low, high))
            offsets[topic] = (
                min((low for low, _ in topic_ranges), default=0),
                max((high for _, high in topic_ranges), default=0),
            )
    finally:
        consumer.close()
    return offsets


def main() -> None:
    args = parse_args()
    state = load_state()
    producer = Producer(
        {
            "bootstrap.servers": BROKER,
            "client.id": "nowcpi-json-batch-publisher",
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 120000,
        }
    )

    total_events = 0
    published_batches = 0
    batches = discover_batches(args.source)
    offsets = topic_offsets(
        BROKER, {topic for _, topic in batches}
    )
    previous_watermarks = state.get("_topic_low_watermarks", {})
    previously_published_topics = {
        batch_state.get("topic")
        for batch_path, batch_state in state.items()
        if batch_path != "_topic_low_watermarks" and isinstance(batch_state, dict)
    }
    expected_published_counts: dict[str, int] = {}
    for path, topic in batches:
        relative_path = path.relative_to(RAW_DATA_DIR).as_posix()
        batch_state = state.get(relative_path, {})
        if batch_state.get("topic") != topic:
            continue
        checksum = sha256_file(path)
        if batch_state.get("sha256") != checksum:
            continue
        expected_count = batch_state.get("event_count")
        if expected_count is None:
            expected_count = len(read_events(path))
        expected_published_counts[topic] = (
            expected_published_counts.get(topic, 0) + int(expected_count)
        )
    replay_topics = {
        topic
        for topic, (low, high) in offsets.items()
        if low > int(previous_watermarks.get(topic, 0))
        or (low > 0 and topic not in previous_watermarks)
        or (high <= low and topic in previously_published_topics)
        or high < expected_published_counts.get(topic, 0)
    }
    if replay_topics:
        print(
            "Detected expired Kafka offsets; replaying raw JSON batches for: "
            + ", ".join(sorted(replay_topics)),
            flush=True,
        )
    if args.force_replay:
        source_label = args.source or "all sources"
        print(f"Force replay enabled for {source_label}.", flush=True)

    for path, topic in batches:
        relative_path = path.relative_to(RAW_DATA_DIR).as_posix()
        checksum = sha256_file(path)
        if (
            not args.force_replay
            and topic not in replay_topics
            and state.get(relative_path, {}).get("sha256") == checksum
        ):
            continue

        count = publish_batch(producer, path, topic)
        state[relative_path] = {
            "sha256": checksum,
            "event_count": count,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "topic": topic,
        }
        save_state(state)
        total_events += count
        published_batches += 1
        print(f"Published {count} event(s) from {relative_path} to {topic}", flush=True)

    if published_batches == 0:
        if args.force_replay:
            print("No JSON batches found for the selected source.", flush=True)
        else:
            print("No new or changed JSON batches to publish.", flush=True)
    else:
        print(
            f"Published {total_events} event(s) from {published_batches} batch file(s).",
            flush=True,
        )
    state["_topic_low_watermarks"] = {
        topic: str(low) for topic, (low, _) in offsets.items()
    }
    save_state(state)


if __name__ == "__main__":
    main()
