"""Publish newly-created NowCPI JSON batches to their Kafka topics."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, Producer, TopicPartition

try:
    from spark.common.schemas import validate_event
except ImportError:
    # Local runs do not inherit the Airflow container's /opt/spark/app path.
    project_processing_path = Path(__file__).resolve().parents[2] / "processing"
    if str(project_processing_path) not in sys.path:
        sys.path.insert(0, str(project_processing_path))
    from spark.common.schemas import validate_event

try:
    from spark.common.prometheus_metrics import push_stage_metrics
except ImportError:  # Allows the publisher to run without the optional monitoring path.
    push_stage_metrics = None

try:
    import fcntl
except ImportError:  # Windows local runs do not need inter-process locking.
    fcntl = None


SOURCE_TOPICS = {
    "imf_cpi_vietnam": "nowcpi.cpi",
    "imf_cpi_components_vietnam": "nowcpi.cpi_components",
    "nso_iip_vietnam": "nowcpi.ppi_iip",
    "brent_oil_daily": "nowcpi.brent_oil",
    "usd_vnd_daily": "nowcpi.usd_vnd",
    "vn_fuel_e10_ron95": "nowcpi.vn_fuel",
}

RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "/app/input"))
STATE_FILE = Path(
    os.getenv("KAFKA_PUBLISHED_STATE", "/app/state/published_batches.json")
)
BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
PIPELINE_AUDIT_DIR = Path(os.getenv("PIPELINE_AUDIT_DIR", "/opt/airflow/audit"))


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


def enrich_event(event: dict[str, Any]) -> dict[str, Any]:
    """Add stable transport metadata without changing the raw JSON archive."""
    enriched = dict(event)
    # Observation period is the economic reference date, not when the source
    # released or NowCPI received the record. Never use it as event time.
    event_time = event.get("release_ts") or event.get("event_time") or event.get("ingested_at")
    if event_time:
        enriched["event_time"] = event_time
        enriched["event_time_basis"] = (
            "source_release_ts" if event.get("release_ts")
            else "source_event_time" if event.get("event_time")
            else "source_ingested_at"
        )
    else:
        enriched["event_time"] = None
        enriched["event_time_basis"] = "unavailable"
    # Distinguish this event-time contract from legacy Bronze rows, where
    # event_time may have been derived from the observation period.
    enriched["event_time_contract_version"] = 2

    business_key_fields = {
        "source": event.get("source"),
        "country": event.get("country"),
        "series_id": event.get("series_id"),
        "frequency": event.get("frequency"),
        "observation_period": event.get("observation_period"),
        "unit": event.get("unit"),
    }
    enriched.setdefault(
        "business_key",
        json.dumps(business_key_fields, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
    )

    # Exclude the transport/check timestamp so identical observations hash the
    # same even when a collector revisits an unchanged source record.
    payload_for_hash = {key: value for key, value in event.items() if key != "ingested_at"}
    canonical_payload = json.dumps(
        payload_for_hash,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=True,
    ).encode("utf-8")
    enriched.setdefault("payload_hash", hashlib.sha256(canonical_payload).hexdigest())
    return enriched


def publish_batch(
    producer: Producer,
    path: Path,
    topic: str,
) -> dict[str, Any]:
    events = read_events(path)
    invalid = []
    valid_events: list[dict[str, Any]] = []
    for index, event in enumerate(events):
        issues = validate_event(event, topic)
        if issues:
            invalid.append((index, event, issues))
        else:
            valid_events.append(event)
    append_contract_quarantine(path, topic, invalid)
    delivery_errors: list[str] = []
    delivery_latencies: list[float] = []

    def delivered(error: Any, message: Any, enqueued_at: float) -> None:
        if error is not None:
            delivery_errors.append(str(error))
        else:
            delivery_latencies.append(max(0.0, time.monotonic() - enqueued_at))

    for event in valid_events:
        event_id = event.get("event_id")
        if not event_id:
            raise ValueError(f"Missing stable event_id in {path}")

        # Enrich only the Kafka copy; the immutable raw batch remains untouched.
        kafka_event = enrich_event(event)
        value = json.dumps(
            kafka_event,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        key = str(event_id).encode("utf-8")

        while True:
            try:
                enqueued_at = time.monotonic()
                producer.produce(
                    topic,
                    key=key,
                    value=value,
                    callback=lambda error, message, queued_at=enqueued_at: delivered(
                        error, message, queued_at
                    ),
                )
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
    ordered_latencies = sorted(delivery_latencies)
    p95_position = (len(ordered_latencies) - 1) * 0.95 if ordered_latencies else None
    if p95_position is None:
        p95_latency = None
    else:
        lower = int(p95_position)
        upper = min(lower + 1, len(ordered_latencies) - 1)
        p95_latency = round(
            ordered_latencies[lower]
            + (ordered_latencies[upper] - ordered_latencies[lower])
            * (p95_position - lower),
            6,
        )
    return {
        "attempted_events": len(events),
        "delivered_events": len(delivery_latencies),
        "rejected_events": len(invalid),
        "_contract_rejections": invalid,
        "p95_delivery_latency_seconds": p95_latency,
        "delivery_latency_samples": len(delivery_latencies),
        "_delivery_latencies": delivery_latencies,
    }


def append_contract_quarantine(
    path: Path,
    topic: str,
    invalid: list[tuple[int, dict[str, Any], list[str]]],
) -> None:
    """Record invalid raw-row references and reasons without copying payloads."""
    if not invalid:
        return
    quarantine_path = PIPELINE_AUDIT_DIR / "source_contract_quarantine.jsonl"
    quarantine_path.parent.mkdir(parents=True, exist_ok=True)
    with quarantine_path.open("a", encoding="utf-8") as quarantine_file:
        if fcntl is not None:
            fcntl.flock(quarantine_file.fileno(), fcntl.LOCK_EX)
        try:
            for index, event, issues in invalid:
                payload = json.dumps(
                    event, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"), allow_nan=True,
                ).encode("utf-8")
                record = {
                    "source": event.get("source"),
                    "topic": topic,
                    "batch_path": path.name,
                    "record_index": index,
                    "event_id": event.get("event_id"),
                    "payload_hash": hashlib.sha256(payload).hexdigest(),
                    "reason_codes": issues,
                    "quarantined_at": datetime.now(timezone.utc).isoformat(),
                }
                quarantine_file.write(
                    json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
                )
            quarantine_file.flush()
            os.fsync(quarantine_file.fileno())
        finally:
            if fcntl is not None:
                fcntl.flock(quarantine_file.fileno(), fcntl.LOCK_UN)


def append_publish_audit(record: dict[str, Any]) -> None:
    """Persist successful per-DAG Kafka publisher measurements for the final report."""
    path = PIPELINE_AUDIT_DIR / "kafka_publish_runs.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as audit_file:
        if fcntl is not None:
            fcntl.flock(audit_file.fileno(), fcntl.LOCK_EX)
        try:
            audit_file.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
            audit_file.flush()
            os.fsync(audit_file.fileno())
        finally:
            if fcntl is not None:
                fcntl.flock(audit_file.fileno(), fcntl.LOCK_UN)


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower), 6)


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
    started_at = datetime.now(timezone.utc)
    started_clock = time.monotonic()
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
    attempted_events = 0
    rejected_events = 0
    processed_batches = 0
    published_batches = 0
    delivery_latencies: list[float] = []
    topic_metrics: dict[str, dict[str, int]] = {}
    batches = discover_batches(args.source)
    offsets = topic_offsets(
        BROKER, {topic for _, topic in batches}
    )
    previous_watermarks = state.get("_topic_low_watermarks", {})
    previously_published_topics = {
        batch_state.get("topic")
        for batch_path, batch_state in state.items()
        if batch_path != "_topic_low_watermarks"
        and isinstance(batch_state, dict)
        and int(batch_state.get("event_count", 0) or 0) > 0
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

        batch_metrics = publish_batch(producer, path, topic)
        count = int(batch_metrics["delivered_events"])
        rejected_count = int(batch_metrics["rejected_events"])
        attempted_events += int(batch_metrics["attempted_events"])
        rejected_events += rejected_count
        processed_batches += 1
        topic_metrics.setdefault(topic, {
            "attempted_events": 0, "delivered_events": 0, "rejected_events": 0,
        })
        topic_metrics[topic]["attempted_events"] += int(batch_metrics["attempted_events"])
        topic_metrics[topic]["delivered_events"] += count
        topic_metrics[topic]["rejected_events"] += rejected_count
        delivery_latencies.extend(batch_metrics.pop("_delivery_latencies"))
        state[relative_path] = {
            "sha256": checksum,
            "event_count": count,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "topic": topic,
        }
        if count:
            state[relative_path]["published_at"] = state[relative_path]["processed_at"]
        save_state(state)
        total_events += count
        if count:
            published_batches += 1
        print(
            f"Batch {relative_path}: delivered={count}, contract_rejected={rejected_count}, "
            f"topic={topic}",
            flush=True,
        )

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
    completed_at = datetime.now(timezone.utc)
    elapsed_seconds = max(time.monotonic() - started_clock, 0.0)
    publish_audit = {
        "dag_id": os.getenv("AIRFLOW_CTX_DAG_ID"),
        "dag_run_id": os.getenv("AIRFLOW_CTX_DAG_RUN_ID"),
        "task_id": os.getenv("AIRFLOW_CTX_TASK_ID", "publish_to_kafka"),
        "started_at": started_at.isoformat().replace("+00:00", "Z"),
        "completed_at": completed_at.isoformat().replace("+00:00", "Z"),
        "duration_seconds": round(elapsed_seconds, 3),
        "batches_discovered": len(batches),
        "batches_processed": processed_batches,
        "batches_published": published_batches,
        "attempted_events": attempted_events,
        "delivered_events": total_events,
        "rejected_events": rejected_events,
        "delivery_failures": 0,
        "acceptance_rate_percent": round(100 * total_events / attempted_events, 2)
        if attempted_events else None,
        "throughput_events_per_second": round(total_events / elapsed_seconds, 3)
        if elapsed_seconds > 0 else None,
        "p95_delivery_latency_seconds": percentile(delivery_latencies, 0.95),
        "delivery_latency_samples": len(delivery_latencies),
        "topic_metrics": topic_metrics,
        "status": "degraded" if rejected_events else "success",
    }
    append_publish_audit(publish_audit)
    if push_stage_metrics is not None:
        push_stage_metrics(
            component="kafka_publisher",
            stage="kafka",
            status=publish_audit["status"],
            values={
                "attempted_events": attempted_events,
                "delivered_events": total_events,
                "rejected_events": rejected_events,
                "acceptance_rate_percent": publish_audit["acceptance_rate_percent"],
                "throughput_records_per_second": publish_audit["throughput_events_per_second"],
                "p95_delivery_latency_seconds": publish_audit["p95_delivery_latency_seconds"],
                "delivery_latency_samples": len(delivery_latencies),
                "stage_duration_seconds": publish_audit["duration_seconds"],
            },
        )


if __name__ == "__main__":
    main()
