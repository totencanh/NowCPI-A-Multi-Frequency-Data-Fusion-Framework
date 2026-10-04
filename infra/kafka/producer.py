"""Publish newly-created NowCPI JSON batches to their Kafka topics."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Producer


SOURCE_TOPICS = {
    "imf_cpi_vietnam": "nowcpi.cpi",
    "imf_cpi_components_vietnam": "nowcpi.cpi_components",
    "worldbank_ppi_iip_vietnam": "nowcpi.ppi_iip",
    "brent_oil_daily": "nowcpi.brent_oil",
    "usd_vnd_daily": "nowcpi.usd_vnd",
    "vn_fuel_e10_ron95": "nowcpi.vn_fuel",
}

RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "/app/input"))
STATE_FILE = Path(
    os.getenv("KAFKA_PUBLISHED_STATE", "/app/state/published_batches.json")
)
BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")


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


def discover_batches() -> list[tuple[Path, str]]:
    """Find JSON snapshots/batches only for the known NowCPI source folders."""
    batches: list[tuple[Path, str]] = []
    for source_name, topic in SOURCE_TOPICS.items():
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


def main() -> None:
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
    for path, topic in discover_batches():
        relative_path = path.relative_to(RAW_DATA_DIR).as_posix()
        checksum = sha256_file(path)
        if state.get(relative_path, {}).get("sha256") == checksum:
            continue

        count = publish_batch(producer, path, topic)
        state[relative_path] = {
            "sha256": checksum,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "topic": topic,
        }
        save_state(state)
        total_events += count
        published_batches += 1
        print(f"Published {count} event(s) from {relative_path} to {topic}", flush=True)

    if published_batches == 0:
        print("No new or changed JSON batches to publish.", flush=True)
    else:
        print(
            f"Published {total_events} event(s) from {published_batches} batch file(s).",
            flush=True,
        )


if __name__ == "__main__":
    main()
