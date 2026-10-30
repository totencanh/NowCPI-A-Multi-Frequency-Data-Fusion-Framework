"""Summarize immutable raw event versions without changing source data."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


SOURCES = (
    "imf_cpi_vietnam",
    "imf_cpi_components_vietnam",
    "brent_oil_daily",
    "usd_vnd_daily",
    "vn_fuel_e10_ron95",
    "worldbank_iip_vietnam",
)


def _read_events(path: Path) -> list[dict]:
    try:
        content = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if isinstance(content, dict):
        return [content]
    return [row for row in content if isinstance(row, dict)] if isinstance(content, list) else []


def build_raw_batch_version_audit(raw_dir: Path) -> list[dict]:
    """Count distinct immutable event versions and true content revisions."""
    audit = []
    for source_name in SOURCES:
        flat_path = raw_dir / f"{source_name}.json"
        batch_dir = raw_dir / source_name
        batch_files = sorted(batch_dir.glob("*.json"))
        files = ([flat_path] if flat_path.is_file() else []) + batch_files
        signatures: dict[str, set[str]] = {}
        event_version_count = 0
        release_ts_count = 0
        ingested_at_values: list[str] = []

        for path in files:
            for event in _read_events(path):
                event_id = event.get("event_id")
                if not event_id:
                    continue
                comparable = {key: value for key, value in event.items() if key != "ingested_at"}
                serialized = json.dumps(
                    comparable, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                signature = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
                signatures.setdefault(str(event_id), set()).add(signature)
                event_version_count += 1
                release_ts_count += int(bool(event.get("release_ts")))
                if event.get("ingested_at"):
                    ingested_at_values.append(str(event["ingested_at"]))

        revised_ids = sum(len(versions) > 1 for versions in signatures.values())
        audit.append(
            {
                "source": source_name,
                "immutable_batch_files": len(batch_files),
                "raw_files_including_legacy_baseline": len(files),
                "event_versions_stored": event_version_count,
                "unique_event_ids": len(signatures),
                "event_ids_with_content_revisions": revised_ids,
                "release_timestamped_event_versions": release_ts_count,
                "release_timestamp_coverage_pct": round(
                    release_ts_count / event_version_count * 100, 2
                ) if event_version_count else 0.0,
                "first_recorded_ingested_at": min(ingested_at_values) if ingested_at_values else None,
                "last_recorded_ingested_at": max(ingested_at_values) if ingested_at_values else None,
                "vintage_note": (
                    "Append-only versions are available only from the first stored raw record; "
                    "they do not recreate source vintages before collection began."
                ),
            }
        )
    return audit
