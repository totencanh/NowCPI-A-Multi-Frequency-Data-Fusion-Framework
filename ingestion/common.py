"""Shared event schema and output helpers for NowCPI ingestion."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def json_safe(value: Any) -> Any:
    """Convert pandas/numpy/date values and missing values into JSON-safe values."""
    if value is None:
        return None

    if hasattr(value, "item") and not isinstance(value, (str, bytes, dict, list, tuple)):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass

    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "isoformat") and not isinstance(value, str):
        try:
            return value.isoformat()
        except (TypeError, ValueError):
            pass
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]

    try:
        import pandas as pd

        missing = pd.isna(value)
        if not hasattr(missing, "__len__") and bool(missing):
            return None
    except (ImportError, TypeError, ValueError):
        pass

    return value


def first_available(*values: Any, default: Any = None) -> Any:
    """Return the first non-null, non-empty scalar from source aliases."""
    for value in values:
        safe_value = json_safe(value)
        if safe_value is not None and str(safe_value).strip().lower() not in {"", "nan", "<na>", "nat"}:
            return safe_value
    return default


def make_event(
    *,
    source: str,
    series_id: str,
    observation_period: Any,
    frequency: str,
    value: Any,
    unit: str | None,
    raw_payload: dict[str, Any],
    country: str = "VNM",
    release_ts: Any = None,
    source_record_id: str | None = None,
) -> dict[str, Any]:
    """Wrap one source observation in the common NowCPI event envelope."""
    period = json_safe(observation_period)
    period = str(period) if period is not None else None
    source_record_id = source_record_id or f"{series_id}:{period or 'unknown'}"
    identity = f"{source}|{source_record_id}"
    event_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()

    return {
        "schema_version": 1,
        "event_id": event_id,
        "source": source,
        "series_id": series_id,
        "country": country,
        "observation_period": period,
        "frequency": frequency,
        "value": json_safe(value),
        "unit": unit,
        "release_ts": json_safe(release_ts),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source_record_id": source_record_id,
        "raw_payload": json_safe(raw_payload),
    }


def write_events(filename: str, events: Iterable[dict[str, Any]]) -> Path:
    """Write a JSON array under NowCPI/data/raw, independent of cwd."""
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RAW_DATA_DIR / filename
    output_path.write_text(
        json.dumps(list(events), ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    return output_path


def raw_source_dir(source_name: str) -> Path:
    """Return a stable directory for verbatim source payloads."""
    path = RAW_DATA_DIR / "source_payloads" / source_name
    path.mkdir(parents=True, exist_ok=True)
    return path
