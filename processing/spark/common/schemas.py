"""Versioned event contract shared by JSON ingestion, Kafka and Spark Bronze."""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any


SCHEMA_VERSION = 1
REQUIRED_EVENT_FIELDS = (
    "schema_version", "event_id", "source", "series_id", "country",
    "observation_period", "frequency", "value", "unit", "ingested_at",
    "source_record_id",
)

# Keep this as the one topic-to-domain contract. Raw payloads remain open-ended
# so source-specific columns are preserved instead of being rejected.
TOPIC_CONTRACTS: dict[str, dict[str, Any]] = {
    "nowcpi.cpi": {
        "sources": {"imf"}, "series_pattern": r"^cpi_headline$",
        "country": "VNM", "frequency": "monthly", "units": {"index"},
        "period_frequency": "month", "positive_value": True,
    },
    "nowcpi.cpi_components": {
        "sources": {"imf"}, "series_pattern": r"^cpi_component_[a-z0-9_]+$",
        "country": "VNM", "frequency": "monthly", "units": {"index"},
        "period_frequency": "month", "positive_value": True,
    },
    "nowcpi.ppi_iip": {
        "sources": {"national_statistics_office_vietnam"},
        "series_pattern": r"^iip_total_index$", "country": "VNM",
        "frequency": "monthly", "units": {"index"},
        "period_frequency": "month", "positive_value": True,
    },
    "nowcpi.brent_oil": {
        "sources": {"yahoo_finance"}, "series_pattern": r"^brent_front_month$",
        "country": "VNM", "frequency": "daily", "units": {"USD/barrel"},
        "period_frequency": "day", "positive_value": True,
    },
    "nowcpi.usd_vnd": {
        "sources": {"yahoo_finance", "frankfurter_v2"},
        "series_pattern": r"^usd_vnd$", "country": "VNM",
        "frequency": "daily", "units": {"VND/USD"},
        "period_frequency": "day", "positive_value": True,
    },
    "nowcpi.vn_fuel": {
        "sources": {"giaxanghomnay"},
        "series_pattern": r"^vn_fuel_(?:ron95|e10_ron95)$", "country": "VNM",
        "frequency": "daily", "units": {"VND/liter"},
        "period_frequency": "day", "positive_value": True,
    },
}


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            return datetime.combine(date.fromisoformat(text[:10]), datetime.min.time())
        except ValueError:
            return None


def _valid_period(value: Any, frequency: str) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip()
    if frequency == "monthly":
        match = re.fullmatch(r"(\d{4})-(\d{2})", text)
        return bool(match and 1 <= int(match.group(2)) <= 12)
    if frequency == "daily":
        return _parse_iso_datetime(text) is not None
    return False


def validate_event(event: dict[str, Any], topic: str) -> list[str]:
    """Return contract violations; an empty list means safe for that topic."""
    contract = TOPIC_CONTRACTS.get(topic)
    if contract is None:
        return [f"unknown_topic:{topic}"]

    issues = [f"missing:{key}" for key in REQUIRED_EVENT_FIELDS if key not in event]
    if issues:
        # Report missing required fields together; further checks are not useful.
        return issues

    version = event.get("schema_version")
    if isinstance(version, bool) or version != SCHEMA_VERSION:
        issues.append(f"unsupported_schema_version:{version}")
    for field in ("event_id", "source", "series_id", "country", "frequency", "unit", "source_record_id"):
        if not isinstance(event.get(field), str) or not event[field].strip():
            issues.append(f"invalid_or_empty:{field}")
    if isinstance(event.get("event_id"), str) and not re.fullmatch(
        r"[a-f0-9]{64}", event["event_id"]
    ):
        issues.append("event_id_must_be_sha256_hex")
    if not isinstance(event.get("source"), str) or event.get("source") not in contract["sources"]:
        issues.append("source_not_allowed_for_topic")
    if not isinstance(event.get("series_id"), str) or not re.fullmatch(
        contract["series_pattern"], event.get("series_id", "")
    ):
        issues.append("series_not_allowed_for_topic")
    if event.get("country") != contract["country"]:
        issues.append("country_not_allowed_for_topic")
    if event.get("frequency") != contract["frequency"]:
        issues.append("frequency_mismatch")
    if event.get("unit") not in contract["units"]:
        issues.append("unit_mismatch")
    if not _valid_period(event.get("observation_period"), contract["frequency"]):
        issues.append("invalid_observation_period")

    try:
        if isinstance(event.get("value"), bool):
            raise ValueError
        numeric_value = float(event.get("value"))
        if not math.isfinite(numeric_value):
            raise ValueError
        if contract.get("positive_value") and numeric_value <= 0:
            issues.append("value_must_be_positive")
    except (TypeError, ValueError):
        issues.append("value_must_be_finite_numeric")

    if _parse_iso_datetime(event.get("ingested_at")) is None:
        issues.append("invalid_ingested_at")
    if event.get("release_ts") is not None and _parse_iso_datetime(event.get("release_ts")) is None:
        issues.append("invalid_release_ts")
    if event.get("event_time") is not None and _parse_iso_datetime(event.get("event_time")) is None:
        issues.append("invalid_event_time")
    if event.get("raw_payload") is not None and not isinstance(event.get("raw_payload"), dict):
        issues.append("raw_payload_must_be_object_or_null")
    return sorted(set(issues))
