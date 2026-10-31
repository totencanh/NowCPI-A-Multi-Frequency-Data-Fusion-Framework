"""Collect Vietnam's monthly industrial production index from the NSO NSDP."""

from __future__ import annotations

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common import make_event, write_events


NSDP_IIP_URL = "https://nsdp.nso.gov.vn/GSO-chung/SDMXFiles/GSO/IIPVNM.xml"
SOURCE = "national_statistics_office_vietnam"
RAW_FILE = "nso_iip_vietnam.json"


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].split(":")[-1].lower()


def _attributes(element: ET.Element) -> dict[str, str]:
    return {str(key).lstrip("@").upper(): value for key, value in element.attrib.items()}


def parse_month(value: str) -> str:
    """Normalize common NSDP SDMX monthly period forms to YYYY-MM."""
    text = value.strip()
    match = re.fullmatch(r"(\d{4})[-/]?M(\d{1,2})", text, flags=re.IGNORECASE)
    if match:
        year, month = map(int, match.groups())
    else:
        match = re.fullmatch(r"(\d{4})-(\d{1,2})", text)
        if not match:
            raise ValueError(f"NSDP IIP period is not monthly: {value!r}")
        year, month = map(int, match.groups())
    if not 1 <= month <= 12:
        raise ValueError(f"Invalid month in NSDP IIP period: {value!r}")
    return f"{year:04d}-{month:02d}"


def parse_nso_iip(xml_text: str) -> list[dict[str, object]]:
    """Read the aggregate Industry series from NSDP's structure-specific SDMX.

    NSDP's IIP file contains five ordered series; the first is the aggregate
    Industry series, followed by sector series. We select that aggregate and
    validate that its observations are monthly before creating events.
    """
    root = ET.fromstring(xml_text)
    series_nodes = [node for node in root.iter() if _local_name(node.tag) == "series"]
    if not series_nodes:
        raise ValueError("NSDP IIP response has no SDMX Series elements")

    selected = series_nodes[0]
    dimension_metadata = _attributes(selected)
    observations: dict[str, tuple[float, dict[str, str]]] = {}
    for node in selected.iter():
        if _local_name(node.tag) != "obs":
            continue
        attributes = _attributes(node)
        period = attributes.get("TIME_PERIOD")
        raw_value = attributes.get("OBS_VALUE")
        if period is None or raw_value in (None, "", "NaN", "nan"):
            continue
        month = parse_month(period)
        value = float(raw_value)
        if value <= 0:
            raise ValueError(f"Non-positive NSO IIP index at {month}: {value}")
        observations[month] = (value, attributes)

    if len(observations) < 24:
        raise ValueError(
            "NSDP IIP aggregate series returned fewer than 24 monthly observations; "
            "refusing to publish a partial or unexpected series."
        )

    records = []
    for month, (value, observation_attributes) in sorted(observations.items()):
        records.append({
            "observation_period": month,
            "value": value,
            "series_dimensions": dimension_metadata,
            "observation_attributes": observation_attributes,
        })
    return records


def fetch_nso_iip() -> list[dict[str, object]]:
    response = requests.get(
        NSDP_IIP_URL,
        headers={"User-Agent": "NowCPI/1.0 (research; Vietnam IIP)"},
        timeout=45,
    )
    response.raise_for_status()
    response.encoding = response.apparent_encoding or "utf-8"
    return parse_nso_iip(response.text)


def build_events(records: list[dict[str, object]]) -> list[dict[str, object]]:
    events = []
    for record in records:
        period = str(record["observation_period"])
        events.append(make_event(
            source=SOURCE,
            series_id="iip_total_index",
            observation_period=period,
            frequency="monthly",
            value=record["value"],
            unit="index",
            source_record_id=f"nso_iip_total_index:{period}",
            # NSDP's current SDMX series does not expose per-observation
            # release dates or historical vintages. Keep this null and use
            # actual ingestion/Kafka availability as the PIT boundary.
            release_ts=None,
            raw_payload={
                "source_url": NSDP_IIP_URL,
                "source_dataset": "NSDP IIPVNM",
                "country_code": "VNM",
                "series_selection": "first NSDP series (aggregate Industry)",
                "series_dimensions": record["series_dimensions"],
                "observation_attributes": record["observation_attributes"],
                "value_definition": "monthly industrial production index level",
            },
        ))
    return events


if __name__ == "__main__":
    source_records = fetch_nso_iip()
    events = build_events(source_records)
    output = write_events(RAW_FILE, events)
    print(f"NSO Vietnam monthly IIP: {len(events)} observations")
    print(f"Latest observation: {events[-1]['observation_period']} = {events[-1]['value']}")
    if output:
        print(f"Đã lưu batch: {output}")
    else:
        print("Không có thay đổi; không tạo batch mới.")
