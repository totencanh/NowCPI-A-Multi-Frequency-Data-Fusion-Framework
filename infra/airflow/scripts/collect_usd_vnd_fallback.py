"""Collect USD/VND reference rates from Frankfurter v2 after primary failure."""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import requests


def project_root() -> Path:
    airflow_root = Path("/opt/airflow")
    if (airflow_root / "ingestion" / "common.py").is_file():
        return airflow_root
    return Path(__file__).resolve().parents[3]


ROOT = project_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingestion import common as ingestion_common  # noqa: E402


API_URL = "https://api.frankfurter.dev/v2/rates"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-data-dir", type=Path, required=True)
    args = parser.parse_args()

    ingestion_common.RAW_DATA_DIR = args.raw_data_dir
    existing = ingestion_common.read_event_batches("usd_vnd_daily.json")
    latest_period = max(
        (
            str(event.get("observation_period") or "")[:10]
            for event in existing
            if event.get("observation_period")
        ),
        default="",
    )
    # Only fill the recent gap on fallback. The primary collector owns the long
    # history, while Frankfurter supplies an independent daily reference rate.
    start_date = (
        date.fromisoformat(latest_period)
        if latest_period
        else date.today() - timedelta(days=7)
    )
    end_date = date.today()
    if start_date > end_date:
        raise ValueError(f"Stored USD/VND period {start_date} is in the future")

    response = requests.get(
        API_URL,
        params={
            "base": "USD",
            "quotes": "VND",
            "from": start_date.isoformat(),
            "to": end_date.isoformat(),
            "expand": "providers",
        },
        timeout=(5, 30),
    )
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list):
        raise ValueError("Frankfurter v2 returned an unexpected response shape")

    events = []
    for row in rows:
        if (
            not isinstance(row, dict)
            or row.get("base") != "USD"
            or row.get("quote") != "VND"
        ):
            continue
        observation_period = str(row.get("date") or "")[:10]
        try:
            rate = float(row.get("rate"))
            date.fromisoformat(observation_period)
        except (TypeError, ValueError):
            continue
        if rate <= 0:
            continue
        events.append(
            ingestion_common.make_event(
                source="frankfurter_v2",
                series_id="usd_vnd",
                country="VNM",
                observation_period=observation_period,
                frequency="daily",
                value=rate,
                unit="VND/USD",
                source_record_id=f"usd_vnd:{observation_period}",
                raw_payload=row,
            )
        )

    if not events:
        raise RuntimeError(
            f"Frankfurter returned no USD/VND observations from {start_date} through {end_date}"
        )

    output_path = ingestion_common.write_events("usd_vnd_daily.json", events)
    changed_count = len(events) if output_path else 0
    print(
        f"Frankfurter v2 USD/VND: {len(events)} observations fetched; "
        f"{changed_count} changed; newest={max(event['observation_period'] for event in events)}; "
        f"batch={output_path or 'none'}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
