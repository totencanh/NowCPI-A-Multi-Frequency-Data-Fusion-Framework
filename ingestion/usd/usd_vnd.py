
import yfinance as yf
import pandas as pd
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common import read_event_batches, write_events


def fetch_usd_vnd(start="2015-01-01"):
    """
    Fetch USD/VND exchange rate daily.
    Ticker: VND=X
    Meaning: 1 USD = X VND
    """
    usd_vnd = yf.download(
        "VND=X",
        start=start,
        interval="1d",
        auto_adjust=False
    )

    return usd_vnd


if __name__ == "__main__":
    previous_events = read_event_batches("usd_vnd_daily.json")
    latest_period = max(
        (str(event.get("observation_period") or "")[:10] for event in previous_events),
        default="",
    )
    # Re-fetch the latest observed day to capture source revisions, rather than
    # downloading the full 2015-to-present history on every scheduled run.
    usd_vnd = fetch_usd_vnd(start=latest_period or "2015-01-01")

    print("=" * 60)
    print("USD/VND EXCHANGE RATE")
    print("=" * 60)

    print("\n--- Latest 10 observations ---")
    print(usd_vnd.tail(10))

    output = usd_vnd.copy()
    if isinstance(output.columns, pd.MultiIndex):
        output.columns = [column[0] for column in output.columns]
    output = output.reset_index()

    raw_records = json.loads(
        output.to_json(orient="records", force_ascii=False, date_format="iso")
    )
    events = []
    for row in raw_records:
        source_date = row.get("Date") or row.get("Datetime")
        observation_period = str(source_date)[:10] if source_date else None
        source_record_id = f"usd_vnd:{observation_period or 'unknown'}"
        event_id = hashlib.sha256(source_record_id.encode("utf-8")).hexdigest()
        events.append({
            "schema_version": 1,
            "event_id": event_id,
            "source": "yahoo_finance",
            "series_id": "usd_vnd",
            "country": "VNM",
            "observation_period": observation_period,
            "frequency": "daily",
            "value": row.get("Close"),
            "unit": "VND/USD",
            "release_ts": None,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "source_record_id": source_record_id,
            "raw_payload": row,
        })

    output_path = write_events("usd_vnd_daily.json", events)

    if output_path:
        print(f"\nĐã lưu {len(events)} event vào: {output_path}")
    else:
        print("\nKhông có event mới hoặc thay đổi; không tạo file batch.")

