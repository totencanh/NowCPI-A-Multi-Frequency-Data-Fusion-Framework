
import yfinance as yf
import pandas as pd
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


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
    usd_vnd = fetch_usd_vnd()

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

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    output_path = RAW_DATA_DIR / "usd_vnd_daily.json"
    output_path.write_text(
        json.dumps(events, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )

    print(f"\nĐã lưu {len(events)} event vào: {output_path}")

