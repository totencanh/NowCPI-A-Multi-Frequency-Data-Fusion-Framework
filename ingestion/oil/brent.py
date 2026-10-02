
import yfinance as yf
import pandas as pd
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common import first_available, make_event, read_event_batches, write_events


def fetch_brent(start="2015-01-01"):
    """
    Fetch Brent Crude Oil Futures daily price.
    Ticker: BZ=F
    """
    brent = yf.download(
        "BZ=F",
        start=start,
        interval="1d",
        auto_adjust=False
    )

    return brent


if __name__ == "__main__":
    previous_events = read_event_batches("brent_oil_daily.json")
    latest_period = max(
        (str(event.get("observation_period") or "")[:10] for event in previous_events),
        default="",
    )
    # Re-fetch the latest observed day to capture source revisions, rather than
    # downloading the full 2015-to-present history on every scheduled run.
    brent = fetch_brent(start=latest_period or "2015-01-01")

    print("=" * 60)
    print("BRENT CRUDE OIL")
    print("=" * 60)

    print("\n--- Latest 10 observations ---")
    print(brent.tail(10))

    output = brent.copy()
    if isinstance(output.columns, pd.MultiIndex):
        output.columns = [column[0] for column in output.columns]
    output = output.reset_index()

    events = []
    for row in output.to_dict(orient="records"):
        observation_period = first_available(row.get("Date"), row.get("Datetime"))
        events.append(make_event(
            source="yahoo_finance",
            series_id="brent_front_month",
            observation_period=observation_period,
            frequency="daily",
            value=row.get("Close"),
            unit="USD/barrel",
            source_record_id=f"brent_front_month:{observation_period}",
            raw_payload=row,
        ))

    output_path = write_events("brent_oil_daily.json", events)

    if output_path:
        print(f"\nĐã lưu {len(events)} event vào: {output_path}")
    else:
        print("\nKhông có event mới hoặc thay đổi; không tạo file batch.")

