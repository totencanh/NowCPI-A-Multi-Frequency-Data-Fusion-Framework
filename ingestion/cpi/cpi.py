import requests
import pandas as pd
import io
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common import first_available, make_event, write_events

def fetch_cpi_imf(start_period="2015-M01"):
    url = (
        "https://api.imf.org/external/sdmx/3.0/data/"
        "dataflow/IMF.STA/CPI/~/VNM.CPI._T.IX.M"
        f"?c[TIME_PERIOD]=ge:{start_period}"
    )
    response = requests.get(url, headers={"Accept": "text/csv"}, timeout=30)
    response.raise_for_status()
    return pd.read_csv(io.StringIO(response.text))

if __name__ == "__main__":
    df = fetch_cpi_imf()

    # Hiện tất cả cột, không cắt bớt
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    print("[IMF API] Các cột có sẵn:")
    print(df.columns.tolist())

    # Chỉ hiển thị một số cột để xem gọn; file JSON bên dưới vẫn lưu toàn bộ cột.
    important_cols = [c for c in ["COUNTRY", "TIME_PERIOD", "OBS_VALUE"] if c in df.columns]
    print("\n--- Dữ liệu CPI (cột quan trọng) ---")
    print(df[important_cols].head(20))

    events = []
    for row in df.to_dict(orient="records"):
        source_period = row.get("TIME_PERIOD")
        period_text = str(source_period) if source_period is not None else ""
        match = re.search(r"(\d{4})-?M(\d{1,2})", period_text, re.IGNORECASE)
        observation_period = (
            f"{match.group(1)}-{int(match.group(2)):02d}" if match else period_text
        )
        value = pd.to_numeric(row.get("OBS_VALUE"), errors="coerce")
        events.append(make_event(
            source="imf",
            series_id="cpi_headline",
            observation_period=observation_period,
            frequency="monthly",
            value=value,
            unit=first_available(row.get("UNIT"), row.get("UNIT_MEASURE"), default="index"),
            release_ts=first_available(row.get("RELEASE_TS"), row.get("RELEASE_DATE")),
            source_record_id=f"cpi_headline:{observation_period}",
            raw_payload=row,
        ))

    output_path = write_events("imf_cpi_vietnam.json", events)
    print(f"\nĐã lưu {len(events)} event vào {output_path}")
