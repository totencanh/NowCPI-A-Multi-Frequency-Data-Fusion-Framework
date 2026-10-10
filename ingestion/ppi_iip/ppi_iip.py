
import requests
import pandas as pd
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common import first_available, make_event, write_events

INDICATORS = {
    "IIP_Growth": "NV.IND.TOTL.KD.ZG",
}


def fetch_worldbank():
    all_records = []

    for name, code in INDICATORS.items():
        url = (
            f"https://api.worldbank.org/v2/country/VNM/"
            f"indicator/{code}?format=json&per_page=1000"
        )

        res = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=30
        )

        res.raise_for_status()
        data = res.json()

        if len(data) > 1 and data[1]:
            for item in data[1]:
                # Retain every field supplied by the World Bank API.
                all_records.append({
                    **item,
                    "collector_indicator": name,
                    "collector_indicator_code": code,
                })

    if not all_records:
        raise RuntimeError("Không tải được dữ liệu World Bank.")

    return pd.DataFrame(all_records).sort_values("date").reset_index(drop=True)


if __name__ == "__main__":
    df = fetch_worldbank()

    print("=" * 60)
    print("WORLD BANK - VIETNAM IIP GROWTH")
    print("=" * 60)

    print("\n--- Latest observations ---")
    print(df.tail(10))

    events = []
    for row in df.to_dict(orient="records"):
        code = first_available(row.get("collector_indicator_code"), default="unknown")
        series_id = "iip_growth"
        period = first_available(row.get("date"))
        events.append(make_event(
            source="world_bank",
            series_id=series_id,
            observation_period=period,
            frequency="annual",
            value=row.get("value"),
            unit=first_available(row.get("unit"), default="percent"),
            source_record_id=f"{code}:{period}",
            raw_payload=row,
        ))

    output_path = write_events("worldbank_iip_vietnam.json", events)

    if output_path:
        print(f"\nĐã lưu {len(events)} event vào: {output_path}")
    else:
        print("\nKhông có event mới hoặc thay đổi; không tạo file batch.")

