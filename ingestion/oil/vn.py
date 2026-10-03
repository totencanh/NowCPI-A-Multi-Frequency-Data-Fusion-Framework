import requests
import pandas as pd
import time
from pathlib import Path
from datetime import date, timedelta
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common import RAW_DATA_DIR, make_event, write_events


# ============================================================
# CONFIG
# ============================================================

BASE_URL = "https://giaxanghomnay.com/api/pvdate"

START_DATE = date(2026, 9, 1)
END_DATE = date.today()

TARGET_PRODUCTS = {
    "Xăng RON 95 Mức 5",       # tên cũ (trước khi chuyển sang E10)
    "Xăng E10 RON 95 Mức 5",   # tên mới
}
TARGET_PRODUCT = "Xăng RON 95 Mức 5 / E10 RON 95 Mức 5"

OUTPUT_FILE = RAW_DATA_DIR / "vn_fuel_e10_ron95.json"

# Để thấp nhưng không spam server
REQUEST_DELAY = 0.3

# Retry khi lỗi mạng/server
MAX_RETRIES = 3


# ============================================================
# FETCH
# ============================================================

def fetch_date(date_str):
    """
    Lấy toàn bộ JSON của một ngày.
    Sau đó sẽ filter sản phẩm cần lấy.
    """

    url = f"{BASE_URL}/{date_str}"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = requests.get(
                url,
                headers=headers,
                timeout=30
            )

            response.raise_for_status()

            return response.json()

        except requests.RequestException as e:

            print(
                f"[ERROR] {date_str} "
                f"(attempt {attempt}/{MAX_RETRIES}): {e}"
            )

            if attempt < MAX_RETRIES:

                # Exponential backoff
                wait_time = 2 ** attempt

                print(
                    f"Waiting {wait_time}s before retry..."
                )

                time.sleep(wait_time)

    return None


# ============================================================
# FILTER PRODUCT
# ============================================================

def extract_target_product(data, date_str):
    """
    Chỉ lấy TARGET_PRODUCT.
    """

    rows = []

    if not isinstance(data, list):
        return rows

    for group in data:

        if not isinstance(group, list):
            continue

        for item in group:

            if not isinstance(item, dict):
                continue

            if item.get("title") not in TARGET_PRODUCTS:
                continue

            # Bỏ nhóm "reference" (không có zone1_price)
            if "zone1_price" not in item:
                continue

            # Một response chứa nhiều ngày, nên lấy ngày thật từ item
            rows.append({
                **item,
                "source_date": str(item.get("date", date_str))[:10],
                "price": item["zone1_price"],
            })

    return rows


# ============================================================
# LOAD EXISTING JSON (with legacy CSV fallback)
# ============================================================

def load_existing_data():
    """
    Load JSON hiện có để resume; hỗ trợ đọc CSV cũ nếu còn.
    """

    if OUTPUT_FILE.exists():

        try:

            df = pd.read_json(OUTPUT_FILE, orient="records")

            print(
                f"Existing data: {len(df):,} records"
            )

            return df

        except Exception as e:

            print(
                f"[WARNING] Cannot read existing JSON: {e}"
            )

    legacy_csv = OUTPUT_FILE.with_suffix(".csv")
    if legacy_csv.exists():
        try:
            df = pd.read_csv(legacy_csv, encoding="utf-8-sig")
            print(f"Loaded legacy CSV for migration: {len(df):,} records")
            return df
        except Exception as e:
            print(f"[WARNING] Cannot read legacy CSV: {e}")

    return pd.DataFrame()


# ============================================================
# MAIN
# ============================================================

def main():

    # --------------------------------------------------------
    # Load existing data
    # --------------------------------------------------------

    existing_df = load_existing_data()

    if not existing_df.empty:
        date_column = "observation_period" if "observation_period" in existing_df.columns else "source_date"
        completed_dates = set(existing_df[date_column].astype(str))

    else:

        completed_dates = set()

    # --------------------------------------------------------
    # Statistics
    # --------------------------------------------------------

    total_days = (
        END_DATE - START_DATE
    ).days + 1

    current_date = START_DATE

    new_rows = []

    processed = 0
    skipped = 0

    print("=" * 70)
    print("GIAXANGHOMNAY COLLECTOR")
    print("=" * 70)

    print(f"Product : {TARGET_PRODUCT}")
    print(f"Start   : {START_DATE}")
    print(f"End     : {END_DATE}")
    print(f"Days    : {total_days}")

    # ========================================================
    # LOOP
    # ========================================================

    while current_date <= END_DATE:

        date_str = current_date.strftime("%Y-%m-%d")

        # ----------------------------------------------------
        # Skip if already collected
        # ----------------------------------------------------

        if date_str in completed_dates:

            skipped += 1

            current_date += timedelta(days=1)

            continue

        processed += 1

        print(
            f"\n[{processed}] Fetching {date_str}..."
        )

        # ----------------------------------------------------
        # Fetch API
        # ----------------------------------------------------

        data = fetch_date(date_str)

        if data is None:

            print(
                f"[FAILED] {date_str}"
            )

            current_date += timedelta(days=1)

            continue

        # ----------------------------------------------------
        # Extract target product
        # ----------------------------------------------------

        rows = extract_target_product(
            data,
            date_str
        )

        if rows:

            new_rows.extend(rows)

            print(
                f"[OK] {date_str}: "
                f"{len(rows)} target record(s)"
            )

        else:

            print(
                f"[NO DATA] "
                f"{TARGET_PRODUCT}"
            )

        # ----------------------------------------------------
        # Delay
        # ----------------------------------------------------

        time.sleep(
            REQUEST_DELAY
        )

        current_date += timedelta(days=1)

    if not existing_df.empty:
        existing_records = existing_df.to_dict(orient="records")
    else:
        existing_records = []

    events = []
    for record in existing_records:
        if record.get("schema_version") == 1:
            events.append(record)
            continue
        # Upgrade records from the previous flat JSON/CSV layout.
        period = str(record.get("source_date") or record.get("date") or "")
        source_id = record.get("id") or record.get("date") or period
        events.append(make_event(
            source="giaxanghomnay",
            series_id="vn_fuel_e10_ron95",
            observation_period=period,
            frequency="daily",
            value=pd.to_numeric(record.get("price"), errors="coerce"),
            unit="VND/liter",
            source_record_id=f"vn_fuel_e10_ron95:{source_id}",
            raw_payload=record,
        ))
    for row in new_rows:
        period = str(row.get("source_date"))
        source_id = row.get("id") or row.get("date") or period
        events.append(make_event(
            source="giaxanghomnay",
            series_id="vn_fuel_e10_ron95",
            observation_period=period,
            frequency="daily",
            value=pd.to_numeric(row.get("price"), errors="coerce"),
            unit="VND/liter",
            source_record_id=f"vn_fuel_e10_ron95:{source_id}",
            raw_payload=row,
        ))

    # Idempotent output: retain one event for each stable event_id.
    events_by_id = {event["event_id"]: event for event in events}
    events = sorted(events_by_id.values(), key=lambda event: event["observation_period"] or "")
    output_path = write_events(OUTPUT_FILE.name, events)

    if not events:
        print("\nKhông có dữ liệu.")
        return

    # ========================================================
    # SUMMARY
    # ========================================================

    print("\n" + "=" * 70)
    print("DONE")
    print("=" * 70)

    print(
        f"Total records : {len(events):,}"
    )

    print(
        f"Unique dates  : {len({event['observation_period'] for event in events})}"
    )

    print(
        f"New records   : {len(new_rows):,}"
    )

    print(
        f"Skipped dates : "
        f"{skipped:,}"
    )

    print(
        f"\nJSON: {output_path}"
    )

    print("\nLatest data:")

    print(
        pd.DataFrame(events).tail(10).to_string(index=False)
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()

