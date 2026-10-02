
import requests
import pandas as pd
import time
from pathlib import Path
from datetime import date, timedelta
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common import RAW_DATA_DIR, make_event, read_event_batches, write_events


# ============================================================
# CONFIG
# ============================================================

BASE_URL = "https://giaxanghomnay.com/api/pvdate"

START_DATE = date(2026, 1, 1)
END_DATE = date.today()

OUTPUT_FILE = RAW_DATA_DIR / "vn_fuel_e10_ron95.json"

# Để thấp nhưng không spam server
REQUEST_DELAY = 0.5

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
    Lấy toàn bộ bản ghi sản phẩm của đúng ngày, không lọc riêng E10.
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

            # The API returns current and previous-day groups in one response.
            # Keep only the record whose source date matches the requested day.
            item_date = str(item.get("date") or "")[:10]
            if item_date and item_date != date_str:
                continue

            # Keep complete source product rows (including both zone prices).
            # The API's separate reference-price groups are derived subsets.
            if not (
                item.get("id") is not None
                or "zone1_price" in item
                or "zone2_price" in item
            ):
                continue

            # Keep the complete source object for the selected product only.
            rows.append({**item, "source_date": date_str})

    return rows


# ============================================================
# LOAD EXISTING JSON (with legacy CSV fallback)
# ============================================================

def load_existing_data():
    """
    Load legacy JSON and all timestamped batches to resume; supports legacy CSV.
    """

    existing_events = read_event_batches(OUTPUT_FILE.name)
    # Ignore the previous-day reference rows that older collector versions
    # accidentally labeled with the requested day.
    existing_events = [
        event
        for event in existing_events
        if not isinstance(event.get("raw_payload"), dict)
        or not event["raw_payload"].get("date")
        or str(event["raw_payload"]["date"])[:10]
        == str(event.get("observation_period") or "")[:10]
    ]
    if existing_events:
        df = pd.DataFrame(existing_events)
        print(f"Existing data: {len(df):,} records")
        return df

    if OUTPUT_FILE.exists():
        try:
            df = pd.read_json(OUTPUT_FILE, orient="records")
            print(f"Existing data: {len(df):,} records")
            return df
        except Exception as e:
            print(f"[WARNING] Cannot read existing JSON: {e}")

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
        if "series_id" in existing_df.columns:
            complete_records = existing_df[existing_df["series_id"] == "vn_fuel_price"]
            completed_dates = set(complete_records[date_column].astype(str))
        else:
            completed_dates = set()

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

    print("Products: all source fuel records for each date")
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
                f"No source fuel records for {date_str}"
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
            series_id="vn_fuel_price",
            observation_period=period,
            frequency="daily",
            value=pd.to_numeric(
                record.get("price")
                if record.get("price") is not None
                else record.get("zone1_price"),
                errors="coerce",
            ),
            unit="VND/liter",
            source_record_id=f"vn_fuel_price:{source_id}",
            raw_payload=record,
        ))
    for row in new_rows:
        period = str(row.get("source_date"))
        source_id = row.get("id") or f"{row.get('title') or 'fuel'}:{row.get('date') or period}"
        events.append(make_event(
            source="giaxanghomnay",
            series_id="vn_fuel_price",
            observation_period=period,
            frequency="daily",
            value=pd.to_numeric(
                row.get("price") if row.get("price") is not None else row.get("zone1_price"),
                errors="coerce",
            ),
            unit="VND/liter",
            source_record_id=f"vn_fuel_price:{source_id}",
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

    if output_path:
        print(f"\nJSON batch: {output_path}")
    else:
        print("\nKhông có event mới hoặc thay đổi; không tạo file batch.")

    print("\nLatest data:")

    print(
        pd.DataFrame(events).tail(10).to_string(index=False)
    )


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()

