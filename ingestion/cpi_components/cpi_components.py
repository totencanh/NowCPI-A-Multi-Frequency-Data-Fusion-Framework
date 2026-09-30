
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


# ============================================================
# CONFIG
# ============================================================

BASE_URL = (
    "https://api.imf.org/external/sdmx/3.0/data/"
    "dataflow/IMF.STA/CPI/~/"
)

START_PERIOD = "2015-M01"


# IMF COICOP 1999
COMPONENTS = {
    "CP01": "Food and non-alcoholic beverages",
    "CP02": "Alcoholic beverages, tobacco and narcotics",
    "CP03": "Clothing and footwear",
    "CP04": "Housing, water, electricity, gas and other fuels",
    "CP05": "Furnishings, household equipment and routine household maintenance",
    "CP06": "Health",
    "CP07": "Transport",
    "CP08": "Communication",
    "CP09": "Recreation and culture",
    "CP10": "Education",
    "CP11": "Restaurants and hotels",
    "CP12": "Miscellaneous goods and services",
}


# ============================================================
# FETCH ONE COMPONENT
# ============================================================

def fetch_component(
    code,
    start_period=START_PERIOD
):

    url = (
        f"{BASE_URL}"
        f"VNM.CPI.{code}.IX.M"
        f"?c[TIME_PERIOD]=ge:{start_period}"
    )

    response = requests.get(
        url,
        headers={
            "Accept": "text/csv"
        },
        timeout=30
    )

    # Không có data
    if response.status_code != 200:
        print(
            f"[SKIP] {code}: "
            f"HTTP {response.status_code}"
        )
        return None

    df = pd.read_csv(
        io.StringIO(response.text)
    )

    if df.empty:
        print(
            f"[SKIP] {code}: empty data"
        )
        return None

    return df


# ============================================================
# MAIN
# ============================================================

def fetch_cpi_components():

    all_data = []

    print("=" * 70)
    print("IMF CPI COMPONENTS - VIETNAM")
    print("=" * 70)

    for code, name in COMPONENTS.items():

        print(
            f"\nFetching {code}: {name}"
        )

        try:

            df = fetch_component(code)

            if df is None:
                continue

            # Thêm tên category
            df["CATEGORY_NAME"] = name
            df["COLLECTOR_COMPONENT_CODE"] = code

            all_data.append(df)

            print(
                f"[OK] {code}: "
                f"{len(df)} observations"
            )

        except Exception as e:

            print(
                f"[ERROR] {code}: {e}"
            )

    # --------------------------------------------------------
    # Không có component
    # --------------------------------------------------------

    if not all_data:

        raise RuntimeError(
            "Không lấy được CPI components nào."
        )

    # --------------------------------------------------------
    # Gộp
    # --------------------------------------------------------

    result = pd.concat(
        all_data,
        ignore_index=True
    )

    # Giữ toàn bộ cột và quan sát IMF trả về; chưa loại duplicate ở lớp ingestion.
    return result


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    df = fetch_cpi_components()

    print("\n" + "=" * 70)
    print("RESULT")
    print("=" * 70)

    print(
        f"\nTotal observations: {len(df):,}"
    )

    print(
        f"Components found: "
        f"{df['COICOP_1999'].nunique()}"
    )

    print("\nComponents:")

    summary = (
        df.groupby(
            [
                "COICOP_1999",
                "CATEGORY_NAME"
            ]
        )
        .size()
        .reset_index(
            name="OBSERVATIONS"
        )
    )

    print(
        summary.to_string(index=False)
    )

    # --------------------------------------------------------
    # Save
    # --------------------------------------------------------

    events = []
    for row in df.to_dict(orient="records"):
        component_code = first_available(
            row.get("COICOP_1999"), row.get("COLLECTOR_COMPONENT_CODE"), default="unknown"
        )
        component_code = str(component_code).strip()
        series_id = f"cpi_component_{component_code.lower()}"
        source_period = row.get("TIME_PERIOD")
        period_text = str(source_period) if source_period is not None else ""
        match = re.search(r"(\d{4})-?M(\d{1,2})", period_text, re.IGNORECASE)
        observation_period = (
            f"{match.group(1)}-{int(match.group(2)):02d}" if match else period_text
        )
        value = pd.to_numeric(row.get("OBS_VALUE"), errors="coerce")
        events.append(make_event(
            source="imf",
            series_id=series_id,
            observation_period=observation_period,
            frequency="monthly",
            value=value,
            unit=first_available(row.get("UNIT"), row.get("UNIT_MEASURE"), default="index"),
            release_ts=first_available(row.get("RELEASE_TS"), row.get("RELEASE_DATE")),
            source_record_id=f"{series_id}:{observation_period}",
            raw_payload=row,
        ))

    output_path = write_events("imf_cpi_components_vietnam.json", events)

    print(
        f"\nĐã lưu {len(events)} event vào: {output_path}"
    )
