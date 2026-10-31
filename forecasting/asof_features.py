"""Point-in-time checks and predictor mapping for Gold feature rows."""

from __future__ import annotations

import numpy as np
import pandas as pd

MACRO_MAP = {
    "brent_monthly": "brent_usd_per_barrel_avg_mtd_asof",
    "usd_vnd": "vnd_per_usd_avg_mtd_asof",
    "fuel_ron95": "fuel_ron95_vnd_per_liter_avg_mtd_asof",
    "fuel_e10": "fuel_e10_ron95_vnd_per_liter_avg_mtd_asof",
    "iip_yoy_growth": "iip_yoy_growth_latest_prior_month_asof",
}
SOURCE_FLAGS = [
    "brent_asof_eligible",
    "fx_asof_eligible",
    "fuel_asof_eligible",
    "iip_asof_eligible",
]
SOURCE_ABLATION_GROUPS = {
    "brent_monthly": (["brent_monthly"], "missing_brent_asof_eligible"),
    "usd_vnd": (["usd_vnd"], "missing_fx_asof_eligible"),
    "fuel": (["fuel_ron95", "fuel_e10"], "missing_fuel_asof_eligible"),
    "iip": (["iip_yoy_growth"], "missing_iip_asof_eligible"),
}

SOURCE_FIELDS = {
    "brent_asof_eligible": (
        "brent_available_at_asof",
        ("brent_monthly",),
    ),
    "fx_asof_eligible": ("fx_available_at_asof", ("usd_vnd",)),
    "fuel_asof_eligible": (
        "fuel_available_at_asof",
        ("fuel_ron95", "fuel_e10"),
    ),
    "iip_asof_eligible": ("iip_available_at_asof", ("iip_yoy_growth",)),
}

VINTAGE_TO_ASOF_COLUMNS = {
    "brent_usd_per_barrel_avg_mtd": "brent_usd_per_barrel_avg_mtd_asof",
    "brent_available_at": "brent_available_at_asof",
    "vnd_per_usd_avg_mtd": "vnd_per_usd_avg_mtd_asof",
    "fx_available_at": "fx_available_at_asof",
    "fuel_ron95_vnd_per_liter_avg_mtd": "fuel_ron95_vnd_per_liter_avg_mtd_asof",
    "fuel_e10_ron95_vnd_per_liter_avg_mtd": "fuel_e10_ron95_vnd_per_liter_avg_mtd_asof",
    "fuel_available_at": "fuel_available_at_asof",
    "iip_yoy_growth_latest_prior_month": "iip_yoy_growth_latest_prior_month_asof",
    "iip_available_at": "iip_available_at_asof",
}


def _utc(value: object) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    return timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")


def _truthy(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return bool(value)


def select_feature_vintage(
    vintages: pd.DataFrame, target_month: pd.Timestamp, forecast_cutoff: pd.Timestamp
) -> dict | None:
    """Select the newest persisted target-month snapshot available by cutoff."""
    if vintages.empty:
        return None
    frame = vintages.copy()
    months = pd.to_datetime(frame["target_month"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    target = pd.Timestamp(target_month).to_period("M").to_timestamp()
    frame = frame.loc[months.eq(target)].copy()
    if frame.empty:
        return None
    for column in ("asof_cutoff_ts", "snapshot_recorded_at"):
        frame[column] = pd.to_datetime(frame[column], errors="coerce", utc=True)
    cutoff = _utc(forecast_cutoff)
    frame = frame.loc[
        frame["asof_cutoff_ts"].notna()
        & frame["snapshot_recorded_at"].notna()
        & (frame["asof_cutoff_ts"] <= cutoff)
        & (frame["snapshot_recorded_at"] <= cutoff)
    ]
    if frame.empty:
        return None
    selected = frame.sort_values(
        ["asof_cutoff_ts", "snapshot_recorded_at"], kind="stable"
    ).iloc[-1].to_dict()
    for source_column, asof_column in VINTAGE_TO_ASOF_COLUMNS.items():
        selected[asof_column] = selected.get(source_column)
    selected["asof_check_passed"] = True
    for flag, (availability_column, _) in SOURCE_FIELDS.items():
        if _truthy(selected.get(flag, False)):
            available = selected.get(availability_column)
            if (
                available is None
                or pd.isna(available)
                or _utc(available) > selected["asof_cutoff_ts"]
            ):
                selected["asof_check_passed"] = False
                break
    return selected


def feature_asof_row(feature: pd.Series | dict | None, target_cutoff: pd.Timestamp) -> dict:
    """Expose only source values available by both the feature and forecast cutoffs."""
    row = {column: np.nan for column in MACRO_MAP}
    row.update({f"missing_{flag}": 1.0 for flag in SOURCE_FLAGS})
    if feature is None:
        return row

    get = feature.get
    feature_cutoff = get("asof_cutoff_ts")
    if feature_cutoff is None or pd.isna(feature_cutoff):
        return row
    cutoff = _utc(target_cutoff)
    if _utc(feature_cutoff) > cutoff or not _truthy(get("asof_check_passed", False)):
        return row

    for flag, (availability_column, value_names) in SOURCE_FIELDS.items():
        eligible = _truthy(get(flag, False))
        available = get(availability_column)
        if eligible:
            eligible = available is not None and not pd.isna(available) and _utc(available) <= cutoff
        row[f"missing_{flag}"] = 0.0 if eligible else 1.0
        if eligible:
            for name in value_names:
                value = get(MACRO_MAP[name])
                row[name] = float(value) if value is not None and pd.notna(value) else np.nan
    return row
