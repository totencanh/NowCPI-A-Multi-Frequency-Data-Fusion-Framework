"""Run a one-month-ahead Vietnam CPI forecast using full multi-source Ridge.

The primary predictor combines lagged monthly CPI, native-frequency weekly
Brent returns, target-month Brent/USD/VND/fuel and prior-month NSO IIP features,
plus source-availability indicators. Macro features are read from Gold's PIT
view and gated by both source availability and forecast cutoff. AR-MIDAS,
U-MIDAS and CPI-only remain explicit baselines and diagnostics. The target is
the month after the latest released CPI month.

Historical evaluation uses rolling-origin folds. CPI target release timestamps
are used as the forecast cutoff where the source provides them. Historical
Brent publication times and source revisions are not archived, so this remains
a hybrid/pseudo-vintage diagnostic rather than a vintage-accurate claim.

When CPI release timestamps are missing, historical folds use an explicit
fallback assumption: prior-month CPI by day 15 of the target month. Daily Brent
is assumed available on the following UTC calendar day because its collector
does not provide a source release timestamp. The live forecast uses stored
source availability timestamps.
"""

from __future__ import annotations

import json
import hashlib
import os
import time
from datetime import date, datetime, time as daytime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from trino.dbapi import connect
from asof_features import (
    MACRO_MAP,
    SOURCE_FLAGS,
    feature_asof_row,
    select_feature_vintage,
)

from evaluation_protocol import (
    MIN_DM_ORIGINS,
    failure_cases,
    paired_model_comparison,
    release_timestamp_audit,
)
from source_history.raw_batch_version_audit import build_raw_batch_version_audit


HERE = Path(__file__).resolve().parent
OUTPUT_DIR = HERE / "outputs"
LAGS = 12
MIN_TRAIN = 8
RIDGE_ALPHA = 10.0
HISTORICAL_CPI_RELEASE_DAY = 15
HISTORICAL_BRENT_PUBLICATION_LAG_DAYS = 1
RECENT_BACKTEST_MONTHS = 9
INNER_VALIDATION_ORIGINS = 12
MAPE_MIN_ABS_ACTUAL_PP = 0.05
RAW_DATA_DIR = HERE.parent / "data" / "raw"
VIETNAM_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def as_utc(value: pd.Timestamp) -> pd.Timestamp:
    return value.tz_convert("UTC") if value.tzinfo else value.tz_localize("UTC")


def read_table(connection, table: str) -> pd.DataFrame:
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM delta.gold.{table}")
    columns = [item[0].lower() for item in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def load_nso_release_calendar() -> tuple[dict[str, dict], str]:
    """Load the newest immutable NSO date-only publication calendar snapshot."""
    calendar_dir = RAW_DATA_DIR / "nso_cpi_release_calendar"
    snapshots = sorted(calendar_dir.glob("snapshot_*.json"))
    if not snapshots:
        raise RuntimeError(
            "No NSO CPI release-calendar snapshot exists; run the NSO calendar collector first."
        )
    # Snapshot names are content hashes, not timestamps; choose the most
    # recently written immutable version by filesystem modification time.
    snapshot_path = max(snapshots, key=lambda path: path.stat().st_mtime_ns)
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    records = snapshot.get("records", [])
    calendar = {
        str(record["target_month"]): record
        for record in records
        if record.get("target_month") and record.get("issue_date")
    }
    if not calendar:
        raise RuntimeError(f"NSO release-calendar snapshot is empty: {snapshot_path}")
    return calendar, snapshot_path.name


def apply_nso_release_calendar(
    labels: pd.DataFrame, calendar: dict[str, dict]
) -> pd.DataFrame:
    """Attach NSO issue-date metadata while preserving CPI label values."""
    frame = labels.copy()
    periods = frame["target_month"].dt.strftime("%Y-%m")
    frame["label_source_release_date"] = periods.map(
        {period: record["issue_date"] for period, record in calendar.items()}
    )
    frame["label_nso_available_at_proxy"] = periods.map(
        {
            period: record["conservative_available_at_proxy_utc"]
            for period, record in calendar.items()
        }
    )
    frame["label_nso_release_article_url"] = periods.map(
        {period: record.get("article_url") for period, record in calendar.items()}
    )
    frame["label_nso_release_basis"] = periods.map(
        {period: record.get("availability_basis") for period, record in calendar.items()}
    )
    frame["label_nso_available_at_proxy"] = pd.to_datetime(
        frame["label_nso_available_at_proxy"], errors="coerce", utc=True
    )
    return frame


def brent_known_as_of(
    brent: pd.DataFrame,
    availability_cutoff: pd.Timestamp,
    observation_cutoff: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Bound observation dates separately from the run-time availability cutoff."""
    observation_cutoff = observation_cutoff or availability_cutoff
    frame = brent.copy()
    frame["observation_date"] = pd.to_datetime(
        frame["observation_date"], errors="coerce"
    ).dt.tz_localize(None)
    frame["value"] = pd.to_numeric(frame["brent_usd_per_barrel"], errors="coerce")
    frame = frame.dropna(subset=["observation_date", "value"])
    frame = frame.loc[
        (frame["observation_date"] <= observation_cutoff.tz_localize(None))
        & (frame["value"] > 0)
    ]
    if "available_at" not in frame:
        return frame.iloc[0:0]
    available = pd.to_datetime(frame["available_at"], errors="coerce", utc=True)
    frame = frame.loc[
        available.notna() & (available <= as_utc(availability_cutoff))
    ].copy()
    frame["available_at"] = available.loc[frame.index]
    return frame


def weekly_returns(
    brent: pd.DataFrame,
    cutoff: pd.Timestamp,
    respect_available_at: bool,
    available_at_cutoff: pd.Timestamp | None = None,
) -> pd.Series:
    """Weekly mean Brent log returns, restricted to observations known by cutoff."""
    frame = brent.copy()
    frame["observation_date"] = pd.to_datetime(
        frame["observation_date"], errors="coerce"
    ).dt.tz_localize(None)
    frame["value"] = pd.to_numeric(frame["brent_usd_per_barrel"], errors="coerce")
    frame = frame.dropna(subset=["observation_date", "value"])
    frame = frame.loc[
        (frame["observation_date"] <= cutoff.tz_localize(None)) & (frame["value"] > 0)
    ]
    if respect_available_at:
        frame = brent_known_as_of(
            frame,
            available_at_cutoff or cutoff,
            observation_cutoff=cutoff,
        )
    if frame.empty:
        return pd.Series(dtype=float)

    weekly = (
        frame.set_index("observation_date")["value"]
        .resample("W-SUN")
        .mean()
        .dropna()
    )
    return np.log(weekly).diff().dropna()


def historical_weekly_returns(
    brent: pd.DataFrame,
    target_month: pd.Timestamp,
    availability_cutoff: pd.Timestamp,
) -> pd.Series:
    """Build a historical Brent block using an explicit next-day availability assumption."""
    frame = brent.copy()
    frame["observation_date"] = pd.to_datetime(
        frame["observation_date"], errors="coerce"
    ).dt.tz_localize(None)
    frame["value"] = pd.to_numeric(frame["brent_usd_per_barrel"], errors="coerce")
    frame = frame.dropna(subset=["observation_date", "value"])
    source_release = (
        pd.to_datetime(frame["source_release_ts"], errors="coerce", utc=True)
        if "source_release_ts" in frame
        else pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
    )
    assumed_available = pd.to_datetime(
        frame["observation_date"].dt.normalize()
        + pd.Timedelta(days=HISTORICAL_BRENT_PUBLICATION_LAG_DAYS),
        utc=True,
    )
    frame["historical_available_at"] = source_release.where(
        source_release.notna(), assumed_available
    )
    cutoff_utc = as_utc(availability_cutoff)
    frame = frame.loc[
        (frame["observation_date"] < target_month + pd.DateOffset(months=1))
        & (frame["historical_available_at"] <= cutoff_utc)
        & (frame["value"] > 0)
    ]
    if frame.empty:
        return pd.Series(dtype=float)
    weekly = (
        frame.set_index("observation_date")["value"]
        .resample("W-SUN")
        .mean()
        .dropna()
    )
    return np.log(weekly).diff().dropna()


def make_row(
    month: pd.Timestamp,
    labels: pd.DataFrame,
    weekly: pd.Series,
    cutoff: pd.Timestamp,
    actual: float = np.nan,
    require_released_lag: bool = False,
    historical_origin: bool = False,
) -> dict:
    prior_month = month - pd.DateOffset(months=1)
    prior = labels.loc[labels["target_month"] == prior_month, "target_cpi_mom_percent"]
    if (require_released_lag or historical_origin) and not prior.empty:
        if historical_origin:
            label_times = pd.Series(dtype="datetime64[ns, UTC]")
            for timestamp_column in (
                "label_source_release_ts",
                "label_nso_available_at_proxy",
            ):
                if timestamp_column in labels:
                    candidate = pd.to_datetime(
                        labels.loc[
                            labels["target_month"] == prior_month,
                            timestamp_column,
                        ],
                        errors="coerce",
                        utc=True,
                    )
                    if not candidate.empty and pd.notna(candidate.iloc[-1]):
                        label_times = candidate
                        break
        else:
            timestamp_column = "label_available_at"
            label_times = pd.to_datetime(
                labels.loc[labels["target_month"] == prior_month, timestamp_column],
                errors="coerce",
                utc=True,
            ) if timestamp_column in labels else pd.Series(dtype="datetime64[ns, UTC]")
        if label_times.empty or pd.isna(label_times.iloc[-1]):
            # Historical pseudo-vintage fallback only: never substitute
            # ingestion time for a source release time.
            assumed = pd.Timestamp(
                year=month.year,
                month=month.month,
                day=HISTORICAL_CPI_RELEASE_DAY,
                tz="UTC",
            )
            if historical_origin and assumed <= as_utc(cutoff):
                label_times = pd.Series([assumed])
            else:
                prior = prior.iloc[0:0]
        if not label_times.empty and label_times.iloc[-1] > as_utc(cutoff):
            prior = prior.iloc[0:0]
    row = {
        "target_month": month.strftime("%Y-%m-%d"),
        "cutoff_at": cutoff.isoformat(),
        "actual_cpi_mom_percent": actual,
        "cpi_mom_lag_1": float(prior.iloc[-1]) if not prior.empty else np.nan,
    }
    values = np.array([], dtype=float)
    if not weekly.empty and isinstance(weekly.index, pd.DatetimeIndex):
        available_weekly = weekly.loc[weekly.index <= cutoff.tz_localize(None)]
        values = available_weekly.tail(LAGS).to_numpy(dtype=float)
    for lag in range(LAGS):
        row[f"brent_weekly_log_return_lag_{LAGS - lag}"] = (
            float(values[lag]) if len(values) == LAGS else np.nan
        )
    return row


def fit_predict(
    train: pd.DataFrame,
    test: pd.DataFrame,
    predictors: list[str],
    alpha: float = RIDGE_ALPHA,
) -> float:
    """Train-only imputation/scaling and ridge with an unpenalized intercept."""
    x_train = train[predictors].to_numpy(dtype=float)
    x_test = test[predictors].to_numpy(dtype=float)
    medians = np.asarray(
        [
            float(np.median(column[np.isfinite(column)]))
            if np.isfinite(column).any()
            else 0.0
            for column in x_train.T
        ],
        dtype=float,
    )
    x_train = np.where(np.isfinite(x_train), x_train, medians)
    x_test = np.where(np.isfinite(x_test), x_test, medians)
    means = x_train.mean(axis=0)
    scales = x_train.std(axis=0)
    scales = np.where(scales > 1e-12, scales, 1.0)
    x_train = (x_train - means) / scales
    x_test = (x_test - means) / scales
    x_train = np.column_stack([np.ones(len(x_train)), x_train])
    x_test = np.column_stack([np.ones(len(x_test)), x_test])
    penalty = np.eye(x_train.shape[1]) * alpha
    penalty[0, 0] = 0.0
    beta = np.linalg.solve(
        x_train.T @ x_train + penalty,
        x_train.T @ train["actual_cpi_mom_percent"].to_numpy(),
    )
    return float((x_test @ beta).item())


def fit_ar1(train: pd.DataFrame, test: pd.DataFrame) -> float:
    """Unregularized AR(1) benchmark using the previous CPI MoM only."""
    usable = train.dropna(subset=["cpi_mom_lag_1", "actual_cpi_mom_percent"])
    x = usable["cpi_mom_lag_1"].to_numpy(dtype=float)
    y = usable["actual_cpi_mom_percent"].to_numpy(dtype=float)
    design = np.column_stack([np.ones(len(x)), x])
    beta = np.linalg.lstsq(design, y, rcond=None)[0]
    return float(beta[0] + beta[1] * float(test["cpi_mom_lag_1"].iloc[0]))


def select_ar_midas_by_validation(
    train: pd.DataFrame,
    test: pd.DataFrame,
    lag_columns: list[str],
) -> tuple[float | None, int | None, float | None, int]:
    """Select lag count and ridge penalty via expanding inner validation only."""
    train = train.reset_index(drop=True)
    candidate_scores = []
    for lag_count in (3, 6, 12):
        predictors = ["cpi_mom_lag_1", *lag_columns[-lag_count:]]
        if test[predictors].isna().any(axis=None):
            continue
        for alpha in (1.0, 10.0, 100.0):
            errors = []
            validation_start = max(MIN_TRAIN, len(train) - INNER_VALIDATION_ORIGINS)
            for validation_index in range(validation_start, len(train)):
                inner_train = train.iloc[:validation_index].dropna(
                    subset=[*predictors, "actual_cpi_mom_percent"]
                )
                validation_row = train.iloc[[validation_index]]
                if len(inner_train) < MIN_TRAIN or validation_row[predictors].isna().any(axis=None):
                    continue
                prediction = fit_predict(inner_train, validation_row, predictors, alpha)
                errors.append(
                    abs(float(validation_row["actual_cpi_mom_percent"].iloc[0]) - prediction)
                )
            if len(errors) >= 3:
                candidate_scores.append((float(np.mean(errors)), lag_count, alpha, len(errors)))
    if not candidate_scores:
        return None, None, None, 0
    _, lag_count, alpha, validation_origins = min(
        candidate_scores, key=lambda item: (item[0], item[1], -item[2])
    )
    predictors = ["cpi_mom_lag_1", *lag_columns[-lag_count:]]
    fit = train.dropna(subset=[*predictors, "actual_cpi_mom_percent"])
    if len(fit) < MIN_TRAIN:
        return None, None, None, validation_origins
    return fit_predict(fit, test, predictors, alpha), lag_count, alpha, validation_origins


def select_cpi_preserving_blend_share(
    train: pd.DataFrame,
    test: pd.DataFrame,
    lag_columns: list[str],
) -> tuple[float | None, float | None, int]:
    """Select the AR-MIDAS share using only recent expanding inner folds.

    The candidate grid keeps at least 25% weight on AR-MIDAS, so CPI remains
    an active predictor while Brent-only U-MIDAS can contribute to the blend.
    """
    predictors = ["cpi_mom_lag_1", *lag_columns]
    if test[predictors].isna().any(axis=None):
        return None, None, 0
    train = train.reset_index(drop=True)
    validation_start = max(MIN_TRAIN, len(train) - INNER_VALIDATION_ORIGINS)
    actuals, ar_preds, u_preds = [], [], []
    for validation_index in range(validation_start, len(train)):
        validation_row = train.iloc[[validation_index]]
        if validation_row[predictors].isna().any(axis=None):
            continue
        inner_train = train.iloc[:validation_index].dropna(
            subset=[*predictors, "actual_cpi_mom_percent"]
        )
        if len(inner_train) < MIN_TRAIN:
            continue
        ar_preds.append(fit_predict(inner_train, validation_row, predictors, RIDGE_ALPHA))
        u_preds.append(fit_predict(inner_train, validation_row, lag_columns, RIDGE_ALPHA))
        actuals.append(float(validation_row["actual_cpi_mom_percent"].iloc[0]))
    if len(actuals) < 3:
        return None, None, len(actuals)
    actual = np.asarray(actuals, dtype=float)
    ar = np.asarray(ar_preds, dtype=float)
    u = np.asarray(u_preds, dtype=float)
    candidates = []
    for ar_share in (0.25, 0.5, 0.75, 1.0):
        blend = ar_share * ar + (1 - ar_share) * u
        candidates.append((float(np.mean(np.abs(actual - blend))), -ar_share, ar_share))
    validation_mae, _, selected_share = min(candidates)
    return selected_share, validation_mae, len(actuals)


def evaluate(predictions: pd.DataFrame, column: str) -> dict:
    actual = predictions["actual_cpi_mom_percent"].to_numpy(dtype=float)
    predicted = predictions[column].to_numpy(dtype=float)
    error = actual - predicted
    mape_mask = np.abs(actual) >= MAPE_MIN_ABS_ACTUAL_PP
    smape_denominator = np.abs(actual) + np.abs(predicted)
    smape_terms = np.divide(
        2 * np.abs(error),
        smape_denominator,
        out=np.zeros_like(error),
        where=smape_denominator > 0,
    )
    return {
        "model": column,
        "evaluated_months": len(error),
        "mae_pp": float(np.mean(np.abs(error))),
        "rmse_pp": float(np.sqrt(np.mean(error**2))),
        "directional_accuracy_pct": float(
            np.mean(np.sign(actual) == np.sign(predicted)) * 100
        ),
        "mape_pct_excluding_near_zero_actuals": (
            float(np.mean(np.abs(error[mape_mask] / actual[mape_mask])) * 100)
            if mape_mask.any() else None
        ),
        "mape_scored_months": int(mape_mask.sum()),
        "mape_excluded_months_abs_actual_below_0_05pp": int((~mape_mask).sum()),
        "smape_pct": float(np.mean(smape_terms) * 100),
    }


def common_backtest_metrics(predictions: pd.DataFrame) -> pd.DataFrame:
    """Score every model on identical expanding-window folds, full and recent."""
    models = [
        "naive_last_cpi",
        "ar1_ols",
        "cpi_only_ridge",
        "u_midas_brent_only",
        "ar_midas_ridge",
        "ar_midas_validation_selected",
        "ar_midas_validation_blend",
        "full_multisource_ridge",
    ]
    if predictions.empty or any(model not in predictions for model in models):
        return pd.DataFrame()
    common = predictions.dropna(
        subset=["actual_cpi_mom_percent", *models]
    ).sort_values("target_month")
    if common.empty:
        return pd.DataFrame()

    windows = [("all_common_months", common)]
    research_holdout = common.tail(min(36, len(common)))
    windows.append((f"latest_{len(research_holdout)}_common_month_research_holdout", research_holdout))
    latest_24 = common.tail(min(24, len(common)))
    windows.append((f"latest_{len(latest_24)}_common_month_subwindow", latest_24))
    recent = common.tail(RECENT_BACKTEST_MONTHS)
    windows.append((f"most_recent_{len(recent)}_common_months", recent))
    metric_rows = []
    for window_name, frame in windows:
        for model_name in models:
            row = evaluate(frame, model_name)
            row["evaluation_window"] = window_name
            row["information_set_policy"] = (
                "target_CPI_source_release_minus_1_second_when_present_else_target_month_end_proxy; "
                "CPI_lag_source_release_when_present_else_NSO_issue_date_plus_1_day_proxy_then_day_15_proxy; "
                "Brent_source_release_when_present_else_observation_plus_1_UTC_day_proxy; source_revisions_unavailable"
            )
            row["origin_count_status"] = (
                "meets_minimum_24_origins" if len(frame) >= 24
                else f"insufficient_for_research_claim:{len(frame)}_of_24_origins"
            )
            row["evaluation_mode"] = "pseudo_vintage_diagnostic"
            row["real_time_performance_claim_allowed"] = False
            metric_rows.append(row)
    return pd.DataFrame(metric_rows)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(build_raw_batch_version_audit(RAW_DATA_DIR)).to_csv(
        OUTPUT_DIR / "raw_event_version_audit.csv", index=False
    )
    processing_started_at = pd.Timestamp.now(tz="UTC")
    processing_started_clock = time.perf_counter()
    # Freeze the information set before querying sources. Records arriving
    # while this run is in progress belong to the next nowcast, not this one.
    data_cutoff = processing_started_at
    connection = connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "nowcpi"),
        catalog="delta",
        schema="gold",
    )
    try:
        labels = read_table(connection, "gold_nowcasting_labels")
        brent = read_table(connection, "gold_brent_daily")
        # Use immutable snapshots for historical origins and the live forecast.
        # The current PIT view alone cannot reconstruct past information sets.
        feature_vintages = read_table(connection, "gold_nowcasting_feature_vintages")
    finally:
        connection.close()

    labels = labels.loc[labels["country"].astype(str).str.upper() == "VNM"].copy()
    brent = brent.loc[brent["country"].astype(str).str.upper() == "VNM"].copy()
    feature_vintages = feature_vintages.loc[
        feature_vintages["country"].astype(str).str.upper() == "VNM"
    ].copy()
    feature_vintages["target_month"] = pd.to_datetime(
        feature_vintages["target_month"], errors="coerce"
    ).dt.to_period("M").dt.to_timestamp()
    release_timestamp_audit(labels, brent).to_csv(
        OUTPUT_DIR / "release_timestamp_audit.csv", index=False
    )
    if "label_available_at" not in labels:
        raise RuntimeError(
            "Gold CPI labels do not expose label_available_at; refusing an as-of forecast."
        )
    labels["label_available_at"] = pd.to_datetime(
        labels["label_available_at"], errors="coerce", utc=True
    )
    labels = labels.loc[
        labels["label_available_at"].notna()
        & (labels["label_available_at"] <= as_utc(data_cutoff))
    ].copy()
    labels["target_month"] = pd.to_datetime(labels["target_month"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    labels["target_cpi_mom_percent"] = pd.to_numeric(
        labels["target_cpi_mom_percent"], errors="coerce"
    )
    labels = (
        labels.dropna(subset=["target_month", "target_cpi_mom_percent"])
        .drop_duplicates("target_month", keep="last")
        .sort_values("target_month")
        .reset_index(drop=True)
    )
    release_calendar, release_snapshot_name = load_nso_release_calendar()
    labels = apply_nso_release_calendar(labels, release_calendar)
    matched_release_dates = int(labels["label_source_release_date"].notna().sum())
    pd.DataFrame(
        [
            {
                "calendar_snapshot": release_snapshot_name,
                "cpi_label_months": len(labels),
                "months_with_nso_issue_date": matched_release_dates,
                "issue_date_coverage_pct": round(
                    matched_release_dates / len(labels) * 100, 2
                ) if len(labels) else 0.0,
                "release_time_precision": "date_only",
                "within_day_publication_time_known": False,
                "historical_cpi_value_vintages_archived": False,
                "strict_vintage_backtest_ready": False,
            }
        ]
    ).to_csv(OUTPUT_DIR / "nso_release_calendar_audit.csv", index=False)
    if labels.empty or brent.empty:
        raise RuntimeError("Gold CPI labels or Vietnam Brent observations are empty.")

    backtest_started_clock = time.perf_counter()
    historical_rows = []
    for item in labels.itertuples(index=False):
        month = item.target_month
        month_end = month + pd.offsets.MonthEnd(0)
        cutoff = (
            month_end + pd.Timedelta(days=1, hours=23, minutes=59, seconds=59)
        ).tz_localize("UTC")
        label_row = labels.loc[labels["target_month"] == month].iloc[-1]
        target_issue_date = label_row["label_source_release_date"]
        if pd.notna(target_issue_date):
            issue_day = date.fromisoformat(str(target_issue_date))
            # With date-only metadata, do not use same-day observations. The
            # cutoff is the beginning of the official issue date in Vietnam.
            cutoff = pd.Timestamp(
                datetime.combine(issue_day, daytime.min, tzinfo=VIETNAM_TZ)
            ).tz_convert("UTC")
        if "label_source_release_ts" in labels:
            target_release = pd.to_datetime(
                labels.loc[labels["target_month"] == month, "label_source_release_ts"],
                errors="coerce", utc=True,
            )
            if not target_release.empty and pd.notna(target_release.iloc[-1]):
                cutoff = target_release.iloc[-1] - pd.Timedelta(seconds=1)
        assumed_cpi_release = pd.Timestamp(
            year=month.year,
            month=month.month,
            day=HISTORICAL_CPI_RELEASE_DAY,
            tz="UTC",
        )
        weekly = historical_weekly_returns(brent, month, cutoff)
        row = make_row(
            month, labels, weekly, cutoff, float(item.target_cpi_mom_percent),
            historical_origin=True,
        )
        point_in_time_feature = select_feature_vintage(
            feature_vintages, month, cutoff
        )
        row.update(
            feature_asof_row(
                point_in_time_feature,
                cutoff,
            )
        )
        prior_label = labels.loc[labels["target_month"] == month - pd.DateOffset(months=1)]
        cpi_lag_nso_available = (
            prior_label["label_nso_available_at_proxy"].iloc[-1]
            if not prior_label.empty and "label_nso_available_at_proxy" in prior_label
            else pd.NaT
        )
        row["cpi_lag_assumed_available_at"] = (
            cpi_lag_nso_available.isoformat()
            if pd.notna(cpi_lag_nso_available)
            else assumed_cpi_release.isoformat()
        )
        row["cpi_lag_nso_release_date"] = (
            prior_label["label_source_release_date"].iloc[-1]
            if not prior_label.empty else None
        )
        if "label_source_release_ts" in labels:
            release_ts = pd.to_datetime(
                labels.loc[labels["target_month"] == month, "label_source_release_ts"],
                errors="coerce", utc=True,
            )
            row["target_source_release_ts"] = (
                release_ts.iloc[-1].isoformat()
                if not release_ts.empty and pd.notna(release_ts.iloc[-1]) else None
            )
        else:
            row["target_source_release_ts"] = None
        assumed_target_release = pd.Timestamp(
            year=(month + pd.DateOffset(months=1)).year,
            month=(month + pd.DateOffset(months=1)).month,
            day=HISTORICAL_CPI_RELEASE_DAY,
            tz="UTC",
        )
        target_nso_available = label_row["label_nso_available_at_proxy"]
        row["target_effective_release_at"] = (
            row["target_source_release_ts"]
            or (
                target_nso_available.isoformat()
                if pd.notna(target_nso_available)
                else assumed_target_release.isoformat()
            )
        )
        row["target_nso_release_date"] = target_issue_date
        row["information_origin_basis"] = (
            "cpi_source_release_ts_minus_1_second"
            if row["target_source_release_ts"]
            else (
                "nso_date_only_issue_date_day_start_vietnam"
                if pd.notna(target_issue_date)
                else "target_month_end_proxy"
            )
        )
        row["cpi_lag_availability_basis"] = (
            "cpi_source_release_ts"
            if not prior_label.empty
            and "label_source_release_ts" in prior_label
            and pd.notna(prior_label["label_source_release_ts"].iloc[-1])
            else (
                "nso_issue_date_plus_one_day_vietnam_midnight_proxy"
                if not prior_label.empty
                and "label_nso_available_at_proxy" in prior_label
                and pd.notna(prior_label["label_nso_available_at_proxy"].iloc[-1])
                else "assumed_day_15_proxy"
            )
        )
        row["cpi_lag_nso_release_date"] = (
            prior_label["label_source_release_date"].iloc[-1]
            if not prior_label.empty
            and "label_source_release_date" in prior_label
            and pd.notna(prior_label["label_source_release_date"].iloc[-1])
            else None
        )
        row["brent_availability_assumption"] = (
            "source_release_ts_when_present_else_observation_date_plus_"
            f"{HISTORICAL_BRENT_PUBLICATION_LAG_DAYS}_utc_days"
        )
        row["historical_information_set"] = (
            "source_release_when_present_else_explicit_proxy_no_archived_revisions"
        )
        historical_rows.append(
            row
        )
    history = pd.DataFrame(historical_rows).sort_values("target_month").reset_index(drop=True)

    lag_columns = [f"brent_weekly_log_return_lag_{lag}" for lag in range(LAGS, 0, -1)]
    ar_midas_predictors = ["cpi_mom_lag_1", *lag_columns]
    multisource_predictors = [*ar_midas_predictors, *MACRO_MAP, *[f"missing_{flag}" for flag in SOURCE_FLAGS]]
    backtest = []
    for index in range(len(history)):
        row = history.iloc[[index]]
        train = history.iloc[:index].dropna(subset=["actual_cpi_mom_percent"])
        train_release_times = pd.to_datetime(
            train["target_effective_release_at"], errors="coerce", utc=True
        )
        train = train.loc[train_release_times <= as_utc(pd.Timestamp(row["cutoff_at"].iloc[0]))]
        if len(train) < MIN_TRAIN:
            continue
        record = {
            "target_month": row["target_month"].iloc[0],
            "actual_cpi_mom_percent": float(row["actual_cpi_mom_percent"].iloc[0]),
            "training_months": len(train),
            "information_cutoff_at": row["cutoff_at"].iloc[0],
            "cpi_lag_assumed_available_at": row[
                "cpi_lag_assumed_available_at"
            ].iloc[0],
            "brent_availability_assumption": row[
                "brent_availability_assumption"
            ].iloc[0],
            "historical_information_set": row[
                "historical_information_set"
            ].iloc[0],
            "information_origin_basis": row["information_origin_basis"].iloc[0],
            "cpi_lag_availability_basis": row["cpi_lag_availability_basis"].iloc[0],
            "cpi_lag_nso_release_date": row["cpi_lag_nso_release_date"].iloc[0],
            "target_nso_release_date": row["target_nso_release_date"].iloc[0],
            "naive_last_cpi": (
                float(row["cpi_mom_lag_1"].iloc[0])
                if pd.notna(row["cpi_mom_lag_1"].iloc[0])
                else np.nan
            ),
        }
        record["ar1_ols"] = (
            fit_ar1(train, row)
            if pd.notna(row["cpi_mom_lag_1"].iloc[0])
            and len(train.dropna(subset=["cpi_mom_lag_1"])) >= MIN_TRAIN
            else np.nan
        )
        midas_train = train.dropna(subset=lag_columns)
        if row[lag_columns].notna().all(axis=None) and len(midas_train) >= MIN_TRAIN:
            record["u_midas_brent_only"] = fit_predict(midas_train, row, lag_columns)
            record["midas_training_months"] = len(midas_train)
        else:
            record["u_midas_brent_only"] = np.nan
        cpi_train = train.dropna(subset=["cpi_mom_lag_1"])
        if pd.notna(row["cpi_mom_lag_1"].iloc[0]) and len(cpi_train) >= MIN_TRAIN:
            record["cpi_only_ridge"] = fit_predict(cpi_train, row, ["cpi_mom_lag_1"])
        else:
            record["cpi_only_ridge"] = np.nan
        ar_midas_train = midas_train.dropna(subset=["cpi_mom_lag_1"])
        if (
            row[lag_columns].notna().all(axis=None)
            and pd.notna(row["cpi_mom_lag_1"].iloc[0])
            and len(ar_midas_train) >= MIN_TRAIN
        ):
            record["ar_midas_ridge"] = fit_predict(
                ar_midas_train, row, ar_midas_predictors
            )
        else:
            record["ar_midas_ridge"] = np.nan
        selection_started_clock = time.perf_counter()
        selected, selected_lags, selected_alpha, validation_origins = select_ar_midas_by_validation(
            train, row, lag_columns
        )
        record["feature_selection_duration_seconds"] = round(
            time.perf_counter() - selection_started_clock, 6
        )
        record["ar_midas_validation_selected"] = (
            selected if selected is not None else np.nan
        )
        record["selected_brent_lags"] = selected_lags
        record["selected_ridge_alpha"] = selected_alpha
        record["inner_validation_origins"] = validation_origins
        blend_share, blend_validation_mae, blend_validation_origins = select_cpi_preserving_blend_share(
            train, row, lag_columns
        )
        record["selected_ar_midas_share"] = blend_share
        record["blend_validation_mae_pp"] = blend_validation_mae
        record["blend_validation_origins"] = blend_validation_origins
        record["ar_midas_validation_blend"] = (
            blend_share * record["ar_midas_ridge"]
            + (1 - blend_share) * record["u_midas_brent_only"]
            if blend_share is not None
            and pd.notna(record["ar_midas_ridge"])
            and pd.notna(record["u_midas_brent_only"])
            else np.nan
        )
        multisource_train = train.dropna(subset=["cpi_mom_lag_1"])
        record["full_multisource_ridge"] = (
            fit_predict(multisource_train, row, multisource_predictors)
            if pd.notna(row["cpi_mom_lag_1"].iloc[0])
            and len(multisource_train) >= MIN_TRAIN
            else np.nan
        )
        record["multisource_training_rows"] = len(multisource_train)
        record["multisource_training_origins_with_any_macro_source"] = int(
            multisource_train[[f"missing_{flag}" for flag in SOURCE_FLAGS]]
            .eq(0)
            .any(axis=1)
            .sum()
        )
        backtest.append(record)

    predictions = pd.DataFrame(backtest)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(OUTPUT_DIR / "midas_backtest_predictions.csv", index=False)
    metrics = common_backtest_metrics(predictions)
    metrics.to_csv(OUTPUT_DIR / "midas_backtest_metrics.csv", index=False)
    backtest_evaluation_duration = round(
        time.perf_counter() - backtest_started_clock, 3
    )
    (OUTPUT_DIR / "backtest_protocol.json").write_text(
        json.dumps(
            {
                "protocol_version": 2,
                "outer_split": "expanding rolling origin; each target is scored once",
                "minimum_initial_training_months": MIN_TRAIN,
                "baseline_models": ["last_cpi_naive", "ar1_ols"],
                "primary_model": "full_multisource_ridge",
                "proposed_models": ["full_multisource_ridge", "fixed_ar_midas_ridge", "validation_selected_ar_midas", "validation_selected_cpi_preserving_blend"],
                "validation_grid": {"brent_weekly_lags": [3, 6, 12], "ridge_alpha": [1, 10, 100], "ar_midas_blend_share": [0.25, 0.5, 0.75, 1.0]},
                "maximum_inner_validation_origins_per_outer_fold": INNER_VALIDATION_ORIGINS,
                "blend_change": "single controlled ablation; choose AR-MIDAS share from {0.25,0.5,0.75,1.0} using inner validation; CPI model retains positive weight",
                "research_holdout_months": 36,
                "minimum_research_holdout_months": 24,
                "feature_selection": "expanding inner validation only; outer target excluded",
                "primary_metric": "MAE in CPI MoM percentage points",
                "guardrail_metric": "RMSE in CPI MoM percentage points",
                "primary_model_hyperparameters": {"ridge_alpha": RIDGE_ALPHA, "training_only_median_imputation": True, "source_missingness_indicators": True},
                "primary_model_sources": ["CPI", "Brent weekly and monthly", "USD/VND", "Vietnam RON95/E10 fuel", "NSO monthly IIP"],
                "supplementary_metrics": ["MAPE excluding abs(actual)<0.05pp; report scored/excluded counts", "sMAPE"],
                "mape_near_zero_exclusion_threshold_pp": MAPE_MIN_ABS_ACTUAL_PP,
                "paired_comparison": "identical target months per model pair",
                "reported_windows": ["all_available", "latest_36", "latest_24", "latest_9_descriptive_only"],
                "dm_minimum_paired_origins": MIN_DM_ORIGINS,
                "dm_losses": ["absolute_error", "squared_error"],
                "uncertainty": "seeded paired circular moving-block bootstrap 95% CI",
                "release_time_policy": "CPI uses source_release_ts when present, otherwise NSO issue date with a conservative next-Vietnam-day availability proxy, then day-15 fallback; Brent uses source_release_ts when present, otherwise observation date + 1 UTC day",
                "vintage_limit": "NSO CPI issue dates are collected, but historical CPI value vintages and complete source vintages are not archived; do not claim strict vintage-accurate results",
                "real_time_performance_claim_allowed": False,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    paired_rows = []
    paired_evaluation_started_clock = time.perf_counter()
    model_baselines = {
        "ar1_ols": ["naive_last_cpi"],
        "cpi_only_ridge": ["naive_last_cpi", "ar1_ols"],
        "u_midas_brent_only": ["naive_last_cpi", "ar1_ols"],
        "ar_midas_ridge": ["naive_last_cpi", "ar1_ols"],
        "ar_midas_validation_selected": ["naive_last_cpi", "ar1_ols"],
        "ar_midas_validation_blend": ["naive_last_cpi", "ar1_ols", "ar_midas_ridge"],
        "full_multisource_ridge": ["naive_last_cpi", "ar_midas_ridge"],
    }
    for model_name, baselines in model_baselines.items():
        if model_name not in predictions:
            continue
        for baseline_model in baselines:
            paired = predictions.dropna(
                subset=["actual_cpi_mom_percent", baseline_model, model_name]
            ).sort_values("target_month")
            for window_name, window in (
                ("all_pairwise_common_origins", paired),
                ("latest_36_pairwise_common_origins", paired.tail(36)),
                ("latest_24_pairwise_common_origins", paired.tail(24)),
                ("latest_9_descriptive_only", paired.tail(RECENT_BACKTEST_MONTHS)),
            ):
                if window.empty:
                    continue
                comparison = paired_model_comparison(window, model_name, baseline_model)
                comparison["evaluation_window"] = window_name
                comparison["evaluation_mode"] = "pseudo_vintage_diagnostic"
                comparison["real_time_performance_claim_allowed"] = False
                paired_rows.append(comparison)
    pd.DataFrame(paired_rows).to_csv(
        OUTPUT_DIR / "midas_paired_comparisons.csv", index=False
    )
    failure_frame = predictions.dropna(
        subset=["actual_cpi_mom_percent", "naive_last_cpi", "ar_midas_ridge"]
    ).sort_values("target_month")
    if not failure_frame.empty:
        failure_cases(failure_frame, "ar_midas_ridge", "naive_last_cpi").to_csv(
            OUTPUT_DIR / "midas_failure_cases.csv", index=False
        )
    paired_evaluation_duration = round(
        time.perf_counter() - paired_evaluation_started_clock, 3
    )

    now = data_cutoff
    calendar_current_month = now.tz_localize(None).to_period("M").to_timestamp()
    latest_actual_month = labels["target_month"].max()
    current_month = latest_actual_month + pd.DateOffset(months=1)
    target_month_end = (
        current_month
        + pd.offsets.MonthEnd(0)
        + pd.Timedelta(hours=23, minutes=59, seconds=59)
    ).tz_localize("UTC")
    target_observation_cutoff = min(now, target_month_end)
    current_weekly = weekly_returns(
        brent,
        target_observation_cutoff,
        respect_available_at=True,
        available_at_cutoff=now,
    )
    current_brent = brent_known_as_of(
        brent,
        availability_cutoff=now,
        observation_cutoff=target_observation_cutoff,
    )
    current = pd.DataFrame(
        [make_row(current_month, labels, current_weekly, now, require_released_lag=True)]
    )
    current_feature_row = select_feature_vintage(
        feature_vintages, current_month, now
    )
    current_macro_features = feature_asof_row(current_feature_row, now)
    current = pd.concat(
        [current.reset_index(drop=True), pd.DataFrame([current_macro_features])], axis=1
    )
    cpi_lag_month = current_month - pd.DateOffset(months=1)
    cpi_lag_label = labels.loc[labels["target_month"] == cpi_lag_month]
    train = history.dropna(subset=["actual_cpi_mom_percent"])
    cpi_train = train.dropna(subset=["cpi_mom_lag_1"])
    midas_train = train.dropna(subset=lag_columns)
    current_result = {
        "target_month": current_month.strftime("%Y-%m"),
        "target_month_basis": "one_month_after_latest_actual_cpi",
        "target_is_current_calendar_month": bool(
            current_month == calendar_current_month
        ),
        "latest_actual_cpi_age_months_at_run": int(
            (calendar_current_month.year - latest_actual_month.year) * 12
            + calendar_current_month.month
            - latest_actual_month.month
        ),
        "cutoff_at_utc": now.isoformat(),
        "target_observation_cutoff_at_utc": target_observation_cutoff.isoformat(),
        "processing_started_at_utc": processing_started_at.isoformat(),
        "training_months": len(train),
        "latest_actual_cpi_month": latest_actual_month.strftime("%Y-%m"),
        "cpi_only_ridge_nowcast_mom_percent": None,
        "u_midas_brent_only_diagnostic_mom_percent": None,
        "ar_midas_ridge_nowcast_mom_percent": None,
        "full_multisource_ridge_nowcast_mom_percent": None,
        "full_multisource_ridge_training_rows": 0,
        "full_multisource_ridge_training_origins_with_any_macro_source": 0,
        "multisource_source_training_origins": {},
        "multisource_current_feature_coverage_pct": float(
            current_feature_row.get("asof_feature_source_coverage_pct", 0.0)
        ) if current_feature_row else 0.0,
        "multisource_feature_build_id": (
            current_feature_row.get("gold_build_id") if current_feature_row else None
        ),
        "ar_midas_validation_blend_diagnostic_mom_percent": None,
        "validation_selected_ar_midas_share": None,
        "nowcast_mom_percent": None,
        "nowcast_model": None,
        "midas_status": "insufficient labeled CPI training months",
        "method": (
            "Primary full multi-source Ridge for one-step-ahead Vietnam CPI MoM; "
            "uses CPI lag, 12 weekly Brent returns, monthly Brent, USD/VND, "
            "Vietnam RON95/E10 fuel, NSO IIP, and source-missingness indicators."
        ),
        "evaluation_note": (
            "Historical scores use source release timestamps when present; absent CPI "
            "timestamps use NSO's actual issue date with a conservative next-Vietnam-day "
            "availability proxy, then a day-15 fallback; absent Brent release dates use "
            "observation date plus one UTC day. Source revisions are not archived, "
            "so scores are pseudo-vintage diagnostics and not real-time performance claims."
        ),
        "availability_policy": (
            "Every macro predictor is read from the newest eligible row in "
            "gold_nowcasting_feature_vintages; its snapshot cutoff, recorded time, and "
            "source available_at must be no later than the forecast cutoff. Missing source "
            "values are imputed using training-only medians with explicit missingness "
            "indicators. Historical source vintages remain incomplete."
        ),
        "brent_observations_available_at_cutoff": int(len(current_brent)),
        "brent_latest_observation_date": (
            pd.to_datetime(current_brent["observation_date"], errors="coerce")
            .max()
            .strftime("%Y-%m-%d")
            if not current_brent.empty
            else None
        ),
        "brent_latest_available_at_utc": (
            current_brent["available_at"].max().isoformat()
            if not current_brent.empty
            else None
        ),
        "brent_latest_available_at_basis": (
            current_brent.loc[current_brent["available_at"].idxmax(), "available_at_basis"]
            if not current_brent.empty and "available_at_basis" in current_brent
            else "basis_not_exposed_by_Gold"
        ),
        "cpi_lag_month": cpi_lag_month.strftime("%Y-%m"),
        "cpi_lag_nso_issue_date": (
            cpi_lag_label["label_source_release_date"].iloc[-1]
            if not cpi_lag_label.empty
            and "label_source_release_date" in cpi_lag_label
            and pd.notna(cpi_lag_label["label_source_release_date"].iloc[-1])
            else None
        ),
        "cpi_lag_nso_available_at_proxy_utc": (
            cpi_lag_label["label_nso_available_at_proxy"].iloc[-1].isoformat()
            if not cpi_lag_label.empty
            and "label_nso_available_at_proxy" in cpi_lag_label
            and pd.notna(cpi_lag_label["label_nso_available_at_proxy"].iloc[-1])
            else None
        ),
        "cpi_lag_available_at_utc": (
            cpi_lag_label["label_available_at"].max().isoformat()
            if not cpi_lag_label.empty
            else None
        ),
        "cpi_lag_available_at_basis": (
            cpi_lag_label["label_available_at_basis"].iloc[-1]
            if not cpi_lag_label.empty and "label_available_at_basis" in cpi_lag_label
            else "basis_not_exposed_by_Gold"
        ),
    }
    if pd.notna(current["cpi_mom_lag_1"].iloc[0]) and len(cpi_train) >= MIN_TRAIN:
        current_result["cpi_only_ridge_nowcast_mom_percent"] = fit_predict(
            cpi_train, current, ["cpi_mom_lag_1"]
        )
    else:
        current_result["cpi_lag_status"] = (
            "previous month's CPI MoM is not present or not yet released by the cutoff"
        )
    if current[lag_columns].notna().all(axis=None) and len(midas_train) >= MIN_TRAIN:
        current_result["u_midas_brent_only_diagnostic_mom_percent"] = fit_predict(
            midas_train, current, lag_columns
        )
        current_result["midas_training_months"] = len(midas_train)
    ar_midas_train = midas_train.dropna(subset=["cpi_mom_lag_1"])
    if (
        current[lag_columns].notna().all(axis=None)
        and pd.notna(current["cpi_mom_lag_1"].iloc[0])
        and len(ar_midas_train) >= MIN_TRAIN
    ):
        current_result["ar_midas_ridge_nowcast_mom_percent"] = fit_predict(
            ar_midas_train, current, ar_midas_predictors
        )
    multisource_train = train.dropna(subset=["cpi_mom_lag_1"])
    if (
        pd.notna(current["cpi_mom_lag_1"].iloc[0])
        and len(multisource_train) >= MIN_TRAIN
    ):
        current_result["full_multisource_ridge_nowcast_mom_percent"] = fit_predict(
            multisource_train, current, multisource_predictors
        )
        current_result["full_multisource_ridge_training_rows"] = len(multisource_train)
        current_result["full_multisource_ridge_training_origins_with_any_macro_source"] = int(
            multisource_train[[f"missing_{flag}" for flag in SOURCE_FLAGS]]
            .eq(0)
            .any(axis=1)
            .sum()
        )
        current_result["multisource_source_training_origins"] = {
            flag: int((~multisource_train[f"missing_{flag}"].astype(bool)).sum())
            for flag in SOURCE_FLAGS
        }
    blend_share, blend_validation_mae, blend_validation_origins = select_cpi_preserving_blend_share(
        train, current, lag_columns
    )
    current_result["validation_selected_ar_midas_share"] = blend_share
    current_result["blend_validation_mae_pp"] = blend_validation_mae
    current_result["blend_validation_origins"] = blend_validation_origins
    if (
        blend_share is not None
        and current_result["ar_midas_ridge_nowcast_mom_percent"] is not None
        and current_result["u_midas_brent_only_diagnostic_mom_percent"] is not None
    ):
        current_result["ar_midas_validation_blend_diagnostic_mom_percent"] = (
            blend_share * current_result["ar_midas_ridge_nowcast_mom_percent"]
            + (1 - blend_share)
            * current_result["u_midas_brent_only_diagnostic_mom_percent"]
        )
    if current_result["full_multisource_ridge_nowcast_mom_percent"] is not None:
        current_result["nowcast_mom_percent"] = current_result[
            "full_multisource_ridge_nowcast_mom_percent"
        ]
        current_result["nowcast_model"] = "full_multisource_ridge"
        current_result["midas_status"] = "ok"
        current_result["model_hyperparameters"] = {"ridge_alpha": RIDGE_ALPHA}
        current_result["model_predictors"] = multisource_predictors
        current_result["model_source_flags"] = SOURCE_FLAGS
        if current_result["full_multisource_ridge_training_origins_with_any_macro_source"] == 0:
            current_result["midas_status"] = (
                "primary multi-source model produced an estimate, but historical source "
                "effects are not yet identifiable; zero PIT training origins contain macro sources"
            )
    elif current_result["ar_midas_ridge_nowcast_mom_percent"] is not None:
        current_result["nowcast_mom_percent"] = current_result[
            "ar_midas_ridge_nowcast_mom_percent"
        ]
        current_result["nowcast_model"] = "ar_midas_ridge_fallback"
        current_result["midas_status"] = "multi-source training not eligible; fell back to AR-MIDAS"
    elif current_result["cpi_only_ridge_nowcast_mom_percent"] is not None:
        current_result["nowcast_mom_percent"] = current_result[
            "cpi_only_ridge_nowcast_mom_percent"
        ]
        current_result["nowcast_model"] = "cpi_only_ridge"
        current_result["midas_status"] = (
            "Brent mixed-frequency features unavailable; used latest-CPI baseline"
        )
    elif len(train) >= MIN_TRAIN:
        current_result["midas_status"] = (
            "no forecast: latest CPI lag or complete target-aligned Brent lags unavailable"
        )
    processing_completed_at = pd.Timestamp.now(tz="UTC")
    current_result["processing_completed_at_utc"] = processing_completed_at.isoformat()
    current_result["processing_duration_seconds"] = round(
        time.perf_counter() - processing_started_clock, 3
    )
    current_result["backtest_evaluation_duration_seconds"] = backtest_evaluation_duration
    current_result["paired_evaluation_duration_seconds"] = paired_evaluation_duration
    current_result["feature_selection_duration_seconds_total"] = round(
        float(predictions.get("feature_selection_duration_seconds", pd.Series(dtype=float)).sum()),
        3,
    )
    current_result["primary_model_policy"] = "full_multisource_ridge; AR-MIDAS and CPI-only are fallback/benchmark models"
    model_code_digest = hashlib.sha256(
        Path(__file__).read_bytes()
        + Path(__file__).with_name("asof_features.py").read_bytes()
    ).hexdigest()
    model_signature = json.dumps(
        {
            "name": current_result["nowcast_model"],
            "predictors": current_result.get("model_predictors", []),
            "hyperparameters": current_result.get("model_hyperparameters", {}),
            "code_sha256": model_code_digest,
        },
        sort_keys=True,
    )
    current_result["model_version"] = "v1-" + hashlib.sha256(model_signature.encode()).hexdigest()[:12]
    current_result["model_code_sha256"] = model_code_digest
    production_model_dir = OUTPUT_DIR / "production_models" / "full_multisource_ridge"
    production_model_dir.mkdir(parents=True, exist_ok=True)
    (production_model_dir / "latest.json").write_text(
        json.dumps(current_result, indent=2, default=str), encoding="utf-8"
    )
    (OUTPUT_DIR / "midas_current_nowcast.json").write_text(
        json.dumps(current_result, indent=2), encoding="utf-8"
    )
    print(json.dumps(current_result, indent=2))
    if not metrics.empty:
        print("\nPseudo-out-of-sample MIDAS metrics:")
        print(metrics.to_string(index=False))


if __name__ == "__main__":
    main()
