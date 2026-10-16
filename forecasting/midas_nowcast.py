"""Run a Vietnam CPI nowcast using a regularized U-MIDAS oil block.

The model follows the mixed-frequency setup in ``midas_capstone``: monthly CPI
is predicted using a lagged monthly CPI value and native-frequency oil
observations. Daily Brent prices are converted to weekly log returns, and the
latest 12 weekly returns enter as separate regressors (U-MIDAS).

Historical evaluation uses calendar month-end information sets, not archived
real-time data vintages. It is therefore a pseudo-out-of-sample diagnostic,
not a vintage-accurate performance claim.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
from trino.dbapi import connect


HERE = Path(__file__).resolve().parent
OUTPUT_DIR = HERE / "outputs"
LAGS = 12
MIN_TRAIN = 8
RIDGE_ALPHA = 10.0


def as_utc(value: pd.Timestamp) -> pd.Timestamp:
    return value.tz_convert("UTC") if value.tzinfo else value.tz_localize("UTC")


def read_table(connection, table: str) -> pd.DataFrame:
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM delta.gold.{table}")
    columns = [item[0].lower() for item in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


def brent_known_as_of(brent: pd.DataFrame, cutoff: pd.Timestamp) -> pd.DataFrame:
    """Keep only Brent observations whose source availability is at/before cutoff."""
    frame = brent.copy()
    frame["observation_date"] = pd.to_datetime(
        frame["observation_date"], errors="coerce"
    ).dt.tz_localize(None)
    frame["value"] = pd.to_numeric(frame["brent_usd_per_barrel"], errors="coerce")
    frame = frame.dropna(subset=["observation_date", "value"])
    frame = frame.loc[
        (frame["observation_date"] <= cutoff.tz_localize(None)) & (frame["value"] > 0)
    ]
    frame = frame.loc[
        (frame["observation_date"] <= cutoff.tz_localize(None)) & (frame["value"] > 0)
    ]
    if "available_at" not in frame:
        return frame.iloc[0:0]
    available = pd.to_datetime(frame["available_at"], errors="coerce", utc=True)
    frame = frame.loc[available.notna() & (available <= as_utc(cutoff))].copy()
    frame["available_at"] = available.loc[frame.index]
    return frame


def weekly_returns(
    brent: pd.DataFrame, cutoff: pd.Timestamp, respect_available_at: bool
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
        frame = brent_known_as_of(frame, cutoff)
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
) -> dict:
    prior_month = month - pd.DateOffset(months=1)
    prior = labels.loc[labels["target_month"] == prior_month, "target_cpi_mom_percent"]
    if require_released_lag and not prior.empty:
        if "label_available_at" not in labels:
            prior = prior.iloc[0:0]
        else:
            label_times = pd.to_datetime(
                labels.loc[labels["target_month"] == prior_month, "label_available_at"],
                errors="coerce",
                utc=True,
            )
            if (
                label_times.empty
                or pd.isna(label_times.iloc[-1])
                or label_times.iloc[-1] > as_utc(cutoff)
            ):
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


def fit_predict(train: pd.DataFrame, test: pd.DataFrame, predictors: list[str]) -> float:
    """Train-only imputation/scaling and ridge with an unpenalized intercept."""
    x_train = train[predictors].to_numpy(dtype=float)
    x_test = test[predictors].to_numpy(dtype=float)
    medians = np.nanmedian(x_train, axis=0)
    medians = np.where(np.isfinite(medians), medians, 0.0)
    x_train = np.where(np.isfinite(x_train), x_train, medians)
    x_test = np.where(np.isfinite(x_test), x_test, medians)
    means = x_train.mean(axis=0)
    scales = x_train.std(axis=0)
    scales = np.where(scales > 1e-12, scales, 1.0)
    x_train = (x_train - means) / scales
    x_test = (x_test - means) / scales
    x_train = np.column_stack([np.ones(len(x_train)), x_train])
    x_test = np.column_stack([np.ones(len(x_test)), x_test])
    penalty = np.eye(x_train.shape[1]) * RIDGE_ALPHA
    penalty[0, 0] = 0.0
    beta = np.linalg.solve(
        x_train.T @ x_train + penalty,
        x_train.T @ train["actual_cpi_mom_percent"].to_numpy(),
    )
    return float((x_test @ beta).item())


def evaluate(predictions: pd.DataFrame, column: str) -> dict:
    actual = predictions["actual_cpi_mom_percent"].to_numpy(dtype=float)
    predicted = predictions[column].to_numpy(dtype=float)
    error = actual - predicted
    return {
        "model": column,
        "evaluated_months": len(error),
        "mae_pp": float(np.mean(np.abs(error))),
        "rmse_pp": float(np.sqrt(np.mean(error**2))),
        "directional_accuracy_pct": float(
            np.mean(np.sign(actual) == np.sign(predicted)) * 100
        ),
    }


def main() -> None:
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
    finally:
        connection.close()

    labels = labels.loc[labels["country"].astype(str).str.upper() == "VNM"].copy()
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
    brent = brent.loc[brent["country"].astype(str).str.upper() == "VNM"].copy()
    if labels.empty or brent.empty:
        raise RuntimeError("Gold CPI labels or Vietnam Brent observations are empty.")

    historical_rows = []
    for item in labels.itertuples(index=False):
        month = item.target_month
        cutoff = month + pd.offsets.MonthEnd(0) + pd.Timedelta(hours=23, minutes=59, seconds=59)
        # Historical source ingestion timestamps are not historical release
        # vintages. Filter by observation date only and disclose this caveat.
        weekly = weekly_returns(brent, cutoff, respect_available_at=False)
        historical_rows.append(
            make_row(month, labels, weekly, cutoff, float(item.target_cpi_mom_percent))
        )
    history = pd.DataFrame(historical_rows).sort_values("target_month").reset_index(drop=True)

    lag_columns = [f"brent_weekly_log_return_lag_{lag}" for lag in range(LAGS, 0, -1)]
    ar_midas_predictors = ["cpi_mom_lag_1", *lag_columns]
    backtest = []
    for index in range(len(history)):
        row = history.iloc[[index]]
        train = history.iloc[:index].dropna(subset=["actual_cpi_mom_percent"])
        if len(train) < MIN_TRAIN:
            continue
        record = {
            "target_month": row["target_month"].iloc[0],
            "actual_cpi_mom_percent": float(row["actual_cpi_mom_percent"].iloc[0]),
            "training_months": len(train),
        }
        midas_train = train.dropna(subset=lag_columns)
        if row[lag_columns].notna().all(axis=None) and len(midas_train) >= MIN_TRAIN:
            record["midas_ridge"] = fit_predict(midas_train, row, lag_columns)
            record["midas_training_months"] = len(midas_train)
        else:
            record["midas_ridge"] = np.nan
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
        backtest.append(record)

    predictions = pd.DataFrame(backtest)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(OUTPUT_DIR / "midas_backtest_predictions.csv", index=False)
    metric_rows = []
    for model_name in ["cpi_only_ridge", "midas_ridge", "ar_midas_ridge"]:
        if model_name in predictions:
            comparable = predictions.dropna(
                subset=["actual_cpi_mom_percent", model_name]
            )
            if not comparable.empty:
                metric_rows.append(evaluate(comparable, model_name))
    metrics = pd.DataFrame(metric_rows)
    metrics.to_csv(OUTPUT_DIR / "midas_backtest_metrics.csv", index=False)

    now = data_cutoff
    current_month = now.tz_localize(None).to_period("M").to_timestamp()
    current_weekly = weekly_returns(brent, now, respect_available_at=True)
    current_brent = brent_known_as_of(brent, now)
    current = pd.DataFrame(
        [make_row(current_month, labels, current_weekly, now, require_released_lag=True)]
    )
    cpi_lag_month = current_month - pd.DateOffset(months=1)
    cpi_lag_label = labels.loc[labels["target_month"] == cpi_lag_month]
    train = history.dropna(subset=["actual_cpi_mom_percent"])
    cpi_train = train.dropna(subset=["cpi_mom_lag_1"])
    midas_train = train.dropna(subset=lag_columns)
    latest_actual_month = labels["target_month"].max()
    current_result = {
        "target_month": current_month.strftime("%Y-%m"),
        "cutoff_at_utc": now.isoformat(),
        "processing_started_at_utc": processing_started_at.isoformat(),
        "training_months": len(train),
        "latest_actual_cpi_month": latest_actual_month.strftime("%Y-%m"),
        "cpi_only_ridge_nowcast_mom_percent": None,
        "midas_ridge_nowcast_mom_percent": None,
        "ar_midas_ridge_nowcast_mom_percent": None,
        "midas_status": "insufficient labeled CPI training months",
        "method": "ridge U-MIDAS: 12 weekly Brent log returns; optional CPI-lag AR-MIDAS",
        "evaluation_note": "Historical scores use month-end calendar cutoffs, not archived real-time vintages.",
        "availability_policy": (
            "Each source is filtered independently by its own available_at/"
            "label_available_at timestamp; inputs arriving after cutoff are excluded."
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
        "cpi_lag_month": cpi_lag_month.strftime("%Y-%m"),
        "cpi_lag_available_at_utc": (
            cpi_lag_label["label_available_at"].max().isoformat()
            if not cpi_lag_label.empty
            else None
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
        current_result["midas_ridge_nowcast_mom_percent"] = fit_predict(
            midas_train, current, lag_columns
        )
        current_result["midas_training_months"] = len(midas_train)
        current_result["midas_status"] = "ok"
    ar_midas_train = midas_train.dropna(subset=["cpi_mom_lag_1"])
    if (
        current[lag_columns].notna().all(axis=None)
        and pd.notna(current["cpi_mom_lag_1"].iloc[0])
        and len(ar_midas_train) >= MIN_TRAIN
    ):
        current_result["ar_midas_ridge_nowcast_mom_percent"] = fit_predict(
            ar_midas_train, current, ar_midas_predictors
        )
    if len(train) >= MIN_TRAIN and current_result["midas_status"] != "ok":
        current_result["midas_status"] = "insufficient complete Brent lag history"
    processing_completed_at = pd.Timestamp.now(tz="UTC")
    current_result["processing_completed_at_utc"] = processing_completed_at.isoformat()
    current_result["processing_duration_seconds"] = round(
        time.perf_counter() - processing_started_clock, 3
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
