"""Compare CPI/Brent baselines with point-in-time-safe multi-source features.

This is a research diagnostic. It refuses to forward-fill features whose stored
availability is after the target information cutoff and writes coverage so a
poorly versioned source cannot silently improve a historical score.
"""

from __future__ import annotations

import json
import os
import resource
import sys
import time
from datetime import date, datetime, time as daytime
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from trino.dbapi import connect

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "forecasting"))

from evaluation_protocol import paired_model_comparison  # noqa: E402
from midas_nowcast import (  # noqa: E402
    HISTORICAL_BRENT_PUBLICATION_LAG_DAYS,
    HISTORICAL_CPI_RELEASE_DAY,
    LAGS,
    MIN_TRAIN,
    RAW_DATA_DIR,
    VIETNAM_TZ,
    apply_nso_release_calendar,
    as_utc,
    fit_ar1,
    historical_weekly_returns,
    load_nso_release_calendar,
    make_row,
    read_table,
)

OUT = ROOT / "forecasting" / "outputs"
BASE_PREDICTORS = ["cpi_mom_lag_1", *[
    f"brent_weekly_log_return_lag_{lag}" for lag in range(LAGS, 0, -1)
]]
MACRO_MAP = {
    "brent_monthly": "brent_usd_per_barrel_avg_mtd_asof",
    "usd_vnd": "vnd_per_usd_avg_mtd_asof",
    "fuel_ron95": "fuel_ron95_vnd_per_liter_avg_mtd_asof",
    "fuel_e10": "fuel_e10_ron95_vnd_per_liter_avg_mtd_asof",
    "iip_growth": "iip_growth_latest_prior_year_asof",
}
SOURCE_FLAGS = ["brent_asof_eligible", "fx_asof_eligible", "fuel_asof_eligible", "iip_asof_eligible"]
MACRO_PREDICTORS = [*MACRO_MAP, *[f"missing_{flag}" for flag in SOURCE_FLAGS]]


def fetch_features() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    conn = connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "nowcpi"),
        catalog="delta",
        schema="gold",
    )
    try:
        labels = read_table(conn, "gold_nowcasting_labels")
        brent = read_table(conn, "gold_brent_daily")
        features = read_table(conn, "gold_nowcasting_features_pit")
    finally:
        conn.close()
    return labels, brent, features


def feature_asof_row(feature: pd.Series | None, target_cutoff: pd.Timestamp) -> dict:
    row = {column: np.nan for column in [*MACRO_MAP, *[f"missing_{x}" for x in SOURCE_FLAGS]]}
    if feature is None or pd.isna(feature.get("asof_cutoff_ts")):
        for flag in SOURCE_FLAGS:
            row[f"missing_{flag}"] = 1.0
        return row
    if not bool(feature.get("asof_check_passed", False)):
        for flag in SOURCE_FLAGS:
            row[f"missing_{flag}"] = 1.0
        return row
    feature_cutoff = as_utc(pd.Timestamp(feature["asof_cutoff_ts"]))
    # The feature table itself must have been assembled for a cutoff no later
    # than the forecasting origin. This is separate from source availability.
    if feature_cutoff > as_utc(target_cutoff):
        for flag in SOURCE_FLAGS:
            row[f"missing_{flag}"] = 1.0
        return row
    source_available_columns = {
        "brent_asof_eligible": "brent_available_at_asof",
        "fx_asof_eligible": "fx_available_at_asof",
        "fuel_asof_eligible": "fuel_available_at_asof",
        "iip_asof_eligible": "iip_available_at_asof",
    }
    value_columns = {
        "brent_monthly": "brent_usd_per_barrel_avg_mtd_asof",
        "usd_vnd": "vnd_per_usd_avg_mtd_asof",
        "fuel_ron95": "fuel_ron95_vnd_per_liter_avg_mtd_asof",
        "fuel_e10": "fuel_e10_ron95_vnd_per_liter_avg_mtd_asof",
        "iip_growth": "iip_growth_latest_prior_year_asof",
    }
    flag_to_macro = {
        "brent_asof_eligible": "brent_monthly",
        "fx_asof_eligible": "usd_vnd",
        "fuel_asof_eligible": "fuel_e10",
        "iip_asof_eligible": "iip_growth",
    }
    for flag, available_column in source_available_columns.items():
        eligible = bool(feature.get(flag, False))
        available = feature.get(available_column)
        if eligible and pd.notna(available):
            eligible = as_utc(pd.Timestamp(available)) <= as_utc(target_cutoff)
        row[f"missing_{flag}"] = 0.0 if eligible else 1.0
        macro_name = flag_to_macro[flag]
        value = feature.get(value_columns[macro_name])
        row[macro_name] = float(value) if eligible and pd.notna(value) else np.nan
    return row


def soft_threshold(value: float, threshold: float) -> float:
    return float(np.sign(value) * max(abs(value) - threshold, 0.0))


def fit_elastic_net(
    train: pd.DataFrame,
    test: pd.DataFrame,
    predictors: list[str],
    alpha: float,
    l1_ratio: float,
) -> float:
    x = train[predictors].to_numpy(dtype=float)
    xt = test[predictors].to_numpy(dtype=float)
    medians = np.array([
        float(np.median(column[np.isfinite(column)]))
        if np.isfinite(column).any() else 0.0
        for column in x.T
    ])
    x = np.where(np.isfinite(x), x, medians)
    xt = np.where(np.isfinite(xt), xt, medians)
    means = x.mean(axis=0)
    scales = x.std(axis=0)
    scales = np.where(scales > 1e-12, scales, 1.0)
    x = (x - means) / scales
    xt = (xt - means) / scales
    y = train["actual_cpi_mom_percent"].to_numpy(dtype=float)
    intercept = float(y.mean())
    yc = y - intercept
    beta = np.zeros(x.shape[1], dtype=float)
    n = max(len(y), 1)
    l1 = alpha * l1_ratio
    l2 = alpha * (1.0 - l1_ratio)
    norms = np.sum(x * x, axis=0) / n + l2
    for _ in range(80):
        max_change = 0.0
        for j in range(x.shape[1]):
            residual = yc - x @ beta + x[:, j] * beta[j]
            rho = float(np.dot(x[:, j], residual) / n)
            updated = soft_threshold(rho, l1) / max(norms[j], 1e-12)
            max_change = max(max_change, abs(updated - beta[j]))
            beta[j] = updated
        if max_change < 1e-8:
            break
    return float(intercept + (xt @ beta).item())


def fit_ridge(train: pd.DataFrame, test: pd.DataFrame, predictors: list[str], alpha: float = 10.0) -> float:
    x = train[predictors].to_numpy(dtype=float)
    xt = test[predictors].to_numpy(dtype=float)
    medians = np.array([
        float(np.median(column[np.isfinite(column)]))
        if np.isfinite(column).any() else 0.0
        for column in x.T
    ])
    x = np.where(np.isfinite(x), x, medians)
    xt = np.where(np.isfinite(xt), xt, medians)
    mean = x.mean(axis=0)
    scale = x.std(axis=0)
    scale = np.where(scale > 1e-12, scale, 1.0)
    x = np.column_stack([np.ones(len(x)), (x - mean) / scale])
    xt = np.column_stack([np.ones(len(xt)), (xt - mean) / scale])
    penalty = np.eye(x.shape[1]) * alpha
    penalty[0, 0] = 0.0
    beta = np.linalg.solve(x.T @ x + penalty, x.T @ train["actual_cpi_mom_percent"].to_numpy(float))
    return float((xt @ beta).item())


def usable_predictors(train: pd.DataFrame, predictors: list[str]) -> list[str]:
    """Drop features wholly missing in this training slice; never inspect future folds."""
    return [column for column in predictors if train[column].notna().any()]


def select_elastic_net(
    train: pd.DataFrame,
    test: pd.DataFrame,
    predictors: list[str],
    alphas: tuple[float, ...] = (0.1, 1.0),
    l1_ratios: tuple[float, ...] = (0.2, 0.8),
) -> tuple[float, float, float, int]:
    train = train.reset_index(drop=True)
    # A compact inner window bounds the daily research task while retaining
    # expanding, time-ordered validation that excludes the outer target.
    start = max(MIN_TRAIN, len(train) - 6)
    candidates = []
    for alpha in alphas:
        for l1_ratio in l1_ratios:
            errors = []
            for i in range(start, len(train)):
                inner = train.iloc[:i].dropna(subset=["actual_cpi_mom_percent", "cpi_mom_lag_1"])
                valid = train.iloc[[i]]
                if len(inner) < MIN_TRAIN or pd.isna(valid["cpi_mom_lag_1"].iloc[0]):
                    continue
                inner_predictors = usable_predictors(inner, predictors)
                pred = fit_elastic_net(inner, valid, inner_predictors, alpha, l1_ratio)
                errors.append(abs(float(valid["actual_cpi_mom_percent"].iloc[0]) - pred))
            if len(errors) >= 3:
                candidates.append((float(np.mean(errors)), alpha, l1_ratio, len(errors)))
    if not candidates:
        alpha = alphas[0]
        l1_ratio = l1_ratios[0]
        fallback_predictors = usable_predictors(train, predictors)
        return fit_elastic_net(train, test, fallback_predictors, alpha, l1_ratio), alpha, l1_ratio, 0
    _, alpha, l1_ratio, count = min(candidates)
    final_predictors = usable_predictors(train, predictors)
    return fit_elastic_net(train, test, final_predictors, alpha, l1_ratio), alpha, l1_ratio, count


def fit_huber(
    train: pd.DataFrame,
    test: pd.DataFrame,
    predictors: list[str],
    alpha: float,
    epsilon: float = 1.345,
) -> float:
    """Huber M-estimator via iteratively reweighted ridge regression."""
    x = train[predictors].to_numpy(dtype=float)
    xt = test[predictors].to_numpy(dtype=float)
    medians = np.array([
        float(np.median(column[np.isfinite(column)]))
        if np.isfinite(column).any() else 0.0
        for column in x.T
    ])
    x = np.where(np.isfinite(x), x, medians)
    xt = np.where(np.isfinite(xt), xt, medians)
    mean = x.mean(axis=0)
    scale_x = np.where(x.std(axis=0) > 1e-12, x.std(axis=0), 1.0)
    x = np.column_stack([np.ones(len(x)), (x - mean) / scale_x])
    xt = np.column_stack([np.ones(len(xt)), (xt - mean) / scale_x])
    y = train["actual_cpi_mom_percent"].to_numpy(dtype=float)
    penalty = np.eye(x.shape[1]) * alpha
    penalty[0, 0] = 0.0
    beta = np.linalg.solve(x.T @ x + penalty, x.T @ y)
    for _ in range(40):
        residual = y - x @ beta
        center = float(np.median(residual))
        robust_scale = float(np.median(np.abs(residual - center)) / 0.6745)
        threshold = epsilon * max(robust_scale, 1e-8)
        weights = np.ones_like(residual)
        large = np.abs(residual) > threshold
        weights[large] = threshold / np.abs(residual[large])
        root_w = np.sqrt(weights)
        xw = x * root_w[:, None]
        yw = y * root_w
        updated = np.linalg.solve(xw.T @ xw + penalty, xw.T @ yw)
        if np.max(np.abs(updated - beta)) < 1e-8:
            beta = updated
            break
        beta = updated
    return float((xt @ beta).item())


def select_huber(
    train: pd.DataFrame, test: pd.DataFrame, predictors: list[str]
) -> tuple[float, float, int]:
    train = train.reset_index(drop=True)
    start = max(MIN_TRAIN, len(train) - 6)
    candidates = []
    for alpha in (0.1, 1.0, 10.0):
        errors = []
        for i in range(start, len(train)):
            inner = train.iloc[:i].dropna(subset=["actual_cpi_mom_percent", "cpi_mom_lag_1"])
            valid = train.iloc[[i]]
            if len(inner) < MIN_TRAIN or pd.isna(valid["cpi_mom_lag_1"].iloc[0]):
                continue
            selected = usable_predictors(inner, predictors)
            forecast = fit_huber(inner, valid, selected, alpha)
            errors.append(abs(float(valid["actual_cpi_mom_percent"].iloc[0]) - forecast))
        if len(errors) >= 3:
            candidates.append((float(np.mean(errors)), alpha, len(errors)))
    alpha = min(candidates)[1] if candidates else 1.0
    count = min(candidates)[2] if candidates else 0
    selected = usable_predictors(train, predictors)
    return fit_huber(train, test, selected, alpha), alpha, count


def main() -> None:
    started = time.perf_counter()
    OUT.mkdir(parents=True, exist_ok=True)
    labels, brent, features = fetch_features()
    labels = labels.loc[labels["country"].astype(str).str.upper() == "VNM"].copy()
    brent = brent.loc[brent["country"].astype(str).str.upper() == "VNM"].copy()
    labels["target_month"] = pd.to_datetime(labels["target_month"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    labels["target_cpi_mom_percent"] = pd.to_numeric(labels["target_cpi_mom_percent"], errors="coerce")
    labels["label_available_at"] = pd.to_datetime(labels["label_available_at"], errors="coerce", utc=True)
    labels = labels.dropna(subset=["target_month", "target_cpi_mom_percent", "label_available_at"])
    labels = labels.drop_duplicates("target_month", keep="last").sort_values("target_month").reset_index(drop=True)
    calendar, snapshot = load_nso_release_calendar()
    labels = apply_nso_release_calendar(labels, calendar)
    features["target_month"] = pd.to_datetime(features["target_month"], errors="coerce").dt.to_period("M").dt.to_timestamp()
    feature_map = {row.target_month: row._asdict() for row in features.itertuples(index=False)}
    history_rows = []
    audit_rows = []
    for label in labels.itertuples(index=False):
        month = label.target_month
        issue_date = getattr(label, "label_source_release_date", None)
        if issue_date is not None and pd.notna(issue_date):
            issue_day = date.fromisoformat(str(issue_date))
            cutoff = pd.Timestamp(datetime.combine(issue_day, daytime.min, tzinfo=VIETNAM_TZ)).tz_convert("UTC")
        else:
            cutoff = (month + pd.offsets.MonthEnd(0) + pd.Timedelta(days=1, hours=23, minutes=59, seconds=59)).tz_localize("UTC")
        weekly = historical_weekly_returns(brent, month, cutoff)
        row = make_row(month, labels, weekly, cutoff, float(label.target_cpi_mom_percent), historical_origin=True)
        feature = feature_map.get(month)
        macro = feature_asof_row(pd.Series(feature) if feature else None, cutoff)
        row.update(macro)
        row["feature_store_row_present"] = feature is not None
        row["target_cutoff_at"] = cutoff.isoformat()
        row["feature_store_asof_cutoff_at"] = feature.get("asof_cutoff_ts") if feature else None
        row["feature_store_asof_check_passed"] = bool(feature.get("asof_check_passed", False)) if feature else False
        row["asof_feature_source_coverage_pct"] = float(feature.get("asof_feature_source_coverage_pct", 0.0)) if feature else 0.0
        row["target_month"] = month
        history_rows.append(row)
        audit_rows.append({
            "target_month": month,
            "target_cutoff_at": cutoff,
            "feature_store_row_present": feature is not None,
            "feature_store_cutoff_not_after_target_cutoff": bool(feature and as_utc(pd.Timestamp(feature["asof_cutoff_ts"])) <= as_utc(cutoff)),
            "source_feature_coverage_pct": row["asof_feature_source_coverage_pct"],
            **{flag: bool(feature.get(flag, False)) if feature else False for flag in SOURCE_FLAGS},
            "nso_issue_date": issue_date,
        })
    history = pd.DataFrame(history_rows).sort_values("target_month").reset_index(drop=True)
    pd.DataFrame(audit_rows).to_csv(OUT / "feature_ablation_asof_audit.csv", index=False)
    # Explicitly record this run's PIT source coverage and vintage limitation.
    coverage_rows = []
    for source, flag in zip(("Brent", "USD/VND", "Fuel", "IIP"), SOURCE_FLAGS):
        current_flag = f"missing_{flag}"
        eligible = history[current_flag].fillna(1).eq(0)
        coverage_rows.append({
            "source": source,
            "eligible_target_months": int(eligible.sum()),
            "target_months": len(history),
            "eligible_pct": round(float(eligible.mean() * 100), 2) if len(history) else 0.0,
            "point_in_time_rule": "source available_at <= Gold as-of cutoff <= forecast cutoff; observation date <= cutoff",
            "strict_historical_vintages_available": False,
        })
    pd.DataFrame(coverage_rows).to_csv(OUT / "feature_ablation_source_coverage.csv", index=False)

    predictions = []
    model_names = [
        "ar1_ols",
        "cpi_only_ridge",
        "u_midas_brent_ridge",
        "ar_midas_ridge",
        "midas_elastic_net",
        "midas_lasso",
        "midas_huber",
        "full_multisource_ridge",
        "full_multisource_elastic_net",
    ]
    runtime = {name: {"fit_predict_seconds": 0.0, "failed_folds": 0, "completed_folds": 0, "max_fit_predict_seconds": 0.0} for name in model_names}
    failures = []
    brent_predictors = [column for column in BASE_PREDICTORS if column.startswith("brent_")]
    for i, test in history.iterrows():
        train = history.iloc[:i].dropna(subset=["actual_cpi_mom_percent", "cpi_mom_lag_1"])
        if len(train) < MIN_TRAIN or pd.isna(test.get("cpi_mom_lag_1")):
            continue
        record = {
            "target_month": test["target_month"],
            "actual_cpi_mom_percent": test["actual_cpi_mom_percent"],
            "naive_last_cpi": test["cpi_mom_lag_1"],
        }
        candidates = {
            "ar1_ols": ("ar1", ["cpi_mom_lag_1"], ["cpi_mom_lag_1"]),
            "cpi_only_ridge": ("ridge", ["cpi_mom_lag_1"], ["cpi_mom_lag_1"]),
            "u_midas_brent_ridge": ("ridge", brent_predictors, brent_predictors),
            "ar_midas_ridge": ("ridge", BASE_PREDICTORS, BASE_PREDICTORS),
            "midas_elastic_net": ("elastic", BASE_PREDICTORS, BASE_PREDICTORS),
            "midas_lasso": ("lasso", BASE_PREDICTORS, BASE_PREDICTORS),
            "midas_huber": ("huber", BASE_PREDICTORS, BASE_PREDICTORS),
            "full_multisource_ridge": ("ridge", [*BASE_PREDICTORS, *MACRO_PREDICTORS], BASE_PREDICTORS),
            "full_multisource_elastic_net": ("elastic", [*BASE_PREDICTORS, *MACRO_PREDICTORS], BASE_PREDICTORS),
        }
        for model_name, (method, predictors, required_test_columns) in candidates.items():
            tick = time.perf_counter()
            try:
                if test[required_test_columns].isna().any():
                    raise ValueError("required features are incomplete at the as-of cutoff")
                if method == "ar1":
                    fit_train = train.dropna(subset=["cpi_mom_lag_1", "actual_cpi_mom_percent"])
                elif model_name in ("cpi_only_ridge",):
                    fit_train = train.dropna(subset=["cpi_mom_lag_1", "actual_cpi_mom_percent"])
                elif model_name == "u_midas_brent_ridge":
                    fit_train = train.dropna(subset=[*brent_predictors, "actual_cpi_mom_percent"])
                elif model_name.startswith("full_multisource_"):
                    fit_train = train.dropna(subset=["cpi_mom_lag_1", "actual_cpi_mom_percent"])
                else:
                    fit_train = train.dropna(subset=[*BASE_PREDICTORS, "actual_cpi_mom_percent"])
                if len(fit_train) < MIN_TRAIN:
                    raise ValueError("fewer than minimum training folds after as-of feature checks")
                selected_predictors = usable_predictors(fit_train, predictors)
                if method == "ar1":
                    pred = fit_ar1(fit_train, test.to_frame().T)
                elif method == "elastic":
                    pred, alpha, l1_ratio, val_count = select_elastic_net(fit_train, test.to_frame().T, predictors)
                    record[f"{model_name}_alpha"] = alpha
                    record[f"{model_name}_l1_ratio"] = l1_ratio
                    record[f"{model_name}_validation_origins"] = val_count
                elif method == "lasso":
                    pred, alpha, _, val_count = select_elastic_net(
                        fit_train, test.to_frame().T, predictors,
                        alphas=(0.1, 1.0), l1_ratios=(1.0,),
                    )
                    record[f"{model_name}_alpha"] = alpha
                    record[f"{model_name}_l1_ratio"] = 1.0
                    record[f"{model_name}_validation_origins"] = val_count
                elif method == "huber":
                    pred, alpha, val_count = select_huber(fit_train, test.to_frame().T, predictors)
                    record[f"{model_name}_alpha"] = alpha
                    record[f"{model_name}_epsilon"] = 1.345
                    record[f"{model_name}_validation_origins"] = val_count
                else:
                    pred = fit_ridge(fit_train, test.to_frame().T, selected_predictors)
                record[model_name] = pred
                runtime[model_name]["completed_folds"] += 1
            except Exception as exc:  # keep failures visible per configuration/fold
                runtime[model_name]["failed_folds"] += 1
                failures.append({"model": model_name, "target_month": test["target_month"], "error": f"{type(exc).__name__}: {exc}"})
                record[model_name] = np.nan
            elapsed = time.perf_counter() - tick
            runtime[model_name]["fit_predict_seconds"] += elapsed
            runtime[model_name]["max_fit_predict_seconds"] = max(runtime[model_name]["max_fit_predict_seconds"], elapsed)
        predictions.append(record)
    pred = pd.DataFrame(predictions)
    pred.to_csv(OUT / "feature_ablation_predictions.csv", index=False)
    pd.DataFrame(failures, columns=["model", "target_month", "error"]).to_csv(OUT / "feature_ablation_failures.csv", index=False)
    error_frames = []
    for model in model_names:
        if model not in pred:
            continue
        scored = pred.dropna(subset=["actual_cpi_mom_percent", model, "naive_last_cpi"]).copy()
        if scored.empty:
            continue
        scored["model"] = model
        scored["proposed_abs_error_pp"] = (scored["actual_cpi_mom_percent"] - scored[model]).abs()
        scored["naive_abs_error_pp"] = (scored["actual_cpi_mom_percent"] - scored["naive_last_cpi"]).abs()
        scored["error_gap_pp_proposed_minus_naive"] = scored["proposed_abs_error_pp"] - scored["naive_abs_error_pp"]
        scored["failure_pattern"] = np.where(
            np.sign(scored["actual_cpi_mom_percent"]) != np.sign(scored[model]),
            "wrong_direction",
            np.where(scored["proposed_abs_error_pp"] > scored["naive_abs_error_pp"], "worse_than_naive", "large_absolute_error"),
        )
        error_frames.append(scored.sort_values("proposed_abs_error_pp", ascending=False).head(3))
    pd.concat(error_frames, ignore_index=True).to_csv(OUT / "feature_ablation_error_cases.csv", index=False) if error_frames else pd.DataFrame().to_csv(OUT / "feature_ablation_error_cases.csv", index=False)

    model_columns = ["naive_last_cpi", *model_names]
    score = pred.dropna(subset=["actual_cpi_mom_percent", *model_columns]).sort_values("target_month") if not pred.empty else pred
    metric_rows = []
    for window_name, window in (("all_common_origins", score), ("latest_36", score.tail(36)), ("latest_24", score.tail(24)), ("latest_9_descriptive_only", score.tail(9))):
        for model in model_columns:
            if window.empty:
                continue
            err = window["actual_cpi_mom_percent"].to_numpy(float) - window[model].to_numpy(float)
            metric_rows.append({
                "configuration": model,
                "evaluation_window": window_name,
                "origins": len(window),
                "mae_pp": float(np.mean(np.abs(err))),
                "rmse_pp": float(np.sqrt(np.mean(err ** 2))),
                "directional_accuracy_pct": float(np.mean(np.sign(window["actual_cpi_mom_percent"]) == np.sign(window[model])) * 100),
                "point_in_time_mode": "asof_feature_store_with_source_availability_gates; historical_source_vintages_unavailable",
                "real_time_performance_claim_allowed": False,
            })
    pd.DataFrame(metric_rows).to_csv(OUT / "feature_ablation_metrics.csv", index=False)
    comparisons = []
    baselines = {
        "ar1_ols": ["naive_last_cpi"],
        "cpi_only_ridge": ["naive_last_cpi", "ar1_ols"],
        "u_midas_brent_ridge": ["naive_last_cpi", "ar1_ols", "ar_midas_ridge"],
        "ar_midas_ridge": ["naive_last_cpi", "ar1_ols"],
        "midas_elastic_net": ["naive_last_cpi", "ar_midas_ridge"],
        "midas_lasso": ["naive_last_cpi", "ar_midas_ridge"],
        "midas_huber": ["naive_last_cpi", "ar_midas_ridge"],
        "full_multisource_ridge": ["naive_last_cpi", "ar_midas_ridge"],
        "full_multisource_elastic_net": ["naive_last_cpi", "ar_midas_ridge", "full_multisource_ridge"],
    }
    for proposed, baseline_set in baselines.items():
        for baseline in baseline_set:
            common = pred.dropna(subset=["actual_cpi_mom_percent", proposed, baseline]).sort_values("target_month")
            for name, window in (("all_common", common), ("latest_36", common.tail(36)), ("latest_24", common.tail(24)), ("latest_9_descriptive_only", common.tail(9))):
                if window.empty:
                    continue
                result = paired_model_comparison(window, proposed, baseline)
                result.update({"evaluation_window": name, "evaluation_mode": "asof_feature_store_pseudo_vintage", "real_time_performance_claim_allowed": False})
                comparisons.append(result)
    pd.DataFrame(comparisons).to_csv(OUT / "feature_ablation_paired_comparisons.csv", index=False)
    elapsed = time.perf_counter() - started
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    pd.DataFrame([{**{"model": model}, **values, "max_rss_kb": int(rss), "script_wall_seconds": round(elapsed, 3), "strict_vintage_available": False} for model, values in runtime.items()]).to_csv(OUT / "feature_ablation_runtime.csv", index=False)
    protocol = {
        "protocol_version": 1,
        "configs": ["naive_last_cpi", "ar1_ols", "cpi_only_ridge", "u_midas_brent_ridge", "fixed_ar_midas_ridge", "midas_elastic_net", "midas_lasso", "midas_huber", "full_multisource_ridge", "full_multisource_elastic_net"],
        "full_sources": ["Brent", "USD/VND", "RON95 fuel", "E10 RON95 fuel", "annual IIP"],
        "elastic_net_grid": {"alpha": [0.1, 1.0], "l1_ratio": [0.2, 0.8], "inner_validation_origins_max": 6},
        "lasso_grid": {"alpha": [0.1, 1.0], "l1_ratio": [1.0], "inner_validation_origins_max": 6},
        "huber_grid": {"alpha": [0.1, 1.0, 10.0], "epsilon": 1.345, "inner_validation_origins_max": 6},
        "selection": "expanding validation folds within each outer fold; preprocessing fitted only on training data",
        "asof_join": "feature-store cutoff <= forecast cutoff; each source available_at <= both cutoffs; observation date/year <= cutoff",
        "missingness": "train-only median imputation plus explicit per-source availability indicators",
        "primary_metric": "MAE CPI MoM percentage points",
        "guardrail_metric": "RMSE CPI MoM percentage points",
        "operational_metrics": "per-model fit/predict seconds, max fold seconds, failures, completed folds, process peak RSS",
        "nso_calendar_snapshot": snapshot,
        "source_vintages": "not archived before first raw collection; results are pseudo-vintage diagnostics",
        "real_time_performance_claim_allowed": False,
    }
    (OUT / "feature_ablation_protocol.json").write_text(json.dumps(protocol, indent=2), encoding="utf-8")
    print(f"Feature ablation complete: {len(pred)} folds, {len(failures)} model-fold failures, wall={elapsed:.2f}s, max_rss={rss} KB")
    print(f"As-of source coverage: {pd.DataFrame(coverage_rows).to_dict(orient='records')}")


if __name__ == "__main__":
    main()
