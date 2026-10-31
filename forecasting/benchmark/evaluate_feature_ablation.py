"""Compare CPI/Brent baselines with point-in-time-safe multi-source features.

This is a research diagnostic. It refuses to forward-fill features whose stored
availability is after the target information cutoff and writes coverage so a
poorly versioned source cannot silently improve a historical score.
"""

from __future__ import annotations

import json
import hashlib
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
from asof_features import (  # noqa: E402
    MACRO_MAP,
    SOURCE_ABLATION_GROUPS,
    SOURCE_FLAGS,
    feature_asof_row,
    select_feature_vintage,
)
from multisource_nowcast import (  # noqa: E402
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
MACRO_PREDICTORS = [*MACRO_MAP, *[f"missing_{flag}" for flag in SOURCE_FLAGS]]
BRENT_WEEKLY_PREDICTORS = [
    column for column in BASE_PREDICTORS if column.startswith("brent_weekly_")
]
MULTISOURCE_ABLATION_GROUPS = {
    "brent": (
        [*BRENT_WEEKLY_PREDICTORS, "brent_monthly"],
        "missing_brent_asof_eligible",
    ),
    **{
        source: group
        for source, group in SOURCE_ABLATION_GROUPS.items()
        if source != "brent_monthly"
    },
}


def source_group_available(frame: pd.DataFrame | pd.Series, source: str) -> pd.Series | bool:
    """Return source availability for a target row or training frame."""
    values, availability_indicator = MULTISOURCE_ABLATION_GROUPS[source]
    if source == "brent":
        weekly_present = frame[BRENT_WEEKLY_PREDICTORS].notna().any(axis=1) if isinstance(frame, pd.DataFrame) else frame[BRENT_WEEKLY_PREDICTORS].notna().any()
        monthly_present = (
            frame[availability_indicator].eq(0) & frame["brent_monthly"].notna()
            if isinstance(frame, pd.DataFrame)
            else frame[availability_indicator] == 0 and pd.notna(frame["brent_monthly"])
        )
        return weekly_present | monthly_present
    present = frame[values].notna().any(axis=1) if isinstance(frame, pd.DataFrame) else frame[values].notna().any()
    eligible = frame[availability_indicator].eq(0) if isinstance(frame, pd.DataFrame) else frame[availability_indicator] == 0
    return eligible & present


def persist_model_artifacts(
    predictions: pd.DataFrame,
    metrics: pd.DataFrame,
    specs: dict[str, dict],
    source_hash: str,
    runtime_frame: pd.DataFrame,
    ineligible_frame: pd.DataFrame,
    failures_frame: pd.DataFrame,
) -> tuple[str, list[dict]]:
    """Write a versioned manifest, per-model predictions, and metrics."""
    run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    registry_rows = []
    for model_name, spec in specs.items():
        version_seed = json.dumps(
            {"source_hash": source_hash, **spec}, sort_keys=True, default=str
        )
        model_version = "v1-" + hashlib.sha256(version_seed.encode()).hexdigest()[:12]
        model_dir = OUT / "models" / model_name / model_version
        model_dir.mkdir(parents=True, exist_ok=True)
        prediction_columns = [
            column for column in predictions.columns
            if column in {"target_month", "actual_cpi_mom_percent", "naive_last_cpi", model_name}
            or column.startswith(f"{model_name}_")
        ]
        if model_name == "naive_last_cpi":
            prediction_columns = ["target_month", "actual_cpi_mom_percent", model_name]
        predictions[prediction_columns].to_csv(model_dir / "predictions.csv", index=False)
        model_metrics = metrics.loc[metrics["configuration"] == model_name].copy()
        model_metrics.to_csv(model_dir / "metrics.csv", index=False)
        model_runtime = runtime_frame.loc[runtime_frame["model"] == model_name].copy()
        model_runtime.to_csv(model_dir / "runtime.csv", index=False)
        model_ineligible = ineligible_frame.loc[
            ineligible_frame["model"] == model_name
        ].copy() if not ineligible_frame.empty else ineligible_frame
        model_ineligible.to_csv(model_dir / "ineligible_folds.csv", index=False)
        model_failures = failures_frame.loc[
            failures_frame["model"] == model_name
        ].copy() if not failures_frame.empty else failures_frame
        model_failures.to_csv(model_dir / "failures.csv", index=False)
        runtime_values = model_runtime.iloc[0].to_dict() if not model_runtime.empty else {}
        manifest = {
            "model_name": model_name,
            "model_version": model_version,
            "evaluation_run_id": run_id,
            "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
            "source_code_sha256": source_hash,
            **spec,
            "prediction_file": "predictions.csv",
            "metrics_file": "metrics.csv",
            "runtime_file": "runtime.csv",
            "ineligible_folds_file": "ineligible_folds.csv",
            "failures_file": "failures.csv",
            "operational_metrics": runtime_values,
            "metrics_protocol": "paired rolling-origin comparisons; MAE primary, RMSE guardrail, MAPE excluding abs(actual)<0.05pp and sMAPE supplementary",
            "point_in_time_claim": "historical source vintages are incomplete; diagnostic only",
        }
        (model_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, default=str), encoding="utf-8"
        )
        registry_rows.append({
            "model_name": model_name,
            "model_version": model_version,
            "role": spec["role"],
            "model_family": spec["model_family"],
            "predictors": json.dumps(spec.get("predictors", [])),
            "hyperparameters": json.dumps(spec.get("hyperparameters", {}), sort_keys=True),
            "artifact_path": str(model_dir.relative_to(OUT)),
            "runtime_failures": int(runtime_values.get("failed_folds", 0)),
            "ineligible_folds": int(runtime_values.get("ineligible_folds", 0)),
            "completed_folds": int(runtime_values.get("completed_folds", 0)),
            "fit_predict_seconds": float(runtime_values.get("fit_predict_seconds", 0.0)),
        })
    registry = pd.DataFrame(registry_rows)
    registry.to_csv(OUT / "feature_ablation_model_registry.csv", index=False)
    run_manifest = {
        "evaluation_run_id": run_id,
        "generated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "primary_model": "full_multisource_ridge",
        "primary_model_version": registry.loc[
            registry["model_name"] == "full_multisource_ridge", "model_version"
        ].iloc[0],
        "source_code_sha256": source_hash,
        "models": registry_rows,
    }
    (OUT / "feature_ablation_run_manifest.json").write_text(
        json.dumps(run_manifest, indent=2), encoding="utf-8"
    )
    return run_id, registry_rows


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
        # The PIT projection is a single current Gold build. Historical folds
        # must instead select immutable snapshots captured no later than each
        # forecast cutoff; otherwise using the current row either leaks future
        # data or makes every historical macro feature ineligible.
        features = read_table(conn, "gold_nowcasting_feature_vintages")
    finally:
        conn.close()
    return labels, brent, features


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


def evaluate_sourcewise_ablation(history: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare AR-MIDAS with one added source on exactly the source-known folds.

    A source is not scored unless it has point-in-time values at the test origin
    and at least MIN_TRAIN prior training rows. This prevents a missing source
    from being represented as a measured ablation effect.
    """
    summaries = []
    prediction_rows = []
    for source, (source_values, availability_flag) in SOURCE_ABLATION_GROUPS.items():
        source_predictors = [*source_values, availability_flag]
        eligible = history[availability_flag].eq(0) & history[source_values].notna().any(axis=1)
        candidate_rows = []
        failures = 0
        for index in range(len(history)):
            test = history.iloc[[index]]
            if not bool(eligible.iloc[index]) or test[BASE_PREDICTORS].isna().any(axis=None):
                continue
            train = history.iloc[:index].dropna(
                subset=["actual_cpi_mom_percent", *BASE_PREDICTORS]
            )
            train_source_count = int(
                (train[availability_flag].eq(0) & train[source_values].notna().any(axis=1)).sum()
            )
            if len(train) < MIN_TRAIN or train_source_count < MIN_TRAIN:
                continue
            try:
                base_prediction = fit_ridge(train, test, BASE_PREDICTORS)
                source_prediction = fit_ridge(
                    train, test, [*BASE_PREDICTORS, *source_predictors]
                )
                row = {
                    "source": source,
                    "target_month": test["target_month"].iloc[0],
                    "actual_cpi_mom_percent": float(test["actual_cpi_mom_percent"].iloc[0]),
                    "ar_midas_ridge": base_prediction,
                    "ar_midas_plus_source": source_prediction,
                    "prior_training_rows_with_source": train_source_count,
                }
                candidate_rows.append(row)
                prediction_rows.append(row)
            except Exception:
                failures += 1

        common = pd.DataFrame(candidate_rows)
        result = {
            "source": source,
            "eligible_target_months": int(eligible.sum()),
            "eligible_percent": round(float(eligible.mean() * 100), 2) if len(history) else 0.0,
            "paired_origins": len(common),
            "failed_folds": failures,
            "status": "not_estimable_no_point_in_time_features" if not eligible.any() else (
                "not_estimable_insufficient_training_history" if common.empty else (
                    "research_window_available" if len(common) >= 24 else "descriptive_only_under_24_origins"
                )
            ),
            "baseline_mae_pp": None,
            "source_added_mae_pp": None,
            "mae_delta_pp_source_minus_baseline": None,
            "mae_relative_uplift_pct": None,
            "mae_delta_95ci_low_pp": None,
            "mae_delta_95ci_high_pp": None,
            "dm_pvalue_mae_loss": None,
            "baseline_rmse_pp": None,
            "source_added_rmse_pp": None,
            "point_in_time_history_available": bool(eligible.any()),
            "strict_vintage_performance_claim_allowed": False,
        }
        if not common.empty:
            paired = paired_model_comparison(
                common, "ar_midas_plus_source", "ar_midas_ridge"
            )
            result.update({
                "baseline_mae_pp": paired["baseline_mae_pp"],
                "source_added_mae_pp": paired["proposed_mae_pp"],
                "mae_delta_pp_source_minus_baseline": paired["mae_delta_pp_proposed_minus_baseline"],
                "mae_relative_uplift_pct": paired["mae_relative_uplift_pct_positive_is_better"],
                "mae_delta_95ci_low_pp": paired["mae_delta_95ci_low_pp"],
                "mae_delta_95ci_high_pp": paired["mae_delta_95ci_high_pp"],
                "dm_pvalue_mae_loss": paired["dm_pvalue_mae_loss"],
                "baseline_rmse_pp": paired["baseline_rmse_pp"],
                "source_added_rmse_pp": paired["proposed_rmse_pp"],
            })
        summaries.append(result)
    prediction_columns = [
        "source", "target_month", "actual_cpi_mom_percent", "ar_midas_ridge",
        "ar_midas_plus_source", "prior_training_rows_with_source",
    ]
    return pd.DataFrame(summaries), pd.DataFrame(prediction_rows, columns=prediction_columns)


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
        feature = select_feature_vintage(features, month, cutoff)
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
            "feature_store_cutoff_not_after_target_cutoff": bool(
                feature
                and as_utc(pd.Timestamp(feature["asof_cutoff_ts"])) <= as_utc(cutoff)
                and as_utc(pd.Timestamp(feature["snapshot_recorded_at"])) <= as_utc(cutoff)
            ),
            "selected_feature_snapshot_id": feature.get("feature_snapshot_id") if feature else None,
            "source_feature_coverage_pct": row["asof_feature_source_coverage_pct"],
            **{flag: bool(feature.get(flag, False)) if feature else False for flag in SOURCE_FLAGS},
            "nso_issue_date": issue_date,
        })
    history = pd.DataFrame(history_rows).sort_values("target_month").reset_index(drop=True)
    pd.DataFrame(audit_rows).to_csv(OUT / "feature_ablation_asof_audit.csv", index=False)
    sourcewise_summary, sourcewise_predictions = evaluate_sourcewise_ablation(history)
    sourcewise_summary.to_csv(OUT / "feature_ablation_sourcewise.csv", index=False)
    sourcewise_predictions.to_csv(OUT / "feature_ablation_sourcewise_predictions.csv", index=False)
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
        *[f"multisource_without_{source}" for source in MULTISOURCE_ABLATION_GROUPS],
    ]
    brent_predictors = [column for column in BASE_PREDICTORS if column.startswith("brent_")]
    candidates = {
        "ar1_ols": ("ar1", ["cpi_mom_lag_1"], ["cpi_mom_lag_1"]),
        "cpi_only_ridge": ("ridge", ["cpi_mom_lag_1"], ["cpi_mom_lag_1"]),
        "u_midas_brent_ridge": ("ridge", brent_predictors, brent_predictors),
        "ar_midas_ridge": ("ridge", BASE_PREDICTORS, BASE_PREDICTORS),
        "midas_elastic_net": ("elastic", BASE_PREDICTORS, BASE_PREDICTORS),
        "midas_lasso": ("lasso", BASE_PREDICTORS, BASE_PREDICTORS),
        "midas_huber": ("huber", BASE_PREDICTORS, BASE_PREDICTORS),
        "full_multisource_ridge": (
            "ridge", [*BASE_PREDICTORS, *MACRO_PREDICTORS], BASE_PREDICTORS
        ),
        "full_multisource_elastic_net": (
            "elastic", [*BASE_PREDICTORS, *MACRO_PREDICTORS], BASE_PREDICTORS
        ),
    }
    for source, (source_predictors, _) in MULTISOURCE_ABLATION_GROUPS.items():
        predictors_without_source = [
            *[column for column in BASE_PREDICTORS if column not in source_predictors],
            *[column for column in MACRO_PREDICTORS if column not in source_predictors],
        ]
        model_name = f"multisource_without_{source}"
        candidates[model_name] = (
            "ridge", predictors_without_source, BASE_PREDICTORS
        )
    runtime = {
        name: {
            "fit_predict_seconds": 0.0,
            "failed_folds": 0,
            "ineligible_folds": 0,
            "completed_folds": 0,
            "max_fit_predict_seconds": 0.0,
        }
        for name in model_names
    }
    failures = []
    ineligible_folds = []
    for i, test in history.iterrows():
        train = history.iloc[:i].dropna(subset=["actual_cpi_mom_percent", "cpi_mom_lag_1"])
        if len(train) < MIN_TRAIN or pd.isna(test.get("cpi_mom_lag_1")):
            continue
        record = {
            "target_month": test["target_month"],
            "actual_cpi_mom_percent": test["actual_cpi_mom_percent"],
            "naive_last_cpi": test["cpi_mom_lag_1"],
        }
        for model_name, (method, predictors, required_test_columns) in candidates.items():
            tick = time.perf_counter()
            source_off_group = (
                model_name.removeprefix("multisource_without_")
                if model_name.startswith("multisource_without_")
                else None
            )
            if source_off_group:
                test_source_available = bool(
                    source_group_available(test, source_off_group)
                )
                train_source_rows = int(
                    source_group_available(train, source_off_group).sum()
                )
                if not test_source_available:
                    fit_train = train.iloc[0:0]
                    ineligible_reason = "source_not_available_at_forecast_cutoff"
                elif train_source_rows < MIN_TRAIN:
                    fit_train = train.iloc[0:0]
                    ineligible_reason = "insufficient_point_in_time_training_rows_for_source"
                else:
                    fit_train = train
                    ineligible_reason = None
            elif test[required_test_columns].isna().any():
                fit_train = train.iloc[0:0]
                ineligible_reason = "required_test_features_unavailable_at_cutoff"
            else:
                if (
                    method == "ar1"
                    or model_name == "cpi_only_ridge"
                    or model_name.startswith("full_multisource_")
                    or model_name.startswith("multisource_without_")
                ):
                    fit_train = train.dropna(subset=["cpi_mom_lag_1", "actual_cpi_mom_percent"])
                elif model_name == "u_midas_brent_ridge":
                    fit_train = train.dropna(subset=[*brent_predictors, "actual_cpi_mom_percent"])
                else:
                    fit_train = train.dropna(subset=[*BASE_PREDICTORS, "actual_cpi_mom_percent"])
                ineligible_reason = (
                    "insufficient_training_history_after_feature_eligibility"
                    if len(fit_train) < MIN_TRAIN else None
                )
            if (
                not ineligible_reason
                and len(fit_train) < MIN_TRAIN
            ):
                ineligible_reason = "insufficient_training_history_after_feature_eligibility"
            if ineligible_reason:
                ineligible_folds.append({
                    "model": model_name,
                    "target_month": test["target_month"],
                    "status": "not_evaluated_insufficient_or_unavailable_data",
                    "reason": ineligible_reason,
                    "required_training_rows": MIN_TRAIN,
                    "available_training_rows": len(fit_train),
                })
                runtime[model_name]["ineligible_folds"] += 1
                elapsed = time.perf_counter() - tick
                runtime[model_name]["fit_predict_seconds"] += elapsed
                runtime[model_name]["max_fit_predict_seconds"] = max(
                    runtime[model_name]["max_fit_predict_seconds"], elapsed
                )
                continue
            try:
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
    for model_name in model_names:
        if model_name not in pred:
            pred[model_name] = np.nan
    pred.to_csv(OUT / "feature_ablation_predictions.csv", index=False)
    pd.DataFrame(failures, columns=["model", "target_month", "error"]).to_csv(OUT / "feature_ablation_failures.csv", index=False)
    pd.DataFrame(
        ineligible_folds,
        columns=[
            "model", "target_month", "status", "reason",
            "required_training_rows", "available_training_rows",
        ],
    ).to_csv(OUT / "feature_ablation_ineligible_folds.csv", index=False)
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
    metric_rows = []
    for model in model_columns:
        model_score = (
            pred.dropna(subset=["actual_cpi_mom_percent", model])
            .sort_values("target_month")
            if not pred.empty else pred
        )
        for window_name, window in (
            ("all_eligible_origins", model_score),
            ("latest_36_eligible_origins", model_score.tail(36)),
            ("latest_24_eligible_origins", model_score.tail(24)),
            ("latest_9_descriptive_only", model_score.tail(9)),
        ):
            if window.empty:
                continue
            err = window["actual_cpi_mom_percent"].to_numpy(float) - window[model].to_numpy(float)
            actual_values = window["actual_cpi_mom_percent"].to_numpy(float)
            predicted_values = window[model].to_numpy(float)
            mape_mask = np.abs(actual_values) >= 0.05
            smape_denominator = np.abs(actual_values) + np.abs(predicted_values)
            smape_values = np.divide(
                200 * np.abs(err),
                smape_denominator,
                out=np.zeros_like(err),
                where=smape_denominator > 0,
            )
            metric_rows.append({
                "configuration": model,
                "evaluation_window": window_name,
                "origins": len(window),
                "mae_pp": float(np.mean(np.abs(err))),
                "rmse_pp": float(np.sqrt(np.mean(err ** 2))),
                "mape_pct_excluding_near_zero": (
                    float(np.mean(np.abs(err[mape_mask] / actual_values[mape_mask])) * 100)
                    if mape_mask.any() else None
                ),
                "mape_scored_origins": int(mape_mask.sum()),
                "mape_excluded_near_zero_origins": int((~mape_mask).sum()),
                "smape_pct": float(np.mean(smape_values)),
                "directional_accuracy_pct": float(np.mean(np.sign(window["actual_cpi_mom_percent"]) == np.sign(window[model])) * 100),
                "point_in_time_mode": "asof_feature_store_with_source_availability_gates; historical_source_vintages_unavailable",
                "real_time_performance_claim_allowed": False,
            })
    metrics_frame = pd.DataFrame(
        metric_rows,
        columns=[
            "configuration", "evaluation_window", "origins", "mae_pp",
            "rmse_pp", "mape_pct_excluding_near_zero", "mape_scored_origins",
            "mape_excluded_near_zero_origins", "smape_pct",
            "directional_accuracy_pct", "point_in_time_mode",
            "real_time_performance_claim_allowed",
        ],
    )
    metrics_frame.to_csv(OUT / "feature_ablation_metrics.csv", index=False)
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
        **{
            f"multisource_without_{source}": ["full_multisource_ridge"]
            for source in MULTISOURCE_ABLATION_GROUPS
        },
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
    runtime_frame = pd.DataFrame([
        {
            **{"model": model},
            **values,
            "max_rss_kb": int(rss),
            "script_wall_seconds": round(elapsed, 3),
            "strict_vintage_available": False,
        }
        for model, values in runtime.items()
    ])
    runtime_frame.to_csv(OUT / "feature_ablation_runtime.csv", index=False)
    model_specs = {
        "naive_last_cpi": {
            "role": "baseline",
            "model_family": "naive_last_observation",
            "predictors": ["cpi_mom_lag_1"],
            "hyperparameters": {},
        },
        "ar1_ols": {
            "role": "baseline",
            "model_family": "AR(1) OLS",
            "predictors": ["cpi_mom_lag_1"],
            "hyperparameters": {"fit_intercept": True, "regularization": None},
        },
        "cpi_only_ridge": {
            "role": "method_ablation",
            "model_family": "Ridge",
            "predictors": ["cpi_mom_lag_1"],
            "hyperparameters": {"alpha": 10.0},
        },
        "u_midas_brent_ridge": {
            "role": "single_source_ablation",
            "model_family": "U-MIDAS Ridge",
            "predictors": brent_predictors,
            "hyperparameters": {"alpha": 10.0, "brent_weekly_lags": LAGS},
        },
        "ar_midas_ridge": {
            "role": "method_ablation",
            "model_family": "AR-MIDAS Ridge",
            "predictors": BASE_PREDICTORS,
            "hyperparameters": {"alpha": 10.0, "brent_weekly_lags": LAGS},
        },
        "midas_elastic_net": {
            "role": "method_ablation",
            "model_family": "Elastic Net",
            "predictors": BASE_PREDICTORS,
            "hyperparameters": {"alpha_grid": [0.1, 1.0], "l1_ratio_grid": [0.2, 0.8]},
        },
        "midas_lasso": {
            "role": "method_ablation",
            "model_family": "Lasso",
            "predictors": BASE_PREDICTORS,
            "hyperparameters": {"alpha_grid": [0.1, 1.0], "l1_ratio": 1.0},
        },
        "midas_huber": {
            "role": "method_ablation",
            "model_family": "Huber Ridge",
            "predictors": BASE_PREDICTORS,
            "hyperparameters": {"alpha_grid": [0.1, 1.0, 10.0], "epsilon": 1.345},
        },
        "full_multisource_ridge": {
            "role": "primary_production_candidate",
            "model_family": "Full multi-source Ridge",
            "predictors": [*BASE_PREDICTORS, *MACRO_PREDICTORS],
            "hyperparameters": {"alpha": 10.0, "missingness_indicators": True},
            "sources": ["CPI", "Brent", "USD/VND", "RON95", "E10 RON95", "NSO IIP"],
        },
        "full_multisource_elastic_net": {
            "role": "method_ablation",
            "model_family": "Full multi-source Elastic Net",
            "predictors": [*BASE_PREDICTORS, *MACRO_PREDICTORS],
            "hyperparameters": {
                "alpha_grid": [0.1, 1.0],
                "l1_ratio_grid": [0.2, 0.8],
                "inner_validation_origins_max": 6,
            },
            "sources": ["CPI", "Brent", "USD/VND", "RON95", "E10 RON95", "NSO IIP"],
        },
    }
    source_labels = {
        "brent": "Brent weekly and monthly",
        "usd_vnd": "USD/VND",
        "fuel": "Vietnam fuel prices (RON95 and E10)",
        "iip": "Vietnam monthly IIP",
    }
    for source, (source_predictors, _) in MULTISOURCE_ABLATION_GROUPS.items():
        model_name = f"multisource_without_{source}"
        model_specs[model_name] = {
            "role": "leave_one_source_out_ablation",
            "model_family": "Full multi-source Ridge without one source group",
            "predictors": [
                *[column for column in BASE_PREDICTORS if column not in source_predictors],
                *[column for column in MACRO_PREDICTORS if column not in source_predictors],
            ],
            "hyperparameters": {
                "alpha": 10.0,
                "missingness_indicators": True,
                "omitted_source_group": source,
            },
            "omitted_source": source_labels[source],
            "comparison_model": "full_multisource_ridge",
        }
    code_paths = [
        Path(__file__),
        ROOT / "forecasting" / "asof_features.py",
        ROOT / "forecasting" / "multisource_nowcast.py",
    ]
    source_digest = hashlib.sha256()
    for code_path in code_paths:
        source_digest.update(code_path.name.encode("utf-8"))
        source_digest.update(code_path.read_bytes())
    source_hash = source_digest.hexdigest()
    ineligible_frame = pd.DataFrame(
        ineligible_folds,
        columns=[
            "model", "target_month", "status", "reason",
            "required_training_rows", "available_training_rows",
        ],
    )
    failures_frame = pd.DataFrame(
        failures, columns=["model", "target_month", "error"]
    )
    run_id, model_registry = persist_model_artifacts(
        pred,
        metrics_frame,
        model_specs,
        source_hash,
        runtime_frame,
        ineligible_frame,
        failures_frame,
    )
    protocol = {
        "protocol_version": 3,
        "evaluation_run_id": run_id,
        "primary_model": "full_multisource_ridge",
        "model_registry": "feature_ablation_model_registry.csv",
        "per_model_artifacts": "models/<model_name>/<model_version>/",
        "configs": ["naive_last_cpi", "ar1_ols", "cpi_only_ridge", "u_midas_brent_ridge", "ar_midas_ridge", "midas_elastic_net", "midas_lasso", "midas_huber", "full_multisource_ridge", "full_multisource_elastic_net", "multisource_without_brent", "multisource_without_usd_vnd", "multisource_without_fuel", "multisource_without_iip"],
        "sourcewise_ablation": "for each source, compare base AR-MIDAS against base plus one source on paired folds where the source is point-in-time available; require at least 8 prior training rows containing that source; otherwise mark not estimable",
        "leave_one_source_out_ablation": "compare Full multi-source Ridge with four configurations that remove the full Brent block (weekly returns and monthly Brent), USD/VND, Vietnam fuel (RON95 and E10 together), or monthly IIP plus that source's missingness indicator; use paired common folds and report not-estimable when point-in-time coverage is absent",
        "full_sources": ["Brent", "USD/VND", "RON95 fuel", "E10 RON95 fuel", "monthly Vietnam IIP YoY growth"],
        "elastic_net_grid": {"alpha": [0.1, 1.0], "l1_ratio": [0.2, 0.8], "inner_validation_origins_max": 6},
        "lasso_grid": {"alpha": [0.1, 1.0], "l1_ratio": [1.0], "inner_validation_origins_max": 6},
        "huber_grid": {"alpha": [0.1, 1.0, 10.0], "epsilon": 1.345, "inner_validation_origins_max": 6},
        "selection": "expanding validation folds within each outer fold; preprocessing fitted only on training data",
        "asof_join": "feature-store cutoff <= forecast cutoff; each source available_at <= both cutoffs; observation date/year <= cutoff",
        "missingness": "train-only median imputation plus explicit per-source availability indicators",
        "primary_metric": "MAE CPI MoM percentage points",
        "guardrail_metric": "RMSE CPI MoM percentage points",
        "supplementary_metrics": [
            "MAPE excluding abs(actual_cpi_mom_percent) < 0.05 percentage points",
            "sMAPE",
        ],
        "mape_near_zero_exclusion_threshold_pp": 0.05,
        "operational_metrics": "per-model fit/predict seconds, max fold seconds, runtime failures, data-ineligible folds, completed folds, process peak RSS",
        "fold_status_policy": "insufficient training history or unavailable point-in-time predictors are ineligible, not runtime failures",
        "primary_model_policy": "full_multisource_ridge is the primary model; naive, CPI-only, AR-MIDAS and alternative estimators are baselines/ablations",
        "nso_calendar_snapshot": snapshot,
        "source_vintages": "not archived before first raw collection; results are pseudo-vintage diagnostics",
        "real_time_performance_claim_allowed": False,
    }
    (OUT / "feature_ablation_protocol.json").write_text(json.dumps(protocol, indent=2), encoding="utf-8")
    print(
        f"Feature ablation complete: {len(pred)} target rows, {len(failures)} runtime failures, "
        f"{len(ineligible_folds)} data-ineligible model-folds, wall={elapsed:.2f}s, "
        f"max_rss={rss} KB; primary=full_multisource_ridge; models={len(model_registry)}; run={run_id}"
    )
    print(f"As-of source coverage: {pd.DataFrame(coverage_rows).to_dict(orient='records')}")


if __name__ == "__main__":
    main()
