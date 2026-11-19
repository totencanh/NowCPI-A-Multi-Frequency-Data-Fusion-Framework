"""Append-only prospective scoring for live NowCPI forecast origins."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd


def _json_safe(value):
    """Convert pandas/numpy scalars and timestamps into strict JSON values."""
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "item"):
        value = value.item()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"Invalid JSONL record in {path} at line {line_number}"
                ) from exc
    return rows


def _append_missing(path: Path, rows: list[dict], key: str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = {str(row[key]) for row in _read_jsonl(path) if row.get(key)}
    additions = [row for row in rows if str(row.get(key)) not in existing]
    if additions:
        with path.open("a", encoding="utf-8", newline="\n") as stream:
            for row in additions:
                stream.write(
                    json.dumps(_json_safe(row), ensure_ascii=False, sort_keys=True,
                               allow_nan=False) + "\n"
                )
    return len(additions)


def append_forecast_origin(
    result: dict,
    current_row: pd.DataFrame,
    feature_vintage: dict | None,
    output_dir: Path,
) -> dict:
    """Persist a successful live nowcast with its decision-time lineage."""
    if result.get("nowcast_mom_percent") is None:
        return {"origin_id": None, "origin_appended": False}

    cutoff = str(result["cutoff_at_utc"])
    target_month = str(result["target_month"])
    model_version = str(result.get("model_version", "unknown"))
    origin_id = hashlib.sha256(
        f"{target_month}|{cutoff}|{model_version}".encode("utf-8")
    ).hexdigest()
    vintage = feature_vintage or {}
    lineage_columns = (
        "feature_snapshot_id", "feature_build_id", "snapshot_recorded_at",
        "asof_cutoff_ts", "cpi_available_at", "cpi_available_at_basis",
        "brent_available_at", "brent_available_at_basis", "fx_available_at",
        "fx_available_at_basis", "fuel_available_at", "fuel_available_at_basis",
        "iip_available_at", "iip_available_at_basis", "cpi_asof_eligible",
        "brent_asof_eligible", "fx_asof_eligible", "fuel_asof_eligible",
        "iip_asof_eligible", "asof_feature_source_coverage_pct",
    )
    cpi_lag = (
        current_row["cpi_mom_lag_1"].iloc[0]
        if not current_row.empty and "cpi_mom_lag_1" in current_row
        else None
    )
    forecasts = {
        "selected_model": result.get("nowcast_mom_percent"),
        "full_multisource_ridge": result.get("full_multisource_ridge_nowcast_mom_percent"),
        "ar_midas_ridge": result.get("ar_midas_ridge_nowcast_mom_percent"),
        "cpi_only_ridge": result.get("cpi_only_ridge_nowcast_mom_percent"),
        "brent_only_u_midas": result.get("u_midas_brent_only_diagnostic_mom_percent"),
        "naive_last_cpi": cpi_lag,
    }
    # Persist the actual feature vector used for this forecast. The snapshot ID
    # remains the authoritative Gold lineage; this copy makes each prospective
    # origin independently auditable and replayable if the feature table changes.
    model_predictors = result.get("model_predictors") or []
    model_input_features = {}
    if not current_row.empty:
        for name in model_predictors:
            if name in current_row.columns:
                model_input_features[name] = current_row[name].iloc[0]
    origin = {
        "origin_id": origin_id,
        "schema_version": 2,
        "target_month": target_month,
        "information_cutoff_at_utc": cutoff,
        "forecast_issued_at_utc": result.get("processing_completed_at_utc"),
        "model_name": result.get("nowcast_model"),
        "model_version": model_version,
        "model_code_sha256": result.get("model_code_sha256"),
        "selected_forecast_mom_percent": result.get("nowcast_mom_percent"),
        "forecasts_mom_percent": forecasts,
        "prediction_interval": {
            "model": result.get("prediction_interval_model"),
            "nominal_coverage_pct": result.get(
                "prediction_interval_nominal_coverage_pct"
            ),
            "lower_mom_percent": result.get("prediction_interval_lower_mom_percent"),
            "upper_mom_percent": result.get("prediction_interval_upper_mom_percent"),
            "calibration_origins": result.get(
                "prediction_interval_calibration_origins", 0
            ),
            "status": result.get("prediction_interval_status"),
            "calibration_basis": result.get("prediction_interval_calibration_basis"),
        },
        "latest_actual_cpi_month_at_origin": result.get("latest_actual_cpi_month"),
        "cpi_lag_month": result.get("cpi_lag_month"),
        "cpi_release_calendar_snapshot": result.get("cpi_release_calendar_snapshot"),
        "cpi_release_date_precision": "date_only",
        "feature_snapshot": {
            name: vintage.get(name) for name in lineage_columns
        },
        "model_input_features": model_input_features,
        "feature_source_training_origins": result.get(
            "multisource_source_training_origins", {}
        ),
        "historical_vintage_status": (
            "incomplete; live origin is prospective pipeline-observed evidence"
        ),
    }
    added = _append_missing(
        output_dir / "pit_forecast_origins.jsonl", [origin], "origin_id"
    )
    return {"origin_id": origin_id, "origin_appended": bool(added)}


def append_observed_outcomes(labels: pd.DataFrame, output_dir: Path) -> dict:
    """Score origins once their target label is available after the cutoff."""
    origin_path = output_dir / "pit_forecast_origins.jsonl"
    outcome_path = output_dir / "pit_forecast_outcomes.jsonl"
    origins = _read_jsonl(origin_path)
    existing_outcomes = {
        str(row.get("origin_id")) for row in _read_jsonl(outcome_path)
    }
    if labels.empty or "label_available_at" not in labels:
        return {"outcomes_appended": 0, "scored_origins": len(existing_outcomes)}

    label_rows = {}
    for label in labels.sort_values("label_available_at").itertuples(index=False):
        label_rows[pd.Timestamp(label.target_month).strftime("%Y-%m")] = label

    scored_at = pd.Timestamp.now(tz="UTC").isoformat()
    additions = []
    for origin in origins:
        origin_id = str(origin.get("origin_id"))
        if origin_id in existing_outcomes:
            continue
        label = label_rows.get(str(origin.get("target_month")))
        if label is None or pd.isna(label.target_cpi_mom_percent):
            continue
        cutoff = pd.to_datetime(origin["information_cutoff_at_utc"], utc=True)
        available_at = pd.to_datetime(
            label.label_available_at, utc=True, errors="coerce"
        )
        if pd.isna(available_at) or available_at <= cutoff:
            continue

        actual = float(label.target_cpi_mom_percent)
        errors = {}
        for model, prediction in origin.get("forecasts_mom_percent", {}).items():
            if prediction is None:
                continue
            error = float(prediction) - actual
            errors[model] = {
                "signed_error_pp": error,
                "absolute_error_pp": abs(error),
                "squared_error_pp": error ** 2,
            }
        additions.append({
            "origin_id": origin_id,
            "target_month": origin["target_month"],
            "information_cutoff_at_utc": origin["information_cutoff_at_utc"],
            "forecast_issued_at_utc": origin.get("forecast_issued_at_utc"),
            "actual_cpi_mom_percent": actual,
            "label_available_at_utc": available_at.isoformat(),
            "actual_availability_basis": getattr(
                label, "label_available_at_basis", "basis_not_exposed_by_gold"
            ),
            "actual_source_release_date": getattr(
                label, "label_source_release_date", None
            ),
            "actual_value_vintage_status": (
                "Gold label at first scoring run; source revision vintage not verified"
            ),
            "errors": errors,
            "prediction_interval_score": _score_prediction_interval(
                origin.get("prediction_interval", {}), actual
            ),
            "scored_at_utc": scored_at,
        })
    added = _append_missing(outcome_path, additions, "origin_id")
    return {
        "outcomes_appended": added,
        "scored_origins": len(existing_outcomes) + added,
    }


def _score_prediction_interval(interval: dict, actual: float) -> dict:
    """Score an issued interval without claiming calibration from one outcome."""
    lower = interval.get("lower_mom_percent")
    upper = interval.get("upper_mom_percent")
    if lower is None or upper is None:
        return {
            "status": interval.get("status", "interval_unavailable"),
            "covered": None,
            "width_pp": None,
        }
    return {
        "status": interval.get("status", "unknown"),
        "nominal_coverage_pct": interval.get("nominal_coverage_pct"),
        "covered": bool(float(lower) <= actual <= float(upper)),
        "width_pp": float(upper) - float(lower),
    }


def summarize_ledger(output_dir: Path) -> dict:
    """Write descriptive metrics using one latest forecast origin per month."""
    origins = _read_jsonl(output_dir / "pit_forecast_origins.jsonl")
    outcomes = _read_jsonl(output_dir / "pit_forecast_outcomes.jsonl")
    origin_by_id = {row["origin_id"]: row for row in origins}
    scored = [
        origin_by_id[row["origin_id"]] | {"outcome": row}
        for row in outcomes if row.get("origin_id") in origin_by_id
    ]
    latest_per_month = {}
    for row in scored:
        month = row["target_month"]
        previous = latest_per_month.get(month)
        if previous is None or row["information_cutoff_at_utc"] > previous[
            "information_cutoff_at_utc"
        ]:
            latest_per_month[month] = row

    observations = {}
    for row in latest_per_month.values():
        actual = float(row["outcome"]["actual_cpi_mom_percent"])
        for model, prediction in row.get("forecasts_mom_percent", {}).items():
            if prediction is None:
                continue
            name = (
                f"selected:{row.get('model_name')}" if model == "selected_model" else model
            )
            observations.setdefault(name, []).append((actual, float(prediction)))

    metric_rows = []
    for model, pairs in sorted(observations.items()):
        actual = pd.Series([pair[0] for pair in pairs], dtype=float)
        predicted = pd.Series([pair[1] for pair in pairs], dtype=float)
        metric_rows.append({
            "model": model,
            "unique_target_months": len(pairs),
            "mae_pp": float((predicted - actual).abs().mean()),
            "rmse_pp": float((((predicted - actual) ** 2).mean()) ** 0.5),
            "directional_accuracy_pct": float(
                (np.sign(predicted) == np.sign(actual)).mean() * 100
            ),
            "evaluation_mode": "prospective_pipeline_observed_descriptive",
            "inferential_claim_allowed": False,
        })
    pd.DataFrame(metric_rows, columns=[
        "model", "unique_target_months", "mae_pp", "rmse_pp",
        "directional_accuracy_pct", "evaluation_mode", "inferential_claim_allowed",
    ]).to_csv(output_dir / "pit_forecast_origin_metrics.csv", index=False)

    interval_scores = [
        row["outcome"].get("prediction_interval_score", {})
        for row in latest_per_month.values()
    ]
    usable_interval_scores = [
        score for score in interval_scores if score.get("covered") is not None
    ]
    if usable_interval_scores:
        interval_metric_rows = [{
            "nominal_coverage_pct": float(np.mean([
                float(score["nominal_coverage_pct"])
                for score in usable_interval_scores
                if score.get("nominal_coverage_pct") is not None
            ])),
            "observed_coverage_pct": float(np.mean([
                bool(score["covered"]) for score in usable_interval_scores
            ]) * 100),
            "mean_width_pp": float(np.mean([
                float(score["width_pp"]) for score in usable_interval_scores
            ])),
            "scored_origins": len(usable_interval_scores),
            "evaluation_mode": "prospective_pipeline_observed_descriptive",
            "inferential_claim_allowed": False,
        }]
    else:
        interval_metric_rows = []
    pd.DataFrame(interval_metric_rows, columns=[
        "nominal_coverage_pct", "observed_coverage_pct", "mean_width_pp",
        "scored_origins", "evaluation_mode", "inferential_claim_allowed",
    ]).to_csv(output_dir / "pit_prediction_interval_metrics.csv", index=False)


    summary = {
        "schema_version": 1,
        "forecast_origins_recorded": len(origins),
        "origins_with_pipeline_observed_outcome": len(outcomes),
        "origins_waiting_for_target_release": max(0, len(origins) - len(outcomes)),
        "unique_target_months_scored": len(latest_per_month),
        "scoring_rule": "latest forecast cutoff per target month; each month counts once",
        "metrics_file": "pit_forecast_origin_metrics.csv",
        "prediction_interval_metrics_file": "pit_prediction_interval_metrics.csv",
        "evaluation_mode": (
            "prospective pipeline-observed; separate from historical pseudo-vintage backtest"
        ),
        "source_release_vintages_verified": False,
        "updated_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
    }
    (output_dir / "pit_forecast_origin_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    return summary
