"""Append run-scoped, per-stage metric events without affecting pipeline work."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _json_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _append(record: dict[str, Any]) -> None:
    path = Path(os.getenv("NOWCPI_STAGE_METRICS_FILE", "/opt/airflow/audit/stage_metrics.jsonl"))
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                stream.write(json.dumps(record, ensure_ascii=False, default=_json_default) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
    except Exception as exc:
        # Audit/monitoring must never turn a successful data task into a failure.
        print(f"STAGE_METRICS_AUDIT_WARNING: {exc}", flush=True)


def _push(component: str, stage: str, source: str, status: str, values: dict[str, Any]) -> None:
    try:
        from spark.common.prometheus_metrics import push_stage_metrics

        push_stage_metrics(component=component, stage=stage, source=source, status=status, values=values)
    except Exception as exc:
        print(f"STAGE_METRICS_PUSH_WARNING: {exc}", flush=True)


def _context_ids() -> dict[str, Any]:
    return {
        "dag_id": os.getenv("AIRFLOW_CTX_DAG_ID"),
        "dag_run_id": os.getenv("AIRFLOW_CTX_DAG_RUN_ID"),
        "task_id": os.getenv("AIRFLOW_CTX_TASK_ID"),
    }


def _base_event(stage: str, source: str, status: str, **metrics: Any) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()), "event_time": _now(), "metric_type": "stage_artifact",
        "stage": stage, "source": source, "status": status, **_context_ids(), **metrics,
    }


def record_dbt_models(path: Path) -> None:
    """Record per-model dbt runtime/status and row counts when dbt exposes them."""
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
        for item in artifact.get("results", []):
            unique_id = str(item.get("unique_id", ""))
            if not unique_id.startswith("model.nowcpi."):
                continue
            adapter = item.get("adapter_response") or {}
            rows = adapter.get("rows_affected")
            try:
                rows = int(rows) if rows is not None else None
                if rows is not None and rows < 0:
                    rows = None
            except (TypeError, ValueError):
                rows = None
            model = unique_id.rsplit(".", 1)[-1]
            runtime = item.get("execution_time")
            _append(_base_event(
                "gold", model, str(item.get("status", "unknown")).lower(),
                model_name=model, model_unique_id=unique_id,
                duration_seconds=float(runtime) if runtime is not None else None,
                records_out=rows, dbt_generated_at=(artifact.get("metadata") or {}).get("generated_at"),
            ))
            _push("dbt_model", "gold", model, str(item.get("status", "unknown")).lower(), {
                "model_execution_seconds": runtime, "model_rows_affected": rows,
            })
    except Exception as exc:
        print(f"STAGE_METRICS_DBT_WARNING: {exc}", flush=True)


def record_forecast(kind: str, path: Path) -> None:
    """Record model execution/eligibility metrics from nowcast or ablation artifacts."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if kind == "nowcast":
            model = str(payload.get("nowcast_model") or "primary_nowcast")
            values = {
                "model_name": model,
                "model_version": payload.get("model_version"),
                "target_month": payload.get("target_month"),
                "forecast_mom_percent": payload.get("nowcast_mom_percent"),
                "duration_seconds": payload.get("processing_duration_seconds"),
                "training_rows": payload.get("full_multisource_ridge_training_rows"),
                "pit_macro_training_origins": payload.get("full_multisource_ridge_training_origins_with_any_macro_source"),
                "feature_coverage_percent": payload.get("multisource_current_feature_coverage_pct"),
                "interval_lower": payload.get("prediction_interval_lower_mom_percent"),
                "interval_upper": payload.get("prediction_interval_upper_mom_percent"),
            }
            status = "success" if values["forecast_mom_percent"] is not None else "unavailable"
            _append(_base_event("forecasting", model, status, **values))
            _push("forecast_model", "forecasting", model, status, {
                key: values.get(key) for key in (
                    "forecast_mom_percent", "duration_seconds", "training_rows",
                    "pit_macro_training_origins", "feature_coverage_percent",
                )
            })
            return

        for model in payload.get("models", []):
            name = str(model.get("model_name") or "unknown")
            runtime_failures = int(model.get("runtime_failures") or 0)
            completed_folds = int(model.get("completed_folds") or 0)
            ineligible_folds = int(model.get("ineligible_folds") or 0)
            status = (
                "runtime_failed" if runtime_failures > 0
                else "success" if completed_folds > 0
                else "ineligible" if ineligible_folds > 0
                else "not_run"
            )
            metrics = {
                "model_name": name, "model_version": model.get("model_version"),
                "model_role": model.get("role"), "completed_folds": model.get("completed_folds"),
                "ineligible_folds": model.get("ineligible_folds"),
                "runtime_failures": model.get("runtime_failures"),
                "duration_seconds": model.get("fit_predict_seconds"),
            }
            _append(_base_event("forecasting", name, status, **metrics))
            _push("forecast_model", "forecasting", name, status, {
                key: metrics.get(key) for key in (
                    "completed_folds", "ineligible_folds", "runtime_failures", "duration_seconds",
                )
            })
    except Exception as exc:
        print(f"STAGE_METRICS_FORECAST_WARNING: {exc}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    dbt = subparsers.add_parser("dbt")
    dbt.add_argument("--run-results", type=Path, required=True)
    forecast = subparsers.add_parser("forecast")
    forecast.add_argument("--kind", choices=("nowcast", "ablation"), required=True)
    forecast.add_argument("--artifact", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "dbt":
        record_dbt_models(args.run_results)
    else:
        record_forecast(args.kind, args.artifact)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
