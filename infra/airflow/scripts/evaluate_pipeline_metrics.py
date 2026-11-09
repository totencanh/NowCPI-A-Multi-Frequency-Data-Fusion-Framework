"""Create a NowCPI freshness, latency, throughput, and quality report.

Run inside the Airflow container after a pipeline run:
    python /opt/airflow/scripts/evaluate_pipeline_metrics.py
"""

from __future__ import annotations

import argparse
import calendar
import csv
import json
import os
import re
import subprocess
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from trino.dbapi import connect
from confluent_kafka import Consumer, TopicPartition

try:
    from spark.common.prometheus_metrics import push_stage_metrics
except ImportError:
    push_stage_metrics = None


SILVER_TABLES = {
    "cpi": "cpi_observations",
    "cpi_components": "cpi_component_observations",
    "brent": "brent_oil_observations",
    "usd_vnd": "usd_vnd_observations",
    "vn_fuel": "vn_fuel_observations",
    "iip": "iip_observations",
}
REJECTED_TABLES = {name: f"{table}_rejected" for name, table in SILVER_TABLES.items()}
AUDIT_SOURCES = {
    "imf_cpi_vietnam": "cpi",
    "imf_cpi_components_vietnam": "cpi_components",
    "brent_oil_daily": "brent",
    "usd_vnd_daily": "usd_vnd",
    "vn_fuel_e10_ron95": "vn_fuel",
    "nso_iip_vietnam": "iip",
}
TOPIC_SOURCES = {
    "nowcpi.cpi": "cpi",
    "nowcpi.cpi_components": "cpi_components",
    "nowcpi.brent_oil": "brent",
    "nowcpi.usd_vnd": "usd_vnd",
    "nowcpi.vn_fuel": "vn_fuel",
    "nowcpi.ppi_iip": "iip",
}
STAGE_TASKS = {
    "ingestion": [
        "collect_cpi", "collect_cpi_components", "collect_iip", "collect_brent",
        "collect_vn_fuel", "collect_usd_vnd", "collect_nso_cpi_release_calendar",
    ],
    "kafka": ["publish_to_kafka"],
    "bronze": ["wait_for_bronze_catchup"],
    "silver": ["silver_cpi_sources", "silver_market_sources"],
    "gold": ["dbt_staging", "dbt_gold", "dbt_pit_feature_tests"],
    "forecasting": ["multisource_inflation_nowcast", "feature_ablation_evaluation"],
}
SOURCE_FREQUENCIES = {
    "imf_cpi_vietnam": "monthly",
    "imf_cpi_components_vietnam": "monthly",
    "brent_oil_daily": "daily",
    "usd_vnd_daily": "daily",
    "vn_fuel_e10_ron95": "daily",
    "nso_iip_vietnam": "monthly",
}
# Provisional, visible monitoring objectives. Adjust these to the seminar's
# agreed service levels; no throughput target is set without a workload target.
FRESHNESS_SLO_DAYS = {"daily": 3, "monthly": 75, "annual": 400}
DAG_LATENCY_SLO_SECONDS = 600
KAFKA_BRONZE_P95_SLO_SECONDS = 60
# CPI/IIP indices and market prices cannot be zero or negative.
POSITIVE_VALUE_DOMAINS = set(SILVER_TABLES)


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def observation_period_end(value: Any) -> date | None:
    """Convert source year/month/date periods to their calendar end date."""
    if not value:
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d{4}", text):
        return date(int(text), 12, 31)
    if re.fullmatch(r"\d{4}-\d{2}", text):
        year, month = map(int, text.split("-"))
        return date(year, month, calendar.monthrange(year, month)[1])
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def rows_as_dicts(cursor) -> list[dict[str, Any]]:
    columns = [item[0] for item in cursor.description]
    return [
        {name: json_safe(value) for name, value in zip(columns, row)}
        for row in cursor.fetchall()
    ]


def read_audit(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records = []
    with path.open(encoding="utf-8") as audit_file:
        for line_number, line in enumerate(audit_file, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at {path}:{line_number}: {exc}") from exc
            if isinstance(record, dict):
                records.append(record)
    return records


def latest_source_checks(records: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        source = record.get("source")
        if not source:
            continue
        previous = latest.get(source)
        if previous is None or str(record.get("check_completed_at", "")) > str(
            previous.get("check_completed_at", "")
        ):
            latest[source] = record

    result = []
    for source, record in sorted(latest.items()):
        # Retired collectors can remain in the append-only audit log. Do not
        # count them as current sources or let their old frequency distort SLOs.
        if source not in AUDIT_SOURCES:
            continue
        completed = parse_time(record.get("check_completed_at"))
        observation_end = observation_period_end(record.get("latest_stored_observation_period"))
        warnings = list(record.get("warnings", []))
        if record.get("fallback_used"):
            warnings.append(
                "recovered with "
                + str(record.get("fallback_source") or "fallback provider")
            )
        result.append(
            {
                "source": source,
                "domain": AUDIT_SOURCES.get(source, source),
                "last_check_status": record.get("status"),
                "warnings": warnings,
                "expected_source_frequency": SOURCE_FREQUENCIES.get(source, "unknown"),
                "freshness_slo_days": FRESHNESS_SLO_DAYS.get(
                    SOURCE_FREQUENCIES.get(source, "unknown")
                ),
                "last_check_completed_at": record.get("check_completed_at"),
                "check_age_minutes": round((now - completed).total_seconds() / 60, 2)
                if completed
                else None,
                "latest_stored_observation_period": record.get(
                    "latest_stored_observation_period"
                ),
                "latest_stored_release_ts": record.get("latest_stored_release_ts"),
                "latest_observation_period_end": observation_end.isoformat()
                if observation_end
                else None,
                "observation_age_days": (now.date() - observation_end).days
                if observation_end
                else None,
                "freshness_slo_met": bool(
                    record.get("status") == "success"
                    and observation_end is not None
                    and SOURCE_FREQUENCIES.get(source) in FRESHNESS_SLO_DAYS
                    and (now.date() - observation_end).days
                    <= FRESHNESS_SLO_DAYS[SOURCE_FREQUENCIES[source]]
                ),
                "changed_records_written": record.get("changed_records_written", 0),
                "stored_unique_records": record.get("stored_unique_records"),
                "check_duration_seconds": record.get("check_duration_seconds"),
            }
        )
    return result


def get_latest_dag_run(scheduled_only: bool = False) -> dict[str, Any] | None:
    try:
        result = subprocess.run(
            [
                "airflow",
                "dags",
                "list-runs",
                "-d",
                "nowcpi_daily_pipeline",
                "--output",
                "json",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=20,
        )
        runs = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return None
    if scheduled_only:
        runs = [run for run in runs if str(run.get("run_id", "")).startswith("scheduled__")]
    if not runs:
        return None
    latest = max(runs, key=lambda run: str(run.get("start_date") or ""))
    started = parse_time(latest.get("start_date"))
    ended = parse_time(latest.get("end_date"))
    if started:
        latest["duration_seconds"] = round(
            ((ended or datetime.now(timezone.utc)) - started).total_seconds(), 2
        )
    else:
        latest["duration_seconds"] = None
    return latest


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    value = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    return round(value, 2)


def airflow_reliability_metrics(now: datetime) -> dict[str, Any]:
    """Summarize scheduled DAG and task reliability from Airflow's metadata DB."""
    try:
        from airflow.models import DagRun, TaskInstance
        from airflow.utils.session import create_session

        with create_session() as session:
            runs = (
                session.query(DagRun)
                .filter(DagRun.dag_id == "nowcpi_daily_pipeline")
                .filter(DagRun.start_date >= now - timedelta(days=30))
                .filter(DagRun.run_id.like("scheduled__%"))
                .all()
            )
            task_instances = (
                session.query(TaskInstance)
                .filter(TaskInstance.dag_id == "nowcpi_daily_pipeline")
                .filter(TaskInstance.start_date >= now - timedelta(days=30))
                .all()
            )
            run_rows = [
                {
                    "run_id": run.run_id,
                    "state": str(run.state).lower(),
                    "start_date": run.start_date,
                    "duration_seconds": (
                        (run.end_date - run.start_date).total_seconds()
                        if run.start_date and run.end_date
                        else None
                    ),
                }
                for run in runs
            ]
            task_rows = [
                {
                    "task_id": task.task_id,
                    "run_id": task.run_id,
                    "state": str(task.state).lower(),
                    "duration_seconds": float(task.duration)
                    if task.duration is not None
                    else None,
                    # Airflow 2.9's public try_number property reports the
                    # *next* try for non-running tasks. Read the stored count
                    # so successful first attempts are not mislabeled retries.
                    "try_number": int(getattr(task, "_try_number", task.try_number) or 0),
                    "start_date": task.start_date,
                    "end_date": task.end_date,
                }
                for task in task_instances
            ]
            latest_db_run = (
                session.query(DagRun)
                .filter(DagRun.dag_id == "nowcpi_daily_pipeline")
                .filter(DagRun.start_date.isnot(None))
                .order_by(DagRun.start_date.desc())
                .first()
            )
            latest_task_rows = []
            if latest_db_run is not None:
                latest_instances = (
                    session.query(TaskInstance)
                    .filter(TaskInstance.dag_id == "nowcpi_daily_pipeline")
                    .filter(TaskInstance.run_id == latest_db_run.run_id)
                    .all()
                )
                latest_task_rows = [
                    {
                        "task_id": task.task_id,
                        "run_id": task.run_id,
                        "state": str(task.state).lower() if task.state else "not_started",
                        "duration_seconds": float(task.duration)
                        if task.duration is not None
                        else None,
                        "try_number": int(getattr(task, "_try_number", task.try_number) or 0),
                        "start_date": task.start_date,
                        "end_date": task.end_date,
                    }
                    for task in latest_instances
                ]
            latest_db_run_id = latest_db_run.run_id if latest_db_run else None
    except Exception as exc:  # Airflow DB visibility may depend on runtime role.
        return {
            "available": False,
            "error": str(exc),
            "windows": {},
            "tasks_30d": [],
            "latest_run_id": None,
            "latest_run_tasks": [],
        }

    windows: dict[str, Any] = {}
    for days in (7, 30):
        cutoff = now - timedelta(days=days)
        selected = [
            row for row in run_rows
            if row["start_date"] is not None and row["start_date"] >= cutoff
        ]
        terminal = [row for row in selected if row["state"] in {"success", "failed"}]
        successful = sum(row["state"] == "success" for row in terminal)
        durations = [
            row["duration_seconds"] for row in terminal
            if row["duration_seconds"] is not None
        ]
        windows[f"{days}d"] = {
            "scheduled_runs": len(selected),
            "terminal_runs": len(terminal),
            "successful_runs": successful,
            "success_rate_percent": round(100 * successful / len(terminal), 2)
            if terminal
            else None,
            "dag_duration_p50_seconds": _percentile(durations, 0.50),
            "dag_duration_p95_seconds": _percentile(durations, 0.95),
        }

    cutoff = now - timedelta(days=30)
    selected_tasks = [
        row for row in task_rows
        if row["start_date"] is not None and row["start_date"] >= cutoff
    ]
    by_task: dict[str, list[dict[str, Any]]] = {}
    for row in selected_tasks:
        by_task.setdefault(row["task_id"], []).append(row)
    task_metrics = []
    for task_id, rows in sorted(by_task.items()):
        terminal = [row for row in rows if row["state"] in {"success", "failed"}]
        durations = [
            row["duration_seconds"] for row in terminal
            if row["duration_seconds"] is not None
        ]
        failed = sum(row["state"] == "failed" for row in terminal)
        retried = sum(row["try_number"] > 1 for row in rows)
        task_metrics.append(
            {
                "task_id": task_id,
                "terminal_instances": len(terminal),
                "failed_instances": failed,
                "retried_instances": retried,
                "failure_rate_percent": round(100 * failed / len(terminal), 2)
                if terminal
                else None,
                "retry_rate_percent": round(100 * retried / len(rows), 2)
                if rows
                else None,
                "duration_p50_seconds": _percentile(durations, 0.50),
                "duration_p95_seconds": _percentile(durations, 0.95),
            }
        )
    return {
        "available": True,
        "windows": windows,
        "tasks_30d": task_metrics,
        "latest_run_id": latest_db_run_id,
        "latest_run_tasks": latest_task_rows,
    }


def _read_json_if_present(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _read_csv_records_if_present(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def read_dbt_test_metrics(
    target_dir: Path, test_task: dict[str, Any] | None
) -> dict[str, Any]:
    """Summarize test counts only when dbt's artifact belongs to this DAG task."""
    path = target_dir / "run_results.json"
    if not path.is_file():
        return {"available": False, "reason": "dbt run_results.json is missing"}
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"available": False, "reason": f"Cannot read dbt run_results.json: {exc}"}
    generated = parse_time((artifact.get("metadata") or {}).get("generated_at"))
    started = parse_time((test_task or {}).get("start_date"))
    ended = parse_time((test_task or {}).get("end_date"))
    if not generated or not started or generated < started - timedelta(seconds=2):
        return {
            "available": False,
            "reason": "dbt artifact timestamp cannot be matched to this run's PIT test task",
            "generated_at": generated.isoformat() if generated else None,
        }
    if ended and generated > ended + timedelta(seconds=2):
        return {
            "available": False,
            "reason": "dbt artifact was generated outside this run's PIT test task",
            "generated_at": generated.isoformat(),
        }
    tests = [
        item for item in artifact.get("results", [])
        if str(item.get("unique_id", "")).startswith("test.")
    ]
    counts = {
        state: sum(str(item.get("status", "")).lower() == state for item in tests)
        for state in ("pass", "fail", "warn", "error", "skipped")
    }
    return {
        "available": bool(tests),
        "generated_at": generated.isoformat(),
        "test_count": len(tests),
        "passed": counts["pass"],
        "failed": counts["fail"] + counts["error"],
        "warned": counts["warn"],
        "skipped": counts["skipped"],
        "failure_rows": sum(int(item.get("failures") or 0) for item in tests),
        "execution_seconds": round(
            sum(float(item.get("execution_time") or 0) for item in tests), 3
        ),
        "metric_scope": "dbt PIT/data tests from run_results.json matched to task interval",
    }


def read_dbt_gold_model_metrics(
    target_dir: Path, gold_task: dict[str, Any] | None
) -> dict[str, Any]:
    """Join successful dbt Gold model timings with their persisted output rows."""
    if str((gold_task or {}).get("state", "")).lower() != "success":
        return {"available": False, "reason": "The current DAG run's dbt_gold task did not succeed"}
    path = target_dir / "nowcpi_gold_run_results.json"
    if not path.is_file():
        return {"available": False, "reason": "No run-scoped dbt Gold artifact yet"}
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"available": False, "reason": f"Cannot read dbt Gold artifact: {exc}"}
    generated = parse_time((artifact.get("metadata") or {}).get("generated_at"))
    started = parse_time((gold_task or {}).get("start_date"))
    ended = parse_time((gold_task or {}).get("end_date"))
    if not generated or not started or generated < started - timedelta(seconds=2):
        return {"available": False, "reason": "dbt Gold artifact does not match this DAG task"}
    if ended and generated > ended + timedelta(seconds=2):
        return {"available": False, "reason": "dbt Gold artifact was generated outside this DAG task"}

    model_results = [
        item for item in artifact.get("results", [])
        if str(item.get("unique_id", "")).startswith("model.nowcpi.gold_")
    ]
    if not model_results:
        return {"available": False, "reason": "No successful Gold model results in dbt artifact"}
    models = [
        item for item in model_results
        if str(item.get("status", "")).lower() == "success"
    ]
    if len(models) != len(model_results):
        return {
            "available": False,
            "reason": "One or more Gold models did not succeed in the run artifact",
            "models_in_run": len(model_results),
            "successful_models": len(models),
        }

    counts: list[dict[str, Any]] = []
    conn = connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "nowcpi"),
        catalog="delta",
        schema="gold",
    )
    try:
        cursor = conn.cursor()
        for model in models:
            model_name = str(model["unique_id"]).rsplit(".", 1)[-1]
            if not re.fullmatch(r"gold_[a-z0-9_]+", model_name):
                continue
            query = f"SELECT count(*) FROM delta.gold.{model_name}"
            count_scope = "full_table_rebuilt_by_dbt"
            if model_name == "gold_nowcasting_feature_vintages" and started and ended:
                start_literal = started.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
                end_literal = ended.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
                query = (
                    "SELECT count(*) FROM delta.gold.gold_nowcasting_feature_vintages "
                    f"WHERE cast(snapshot_recorded_at AS timestamp) >= TIMESTAMP '{start_literal}' "
                    f"AND cast(snapshot_recorded_at AS timestamp) <= TIMESTAMP '{end_literal}'"
                )
                count_scope = "rows_appended_during_this_gold_task"
            try:
                cursor.execute(query)
                output_rows = int(cursor.fetchone()[0])
                counts.append({
                    "model": model_name,
                    "status": model.get("status"),
                    "output_rows": output_rows,
                    "execution_seconds": round(float(model.get("execution_time") or 0), 3),
                    "count_scope": count_scope,
                })
            except Exception as exc:
                counts.append({
                    "model": model_name,
                    "status": model.get("status"),
                    "output_rows": None,
                    "execution_seconds": round(float(model.get("execution_time") or 0), 3),
                    "count_scope": count_scope,
                    "count_error": str(exc),
                })
    finally:
        conn.close()

    measured = [row for row in counts if row.get("output_rows") is not None]
    total_rows = sum(int(row["output_rows"]) for row in measured)
    total_seconds = sum(
        float(row["execution_seconds"])
        for row in measured
        if row.get("execution_seconds") is not None
    )
    complete = bool(measured) and len(measured) == len(counts) == len(models)
    return {
        "available": complete,
        "generated_at": generated.isoformat(),
        "models_in_run": len(models),
        "models_with_row_counts": len(measured),
        "output_rows_across_model_materializations": total_rows if complete else None,
        "model_execution_seconds_for_counted_outputs": round(total_seconds, 3) if complete and total_seconds else None,
        "materialization_rows_per_model_second": round(total_rows / total_seconds, 3)
        if complete and total_seconds > 0 else None,
        "models": counts,
        "metric_scope": (
            "sum of output rows across successful dbt Gold model materializations; "
            "rows may represent overlapping data across models"
        ),
    }


def _json_default(value: Any) -> str | float:
    """Serialize values returned by Airflow/Trino into report-safe JSON."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def build_pipeline_stage_metrics(
    *,
    now: datetime,
    dag_run: dict[str, Any] | None,
    airflow_reliability: dict[str, Any],
    audit_records: list[dict[str, Any]],
    silver_stage_runs: list[dict[str, Any]],
    kafka_bronze_latency: list[dict[str, Any]],
    kafka_publish_record: dict[str, Any],
    bronze_batch_metrics: dict[str, Any],
    bronze_events_24h: int | None,
    bronze_quarantine: dict[str, Any],
    kafka_lag: dict[str, Any],
    gold_cpi: dict[str, Any],
    gold_features: dict[str, Any],
    gold_model_metrics: dict[str, Any],
    nowcast: dict[str, Any],
    nowcast_backtest: dict[str, Any],
    ablation: dict[str, Any],
) -> dict[str, Any]:
    """Build one run-scoped metric row per pipeline stage plus task detail.

    Counts are populated only when they can be joined to this DAG run. Metrics
    with a wider observation window are explicitly labeled as such.
    """
    run_id = (dag_run or {}).get("run_id") or airflow_reliability.get("latest_run_id")
    task_rows = [
        row for row in airflow_reliability.get("latest_run_tasks", [])
        if not run_id or row.get("run_id") == run_id
    ]
    by_task = {row["task_id"]: row for row in task_rows}
    nowcast_timestamp = parse_time(nowcast.get("processing_completed_at_utc"))
    ablation_timestamp = parse_time(ablation.get("generated_at_utc"))
    artifact_freshness = {
        "nowcast_artifact_matches_latest_run": bool(
            nowcast_timestamp
            and dag_run
            and parse_time(dag_run.get("start_date"))
            and nowcast_timestamp >= parse_time(dag_run["start_date"])
        ),
        "ablation_artifact_matches_latest_run": bool(
            ablation_timestamp
            and dag_run
            and parse_time(dag_run.get("start_date"))
            and ablation_timestamp >= parse_time(dag_run["start_date"])
        ),
    }

    collector_by_task = {
        "collect_cpi": "imf_cpi_vietnam",
        "collect_cpi_components": "imf_cpi_components_vietnam",
        "collect_iip": "nso_iip_vietnam",
        "collect_brent": "brent_oil_daily",
        "collect_vn_fuel": "vn_fuel_e10_ron95",
        "collect_usd_vnd": "usd_vnd_daily",
        "collect_nso_cpi_release_calendar": "nso_cpi_release_calendar",
    }
    audit_by_source = {
        row.get("source"): row
        for row in audit_records
        if row.get("dag_run_id") == run_id and row.get("source")
    }
    run_silver = [row for row in silver_stage_runs if row.get("run_id") == run_id]
    silver_input = sum(int(row.get("input_events") or 0) for row in run_silver)
    silver_accepted = sum(int(row.get("accepted_events") or 0) for row in run_silver)
    silver_rejected = sum(int(row.get("rejected_events") or 0) for row in run_silver)
    silver_duration = sum(
        float(row.get("duration_seconds") or 0)
        for row in run_silver
        if row.get("duration_seconds") is not None
    )

    stage_rows = []
    task_details = []
    for stage, expected_tasks in STAGE_TASKS.items():
        stage_tasks = [by_task.get(task_id, {
            "task_id": task_id,
            "run_id": run_id,
            "state": "not_started",
            "duration_seconds": None,
            "try_number": 0,
            "start_date": None,
            "end_date": None,
        }) for task_id in expected_tasks]
        for task in stage_tasks:
            start = task.get("start_date")
            end = task.get("end_date")
            elapsed = task.get("duration_seconds")
            if elapsed is None and start:
                started = parse_time(start)
                ended = parse_time(end) if end else now
                elapsed = round((ended - started).total_seconds(), 3) if started else None
            task_details.append({
                "run_id": run_id,
                "stage": stage,
                "task_id": task["task_id"],
                "state": task.get("state", "not_started"),
                "start_date": task.get("start_date"),
                "end_date": task.get("end_date"),
                "duration_seconds": elapsed,
                "try_number": task.get("try_number", 0),
            })

        states = [str(task.get("state") or "not_started").lower() for task in stage_tasks]
        started_tasks = [task for task in stage_tasks if task.get("start_date")]
        successful = sum(state == "success" for state in states)
        failed = sum(state == "failed" for state in states)
        retried = sum(int(task.get("try_number") or 0) > 1 for task in stage_tasks)
        if failed:
            stage_status = "failed"
        elif any(state in {"running", "queued", "up_for_retry", "scheduled"} for state in states):
            stage_status = "running"
        elif successful == len(expected_tasks):
            stage_status = "success"
        elif not started_tasks:
            stage_status = "not_started"
        else:
            stage_status = "partial"

        starts = [parse_time(task.get("start_date")) for task in started_tasks]
        starts = [value for value in starts if value]
        ends = [parse_time(task.get("end_date")) for task in started_tasks]
        stage_end = max((value for value in ends if value), default=now)
        stage_wall = round((stage_end - min(starts)).total_seconds(), 3) if starts else None
        task_duration_sum = sum(
            float(task["duration_seconds"])
            for task in task_details[-len(stage_tasks):]
            if task["duration_seconds"] is not None
        )

        row = {
            "run_id": run_id,
            "stage": stage,
            "status": stage_status,
            "task_count": len(expected_tasks),
            "succeeded_tasks": successful,
            "failed_tasks": failed,
            "not_started_tasks": sum(state == "not_started" for state in states),
            "retried_tasks": retried,
            "task_success_percent": round(100 * successful / len(expected_tasks), 2)
            if expected_tasks else None,
            "stage_wall_seconds": stage_wall,
            "task_duration_sum_seconds": round(task_duration_sum, 3),
            "records_in": None,
            "records_out": None,
            "rejected_records": None,
            "acceptance_rate_percent": None,
            "throughput_records_per_second": None,
            "p95_latency_seconds": None,
            "p95_latency_sample_count": 0,
            "p95_latency_basis": None,
            "metric_scope": "latest_dag_run",
            "metric_notes": {},
            "details": {},
        }
        if stage == "ingestion":
            collector_records = []
            for task_id, source in collector_by_task.items():
                audit = audit_by_source.get(source)
                if audit:
                    changed = int(audit.get("changed_records_written") or 0)
                    collector_records.append({"task_id": task_id, **audit})
                    row["records_out"] = (row["records_out"] or 0) + changed
            row["metric_scope"] = "collector_audit_joined_by_dag_run_id"
            row["details"] = {
                "measured_collectors": len(collector_records),
                "collector_checks": collector_records,
                "records_out_definition": "sum(changed_records_written); unchanged records excluded",
            }
            if row["records_out"] is not None and stage_wall and stage_wall > 0:
                row["throughput_records_per_second"] = round(row["records_out"] / stage_wall, 3)
            collector_durations = [
                float(item["check_duration_seconds"])
                for item in collector_records
                if item.get("check_duration_seconds") is not None
            ]
            row["p95_latency_seconds"] = _percentile(collector_durations, 0.95)
            row["p95_latency_sample_count"] = len(collector_durations)
            row["p95_latency_basis"] = "per-collector check duration"
            row["details"]["p95_sample_count"] = len(collector_durations)
            row["metric_notes"]["acceptance_rate_percent"] = (
                "Not measured: raw ingestion preserves source rows and does not audit fetched/rejected counts."
            )
            row["metric_notes"]["rejected_records"] = (
                "Not applicable to raw capture; source rows are retained before Silver validation."
            )
            if not collector_durations:
                row["metric_notes"]["p95_latency_seconds"] = "Collector duration samples are unavailable."
        elif stage == "kafka":
            candidate_events = kafka_publish_record.get("attempted_events")
            delivered_events = kafka_publish_record.get("delivered_events")
            row["records_in"] = candidate_events
            row["records_out"] = delivered_events
            row["rejected_records"] = kafka_publish_record.get("rejected_events")
            row["acceptance_rate_percent"] = kafka_publish_record.get("acceptance_rate_percent")
            row["throughput_records_per_second"] = kafka_publish_record.get(
                "throughput_events_per_second"
            )
            row["p95_latency_seconds"] = kafka_publish_record.get(
                "p95_delivery_latency_seconds"
            )
            row["p95_latency_sample_count"] = int(
                kafka_publish_record.get("delivery_latency_samples") or 0
            )
            row["p95_latency_basis"] = "producer send-to-ack latency per message"
            row["details"] = {
                "kafka_to_bronze_p95_by_topic_seconds": kafka_bronze_latency,
                "consumer_lag_events_current": kafka_lag.get("total_lag_events"),
                "publisher_run": kafka_publish_record,
                "measurement_scope": "publisher counts and send-to-ack latency joined by dag_run_id",
            }
            row["metric_scope"] = "publisher_run_joined_by_dag_run_id"
            if not kafka_publish_record:
                row["metric_notes"].update({
                    "records_in": "Publisher per-run audit is missing; available after the instrumented publisher runs.",
                    "records_out": "Publisher per-run audit is missing; available after the instrumented publisher runs.",
                    "rejected_records": "Publisher per-run audit is missing.",
                    "acceptance_rate_percent": "No per-run attempted/delivered counts are available.",
                    "throughput_records_per_second": "No per-run delivered count and duration are available.",
                    "p95_latency_seconds": "No per-message producer acknowledgement samples are available.",
                })
            elif not candidate_events:
                row["metric_notes"]["acceptance_rate_percent"] = "Undefined for this run: no messages were sent (0/0)."
                row["metric_notes"]["p95_latency_seconds"] = "No messages were sent in this run, so no latency sample exists."
        elif stage == "bronze":
            row["records_in"] = bronze_batch_metrics.get("input_events_24h")
            row["records_out"] = bronze_batch_metrics.get("persisted_candidates_24h", bronze_events_24h)
            row["rejected_records"] = bronze_batch_metrics.get("rejected_events_24h")
            row["acceptance_rate_percent"] = bronze_batch_metrics.get("acceptance_rate_percent")
            row["throughput_records_per_second"] = bronze_batch_metrics.get(
                "throughput_records_per_second"
            )
            row["p95_latency_seconds"] = bronze_batch_metrics.get("p95_processing_seconds")
            row["p95_latency_sample_count"] = int(bronze_batch_metrics.get("batch_count_24h") or 0)
            row["p95_latency_basis"] = "Bronze micro-batch processing duration"
            row["metric_scope"] = "Bronze audit over last 24h; not isolated to one DAG run"
            row["details"] = {
                "persisted_events_24h": bronze_events_24h,
                "quarantined_events_24h": bronze_quarantine.get("rejected_events_24h"),
                "batch_audit_24h": bronze_batch_metrics,
                "acceptance_definition": "sum(valid_events) / sum(input_events) across non-empty Bronze micro-batches",
                "throughput_definition": "sum(persisted_candidates) / sum(batch processing seconds) across the 24-hour window",
                "kafka_lag_events_current": kafka_lag.get("total_lag_events"),
                "quarantine_by_reason": bronze_quarantine.get("by_reason", []),
            }
            if not bronze_batch_metrics.get("available"):
                row["metric_notes"].update({
                    "records_in": "Bronze micro-batch audit is not present yet; it starts recording after the Bronze stream restarts with the new instrumentation.",
                    "rejected_records": "Bronze micro-batch rejection counts are not yet audited.",
                    "acceptance_rate_percent": "Requires Bronze per-batch valid/input counts.",
                    "throughput_records_per_second": "Requires Bronze per-batch counts and processing duration.",
                    "p95_latency_seconds": "Requires Bronze per-batch processing duration samples.",
                })
            elif not bronze_batch_metrics.get("batch_count_24h"):
                row["metric_notes"]["acceptance_rate_percent"] = "Undefined: no non-empty Bronze micro-batches in the last 24 hours."
                row["metric_notes"]["p95_latency_seconds"] = "No non-empty Bronze micro-batch duration samples in the last 24 hours."
        elif stage == "silver":
            row["records_in"] = silver_input if run_silver else None
            row["records_out"] = silver_accepted if run_silver else None
            row["rejected_records"] = silver_rejected if run_silver else None
            row["acceptance_rate_percent"] = round(100 * silver_accepted / silver_input, 2) if silver_input else None
            row["throughput_records_per_second"] = round(silver_input / silver_duration, 3) if silver_duration > 0 else None
            silver_durations = [
                float(item["duration_seconds"])
                for item in run_silver
                if item.get("duration_seconds") is not None
            ]
            row["p95_latency_seconds"] = _percentile(silver_durations, 0.95)
            row["p95_latency_sample_count"] = len(silver_durations)
            row["p95_latency_basis"] = "per-topic Silver processing duration"
            row["metric_scope"] = "silver.stage_run_audit joined by exact Airflow dag_run_id"
            row["details"] = {
                "topic_runs": run_silver,
                "audit_available": bool(run_silver),
                "p95_sample_count": len(silver_durations),
            }
            if not silver_input:
                row["metric_notes"]["acceptance_rate_percent"] = "Undefined for this run: Silver received 0 events (0/0)."
            if not run_silver:
                row["metric_notes"].update({
                    "records_in": "Silver run audit is missing for this dag_run_id.",
                    "records_out": "Silver run audit is missing for this dag_run_id.",
                    "rejected_records": "Silver run audit is missing for this dag_run_id.",
                    "acceptance_rate_percent": "No run-scoped Silver input/output counts are available.",
                    "throughput_records_per_second": "No run-scoped Silver counts and duration are available.",
                })
            elif silver_duration <= 0:
                row["metric_notes"]["throughput_records_per_second"] = "Silver processing duration is unavailable or zero."
            if not silver_durations:
                row["metric_notes"]["p95_latency_seconds"] = "No per-topic Silver duration samples were recorded."
        elif stage == "gold":
            dbt_tests = read_dbt_test_metrics(
                Path(os.getenv("DBT_TARGET_DIR", "/opt/airflow/dbt/target")),
                by_task.get("dbt_pit_feature_tests"),
            )
            row["records_out"] = (
                gold_model_metrics.get("output_rows_across_model_materializations")
                if gold_model_metrics.get("available")
                else gold_features.get("feature_rows")
            )
            row["throughput_records_per_second"] = gold_model_metrics.get(
                "materialization_rows_per_model_second"
            )
            row["metric_scope"] = (
                "sum of per-model output rows / sum of model execution seconds; output tables may overlap"
                if gold_model_metrics.get("available")
                else "current Gold feature table snapshot; dbt model run artifact unavailable"
            )
            row["details"] = {
                "gold_cpi_rows": gold_cpi.get("gold_cpi_rows"),
                "gold_nowcasting_feature_rows": gold_features.get("feature_rows"),
                "dbt_gold_model_metrics": gold_model_metrics,
                "dbt_test_task_state": by_task.get("dbt_pit_feature_tests", {}).get("state"),
                "dbt_test_metrics": dbt_tests,
                "gold_output_count_definition": (
                    "sum of output row counts across this run's successful Gold model materializations; model outputs can overlap"
                    if gold_model_metrics.get("available")
                    else "current row count of gold_nowcasting_features; snapshot size, not rows created by this run"
                ),
                "gold_source_coverage_percent": gold_features.get("latest_target_source_coverage_pct"),
            }
            gold_task_durations = [
                float(task["duration_seconds"])
                for task in stage_tasks
                if task.get("duration_seconds") is not None
            ]
            row["p95_latency_seconds"] = _percentile(gold_task_durations, 0.95)
            row["p95_latency_sample_count"] = len(gold_task_durations)
            row["p95_latency_basis"] = "within-run dbt task duration (descriptive sample)"
            row["details"]["p95_sample_count"] = len(gold_task_durations)
            if not gold_task_durations:
                row["metric_notes"]["p95_latency_seconds"] = "Airflow task duration samples are unavailable."
            row["metric_notes"].update({
                "records_in": "dbt source-row counts are not captured per model run yet.",
                "rejected_records": "dbt tests are reported as task status; they do not provide a record rejection count.",
                "acceptance_rate_percent": "Not a record-level stage; use dbt test/task success rate instead.",
            })
            if not gold_model_metrics.get("available"):
                row["metric_notes"]["throughput_records_per_second"] = (
                    "Run-scoped dbt Gold model results or output-row counts are unavailable."
                )
        elif stage == "forecasting":
            all_backtest = nowcast_backtest.get("windows", {}).get("all_common_months", {})
            recent_window_name = next(
                (
                    name for name in nowcast_backtest.get("windows", {})
                    if name.startswith("most_recent_")
                ),
                None,
            )
            recent_backtest = (
                nowcast_backtest.get("windows", {}).get(recent_window_name, {})
                if recent_window_name else {}
            )
            all_models = all_backtest.get("models", {})
            recent_models = recent_backtest.get("models", {})
            forecast_quality = {
                "available": bool(nowcast_backtest.get("available")),
                "evaluation_note": nowcast_backtest.get("evaluation_note"),
                "all_common_origins": all_backtest.get("comparison_months"),
                "all_common_models": all_models,
                "all_common_mae_delta_full_vs_ar_midas_pp": (
                    all_models["full_multisource_ridge"]["mae_pp"]
                    - all_models["ar_midas_ridge"]["mae_pp"]
                    if {"full_multisource_ridge", "ar_midas_ridge"}.issubset(all_models)
                    else None
                ),
                "recent_window": recent_window_name,
                "recent_origins": recent_backtest.get("comparison_months"),
                "recent_models": recent_models,
                "recent_mae_delta_full_vs_ar_midas_pp": (
                    recent_models["full_multisource_ridge"]["mae_pp"]
                    - recent_models["ar_midas_ridge"]["mae_pp"]
                    if {"full_multisource_ridge", "ar_midas_ridge"}.issubset(recent_models)
                    else None
                ),
            }
            row["records_in"] = nowcast.get("training_months")
            row["records_out"] = 1 if nowcast.get("nowcast_mom_percent") is not None else None
            row["metric_scope"] = "latest forecast/ablation artifacts; timestamp checked against DAG start"
            row["details"] = {
                "forecast_target_month": nowcast.get("target_month"),
                "latest_actual_cpi_month": nowcast.get("latest_actual_cpi_month"),
                "model": nowcast.get("nowcast_model"),
                "nowcast_mom_percent": nowcast.get("nowcast_mom_percent"),
                "training_rows": nowcast.get("full_multisource_ridge_training_rows"),
                "training_origins_with_macro_pit": nowcast.get("full_multisource_ridge_training_origins_with_any_macro_source"),
                "current_macro_source_coverage_percent": nowcast.get("multisource_current_feature_coverage_pct"),
                "model_processing_seconds": nowcast.get("processing_duration_seconds"),
                "model_quality_backtest": forecast_quality,
                "ablation_run_id": ablation.get("evaluation_run_id"),
                "artifact_freshness": artifact_freshness,
            }
            forecast_task_durations = [
                float(task["duration_seconds"])
                for task in stage_tasks
                if task.get("duration_seconds") is not None
            ]
            row["p95_latency_seconds"] = _percentile(forecast_task_durations, 0.95)
            row["p95_latency_sample_count"] = len(forecast_task_durations)
            row["p95_latency_basis"] = "within-run forecasting task duration (descriptive sample)"
            row["details"]["p95_sample_count"] = len(forecast_task_durations)
            if not forecast_task_durations:
                row["metric_notes"]["p95_latency_seconds"] = "Airflow task duration samples are unavailable."
            if row["records_out"] and stage_wall and stage_wall > 0:
                row["throughput_records_per_second"] = round(row["records_out"] / stage_wall, 6)
            row["metric_notes"].update({
                "rejected_records": "Forecasting produces model outputs; it does not reject source records at this stage.",
                "acceptance_rate_percent": "Not applicable to model inference; forecast validation is reported separately.",
            })
        stage_rows.append(row)

    return {
        "available": bool(task_rows),
        "run_id": run_id,
        "dag_state_at_measurement": (dag_run or {}).get("state"),
        "generated_at": isoformat_now(now),
        "artifact_freshness": artifact_freshness,
        "stages": stage_rows,
        "tasks": task_details,
        "definitions": {
            "stage_wall_seconds": "min(task start) to max(task end); active stage ends at report time",
            "records_out": "null means unavailable/not attributable, not zero",
            "throughput": "only computed when numerator and duration share a measurement scope",
        },
    }


def measure_kafka_bronze_lag(
    broker: str,
    bronze_offsets: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compare Kafka end offsets with the last persisted Bronze offset."""
    persisted = {
        (row["kafka_topic"], int(row["kafka_partition"])): int(row["max_kafka_offset"])
        for row in bronze_offsets
        if row.get("max_kafka_offset") is not None
    }
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "nowcpi-metrics-offset-probe",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    by_topic: list[dict[str, Any]] = []
    try:
        metadata = consumer.list_topics(timeout=10)
        for topic in sorted(TOPIC_SOURCES):
            topic_meta = metadata.topics.get(topic)
            if topic_meta is None or topic_meta.error is not None:
                by_topic.append(
                    {"topic": topic, "status": "unavailable", "lag_events": None}
                )
                continue
            total_lag = 0
            total_retention_gap = 0
            measured_partitions = 0
            max_partition_lag = 0
            for partition_id, partition_meta in topic_meta.partitions.items():
                if partition_meta.error is not None:
                    continue
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition_id), timeout=10, cached=False
                )
                last_bronze = persisted.get((topic, partition_id))
                next_required_offset = low if last_bronze is None else last_bronze + 1
                total_retention_gap += max(0, low - next_required_offset)
                next_readable_offset = max(low, next_required_offset)
                lag = max(0, high - next_readable_offset)
                total_lag += lag
                max_partition_lag = max(max_partition_lag, lag)
                measured_partitions += 1
            by_topic.append(
                {
                    "topic": topic,
                    "status": "measured" if measured_partitions else "unavailable",
                    "partitions": measured_partitions,
                    "lag_events": total_lag if measured_partitions else None,
                    "max_partition_lag_events": max_partition_lag
                    if measured_partitions
                    else None,
                    "expired_offset_gap_events": total_retention_gap
                    if measured_partitions
                    else None,
                }
            )
    except Exception as exc:
        return {"available": False, "error": str(exc), "by_topic": []}
    finally:
        consumer.close()
    measured = [row for row in by_topic if row["status"] == "measured"]
    return {
        "available": bool(measured),
        "total_lag_events": sum(row["lag_events"] for row in measured),
        "expired_offset_gap_events": sum(
            row["expired_offset_gap_events"] for row in measured
        ),
        "by_topic": by_topic,
    }


def read_nowcast_backtest(output_dir: Path) -> dict[str, Any]:
    """Compare the primary multi-source model with baselines on common folds."""
    path = output_dir / "midas_backtest_predictions.csv"
    if not path.is_file():
        return {"available": False, "reason": f"Missing {path.name}"}
    raw_rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8", newline="") as source:
        raw_rows = list(csv.DictReader(source))

    actual_by_month: dict[str, float] = {}
    for row in raw_rows:
        try:
            actual_by_month[row["target_month"][:7]] = float(
                row["actual_cpi_mom_percent"]
            )
        except (KeyError, TypeError, ValueError):
            continue

    rows: list[dict[str, float | str]] = []
    for row in raw_rows:
        try:
            target = datetime.fromisoformat(row["target_month"][:10]).date()
            actual = float(row["actual_cpi_mom_percent"])
            cpi_only = float(row["cpi_only_ridge"])
            ar_midas = float(row["ar_midas_ridge"])
            full_multisource = float(row["full_multisource_ridge"])
            naive_text = row.get("naive_last_cpi", "")
            if naive_text:
                naive = float(naive_text)
            else:
                prior_month = (target.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
                naive = actual_by_month[prior_month]
        except (KeyError, TypeError, ValueError):
            continue
        values = (actual, cpi_only, ar_midas, full_multisource, naive)
        if all(value == value and abs(value) != float("inf") for value in values):
            rows.append(
                {
                    "target_month": target.isoformat(),
                    "actual": actual,
                    "naive": naive,
                    "cpi_only": cpi_only,
                    "ar_midas": ar_midas,
                    "full_multisource": full_multisource,
                }
            )
    rows.sort(key=lambda row: str(row["target_month"]))
    if not rows:
        return {"available": False, "reason": "No common finite naïve/CPI-only/AR-MIDAS/full multi-source predictions"}

    def metrics(frame: list[dict[str, float | str]], column: str) -> dict[str, float]:
        actuals = [float(row["actual"]) for row in frame]
        predictions = [float(row[column]) for row in frame]
        errors = [actual - predicted for actual, predicted in zip(actuals, predictions)]
        mape_rows = [
            (actual, predicted)
            for actual, predicted in zip(actuals, predictions)
            if abs(actual) >= 0.05
        ]
        smape_values = [
            200 * abs(actual - predicted) / (abs(actual) + abs(predicted))
            if abs(actual) + abs(predicted) > 0
            else 0.0
            for actual, predicted in zip(actuals, predictions)
        ]
        return {
            "mae_pp": round(sum(abs(error) for error in errors) / len(errors), 6),
            "rmse_pp": round((sum(error * error for error in errors) / len(errors)) ** 0.5, 6),
            "mape_pct_excluding_near_zero": round(
                100 * sum(abs(actual - predicted) / abs(actual) for actual, predicted in mape_rows) / len(mape_rows), 6
            ) if mape_rows else None,
            "mape_scored_months": len(mape_rows),
            "mape_excluded_months": len(actuals) - len(mape_rows),
            "smape_pct": round(sum(smape_values) / len(smape_values), 6),
            "directional_accuracy_pct": round(
                100 * sum(
                    (actual == 0 and predicted == 0)
                    or (actual > 0 and predicted > 0)
                    or (actual < 0 and predicted < 0)
                    for actual, predicted in zip(actuals, predictions)
                ) / len(errors),
                2,
            ),
        }

    windows = {
        "all_common_months": rows,
        f"most_recent_{min(9, len(rows))}_common_months": rows[-9:],
    }
    comparisons = {}
    for window_name, frame in windows.items():
        comparisons[window_name] = {
            "comparison_months": len(frame),
            "models": {
                name: metrics(frame, column)
                for name, column in [
                    ("naive_last_cpi", "naive"),
                    ("cpi_only_ridge", "cpi_only"),
                    ("ar_midas_ridge", "ar_midas"),
                    ("full_multisource_ridge", "full_multisource"),
                ]
            },
        }

    full = comparisons["all_common_months"]["models"]
    naive_metrics = full["naive_last_cpi"]
    model_metrics = full["full_multisource_ridge"]
    paired_comparisons: dict[str, dict[str, Any]] = {}
    paired_path = output_dir / "midas_paired_comparisons.csv"
    if paired_path.is_file():
        with paired_path.open(encoding="utf-8", newline="") as source:
            for row in csv.DictReader(source):
                if (
                    row.get("proposed_model") != "full_multisource_ridge"
                    or row.get("baseline_model") not in {"naive_last_cpi", "ar_midas_ridge"}
                    or row.get("evaluation_window") not in {
                        "all_pairwise_common_origins",
                        "latest_36_pairwise_common_origins",
                        "latest_24_pairwise_common_origins",
                        "latest_9_descriptive_only",
                    }
                ):
                    continue
                window_name = row["evaluation_window"]
                baseline_name = row["baseline_model"]
                paired_comparisons.setdefault(window_name, {})[baseline_name] = {
                    key: (float(row[key]) if row.get(key) not in {None, ""} else None)
                    for key in (
                        "paired_origins",
                        "mae_delta_pp_proposed_minus_baseline",
                        "mae_delta_95ci_low_pp",
                        "mae_delta_95ci_high_pp",
                        "rmse_delta_pp_proposed_minus_baseline",
                        "dm_pvalue_mae_loss",
                        "dm_pvalue_squared_loss",
                    )
                }
    model_source = "full_multisource_ridge"
    return {
        "available": True,
        "comparison_months": len(rows),
        "windows": comparisons,
        "baseline": {"model": "naive_last_cpi", **naive_metrics},
        "model": {"model": model_source, **model_metrics},
        "paired_comparisons": paired_comparisons,
        "mae_improvement_percent": round(
            100 * (naive_metrics["mae_pp"] - model_metrics["mae_pp"])
            / naive_metrics["mae_pp"], 2
        ) if naive_metrics["mae_pp"] else None,
        "rmse_improvement_percent": round(
            100 * (naive_metrics["rmse_pp"] - model_metrics["rmse_pp"])
            / naive_metrics["rmse_pp"], 2
        ) if naive_metrics["rmse_pp"] else None,
        "directional_accuracy_delta_pp": round(
            model_metrics["directional_accuracy_pct"]
            - naive_metrics["directional_accuracy_pct"], 2
        ),
        "evaluation_note": (
            "Expanding-window pseudo-vintage. Historical source-release vintages are "
            "unavailable; latest-nine results are descriptive only and DM inference "
            "is not reported below 24 paired origins."
        ),
    }


def latest_collector_group(
    records: list[dict[str, Any]], dag_run_id: str | None = None
) -> list[dict[str, Any]]:
    """Group the most recent parallel collector checks for one DAG run."""
    if not records:
        return []
    if dag_run_id:
        exact = [row for row in records if row.get("dag_run_id") == dag_run_id]
        if exact:
            return exact
        # Only use the legacy time-window fallback when these rows predate
        # run-id audit fields; never attach a different known run's collectors.
        legacy_records = [row for row in records if not row.get("dag_run_id")]
        if not legacy_records:
            return []
        records = legacy_records
    latest = max(records, key=lambda row: str(row.get("check_completed_at") or ""))
    run_id = latest.get("dag_run_id")
    if run_id:
        return [row for row in records if row.get("dag_run_id") == run_id]
    latest_start = parse_time(latest.get("check_started_at"))
    if not latest_start:
        return [latest]
    # Older audit rows may not include dag_run_id. Collector tasks in a run
    # launch together; use a bounded window only for those legacy rows.
    return [
        row
        for row in records
        if not row.get("dag_run_id")
        and (start := parse_time(row.get("check_started_at"))) is not None
        and abs((latest_start - start).total_seconds()) <= 15 * 60
    ]


def summarize_bursts(
    event_rows: list[dict[str, Any]], gap_seconds: int = 60
) -> tuple[int, list[dict[str, Any]]]:
    """Calculate rates within activity bursts, excluding long idle gaps."""
    events = [
        row
        for row in event_rows
        if parse_time(row.get("kafka_timestamp"))
        and parse_time(row.get("bronze_ingested_at"))
    ]
    events.sort(key=lambda row: parse_time(row["kafka_timestamp"]))
    groups: list[list[dict[str, Any]]] = []
    for event in events:
        event_time = parse_time(event["kafka_timestamp"])
        if not groups:
            groups.append([event])
            continue
        previous_time = parse_time(groups[-1][-1]["kafka_timestamp"])
        if (event_time - previous_time).total_seconds() > gap_seconds:
            groups.append([event])
        else:
            groups[-1].append(event)

    bursts = []
    for group in groups:
        first_kafka = min(parse_time(row["kafka_timestamp"]) for row in group)
        last_kafka = max(parse_time(row["kafka_timestamp"]) for row in group)
        last_bronze = max(parse_time(row["bronze_ingested_at"]) for row in group)
        source_span = (last_kafka - first_kafka).total_seconds()
        end_to_end_span = (last_bronze - first_kafka).total_seconds()
        bursts.append(
            {
                "persisted_events": len(group),
                "first_kafka_event_at": isoformat_now(first_kafka),
                "last_kafka_event_at": isoformat_now(last_kafka),
                "last_bronze_commit_at": isoformat_now(last_bronze),
                "event_publish_rate_per_second": round(len(group) / source_span, 3)
                if source_span > 0
                else None,
                "effective_persisted_events_per_second": round(
                    len(group) / end_to_end_span, 3
                )
                if end_to_end_span > 0
                else None,
            }
        )
    return len(events), bursts


def build_sql(existing_silver_tables: set[str] | None = None) -> tuple[str, str, str, str]:
    quality_selects = []
    for domain, table in SILVER_TABLES.items():
        nonpositive_metric = (
            "count_if(value <= 0)"
            if domain in POSITIVE_VALUE_DOMAINS
            else "CAST(NULL AS BIGINT)"
        )
        quality_selects.append(
            f"""SELECT '{domain}' AS domain, count(*) AS row_count,
                count_if(value IS NULL OR is_nan(value)) AS null_or_nan_values,
                count_if(country IS NULL OR trim(country) = '') AS missing_country,
                count_if(observation_date IS NULL) AS missing_observation_date,
                {nonpositive_metric} AS nonpositive_values,
                count(*) - count(DISTINCT concat(
                    coalesce(series_id, ''), '|', coalesce(country, ''), '|',
                    coalesce(frequency, ''), '|',
                    coalesce(cast(observation_date AS varchar), ''), '|', coalesce(unit, '')
                )) AS duplicate_business_keys,
                max(observation_date) AS latest_observation_date
              FROM delta.silver.{table}"""
        )
    rejected_selects = []
    rejection_reason_selects = []
    for domain, table in REJECTED_TABLES.items():
        if existing_silver_tables is not None and table not in existing_silver_tables:
            continue
        rejected_selects.append(
            f"""SELECT '{domain}' AS domain, count(*) AS rejected_events,
                count_if(rejection_reason LIKE '%missing_or_non_numeric_value%')
                    AS missing_or_non_numeric_events,
                count_if(rejection_reason = 'future_observation_date'
                    OR rejection_reason LIKE '%;future_observation_date%'
                    OR rejection_reason LIKE '%observation_out_of_scope%')
                    AS policy_excluded_events
              FROM delta.silver.{table}"""
        )
        rejection_reason_selects.append(
            f"""SELECT '{domain}' AS domain, rejection_reason, count(*) AS rejected_events
              FROM delta.silver.{table}
              GROUP BY rejection_reason"""
        )
    quality_sql = " UNION ALL ".join(quality_selects)
    rejected_sql = " UNION ALL ".join(rejected_selects) or (
        "SELECT CAST(NULL AS varchar) AS domain, CAST(NULL AS bigint) AS rejected_events, "
        "CAST(NULL AS bigint) AS missing_or_non_numeric_events, "
        "CAST(NULL AS bigint) AS policy_excluded_events WHERE false"
    )
    rejection_reasons_sql = " UNION ALL ".join(rejection_reason_selects) or (
        "SELECT CAST(NULL AS varchar) AS domain, CAST(NULL AS varchar) AS rejection_reason, "
        "CAST(NULL AS bigint) AS rejected_events WHERE false"
    )
    latency_sql = """SELECT kafka_topic, count(*) AS persisted_events,
        min(kafka_timestamp) AS first_kafka_event_at,
        max(kafka_timestamp) AS last_kafka_event_at,
        max(bronze_ingested_at) AS latest_bronze_commit_at,
        round(avg(date_diff('millisecond', kafka_timestamp, bronze_ingested_at)) / 1000.0, 3)
            AS avg_kafka_to_bronze_seconds,
        approx_percentile(
            date_diff('millisecond', kafka_timestamp, bronze_ingested_at) / 1000.0, 0.95
        ) AS p95_kafka_to_bronze_seconds
      FROM delta.bronze.kafka_events
      WHERE kafka_timestamp >= current_timestamp - INTERVAL '24' HOUR
      GROUP BY kafka_topic
      ORDER BY kafka_topic"""
    return quality_sql, rejected_sql, rejection_reasons_sql, latency_sql


def duration_seconds(start: Any, end: Any) -> float | None:
    start_time, end_time = parse_time(start), parse_time(end)
    if not start_time or not end_time:
        return None
    return round((end_time - start_time).total_seconds(), 3)


def markdown_report(report: dict[str, Any]) -> str:
    def cell(value: Any, suffix: str = "") -> str:
        return "N/A" if value is None else f"{value}{suffix}"

    lines = [
        "# NowCPI Pipeline Metrics",
        "",
        f"Generated at: `{report['generated_at']}`",
        "",
        "## Provisional SLO attainment",
        "",
        f"- Source freshness: **{report['metric_slo_attainment']['freshness']['passed_sources']}/{report['metric_slo_attainment']['freshness']['measured_sources']} ({report['metric_slo_attainment']['freshness']['achievement_percent']}%)**; targets are daily ≤3d, monthly ≤75d, annual ≤400d, and the latest collector check must be successful.",
        f"- Scheduled DAG latency: **{report['metric_slo_attainment']['dag_latency']['passed_runs']}/{report['metric_slo_attainment']['dag_latency']['measured_runs']} runs ({report['metric_slo_attainment']['dag_latency']['achievement_percent']}%)**; target ≤{report['metric_slo_attainment']['dag_latency']['target_seconds']}s ({report['metric_slo_attainment']['dag_latency']['duration_seconds']}s observed). Manual full-history rebuilds are shown in Pipeline Latency but excluded from this daily-run SLO.",
        f"- Kafka → Bronze P95: **{report['metric_slo_attainment']['kafka_bronze_latency']['passed_topics']}/{report['metric_slo_attainment']['kafka_bronze_latency']['measured_topics']} topics ({report['metric_slo_attainment']['kafka_bronze_latency']['achievement_percent']}%)**; target ≤{report['metric_slo_attainment']['kafka_bronze_latency']['target_seconds']}s.",
        f"- Silver integrity checks: **{report['metric_slo_attainment']['silver_integrity']['passed_checks']}/{report['metric_slo_attainment']['silver_integrity']['measured_checks']} ({report['metric_slo_attainment']['silver_integrity']['achievement_percent']}%)**; checks are domain-aware. IIP is now an index level and must be positive.",
        "- Throughput: **not scored**; an expected workload/throughput target has not been agreed, so the report shows observed rates only.",
        "These are provisional targets for transparent tracking, not externally established guarantees.",
        "",
        "## Source Observation Freshness",
        "",
        "This section reports observation-age freshness from the latest stored source period plus the last collector-check status. It is not source publication-to-ingestion latency; that requires trustworthy release timestamps. The isolated synthetic benchmark reports processing latency only and is not a source-freshness measurement.",
        "",
        "| Source | Last check | Warning | Observation age / target | SLO | Latest observation | Release timestamp | New records |",
        "|---|---|---|---:|---|---|---|---:|",
    ]
    for row in report["data_freshness"]["sources"]:
        lines.append(
            f"| {row['source']} ({row['expected_source_frequency']}) | {row['last_check_status']} | {'; '.join(row['warnings']) or '—'} | {row['observation_age_days']}d / {row['freshness_slo_days']}d "
            f"| {'PASS' if row['freshness_slo_met'] else 'FAIL'} "
            f"| {row['latest_stored_observation_period']} (age {row['observation_age_days']}d) | {row['latest_stored_release_ts']} "
            f"| {row['changed_records_written']} |"
        )
    lines += [
        "",
        "## Pipeline Latency",
        "",
        f"Latest DAG run: `{report['pipeline_latency']['latest_dag_run'].get('run_id')}` "
        f"(state: `{report['pipeline_latency']['latest_dag_run'].get('state')}`)."
        if report['pipeline_latency']['latest_dag_run']
        else "Latest DAG run: unavailable.",
        f"DAG duration: `{report['pipeline_latency']['latest_dag_duration_seconds']}` seconds.",
        "Kafka-to-Bronze latency is measured per event. Silver/Gold task duration and reliability are read from Airflow history; per-record commit timestamps are not yet stored.",
        "",
        "| Kafka topic | Persisted events (24h) | Average Kafka → Bronze (s) | P95 (s) |",
        "|---|---:|---:|---:|",
    ]
    for row in report["pipeline_latency"]["kafka_to_bronze_by_topic"]:
        lines.append(
            f"| {row['kafka_topic']} | {row['persisted_events']} "
            f"| {row['avg_kafka_to_bronze_seconds']} | {row['p95_kafka_to_bronze_seconds']} |"
        )
    stage_metrics = report["pipeline_latency"].get("stage_metrics", {})
    lines += ["", "## Metrics by pipeline stage", ""]
    if stage_metrics.get("available"):
        lines += [
            f"Run: `{stage_metrics.get('run_id')}`; Airflow state at measurement: `{stage_metrics.get('dag_state_at_measurement')}`.",
            "Stage duration is wall time from its first task start to its last task end. Counts are joined to the run where the source audit supports it; wider-window values are labeled in the scope column.",
            "",
            "| Stage | Status | Tasks successful/total | Retries | Wall (s) | Task time sum (s) | Input | Output | Rejected | Acceptance | Throughput/s | P95 latency (s) | Scope |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
        ]
        for row in stage_metrics.get("stages", []):
            lines.append(
                f"| {row['stage']} | {row['status']} | {row['succeeded_tasks']}/{row['task_count']} "
                f"| {row['retried_tasks']} | {row['stage_wall_seconds']} | {row['task_duration_sum_seconds']} "
                f"| {cell(row['records_in'])} | {cell(row['records_out'])} | {cell(row['rejected_records'])} "
                f"| {cell(row['acceptance_rate_percent'], '%')} | {cell(row['throughput_records_per_second'])} "
                f"| {cell(row['p95_latency_seconds'])} | {row['metric_scope']} |"
            )
        stage_by_name = {row["stage"]: row for row in stage_metrics.get("stages", [])}
        gold_details = stage_by_name.get("gold", {}).get("details", {})
        dbt_tests = gold_details.get("dbt_test_metrics", {})
        gold_models = gold_details.get("dbt_gold_model_metrics", {})
        lines += ["", "### Gold data and test metrics", ""]
        if gold_models.get("available"):
            lines.append(
                f"This run materialized **{gold_models.get('output_rows_across_model_materializations')} model-output rows** "
                f"across {gold_models.get('models_with_row_counts')}/{gold_models.get('models_in_run')} Gold models; "
                f"aggregate model execution time **{gold_models.get('model_execution_seconds_for_counted_outputs')}s**; "
                f"materialization rate **{gold_models.get('materialization_rows_per_model_second')} rows/model-second**."
            )
            lines.append(
                "Counts are summed across model outputs, so the same underlying observations may appear in multiple tables. "
                "The append-only feature-vintage model counts only rows appended during this run."
            )
            lines.append("")
            lines.append("| Gold model | Output rows | Model seconds | Count scope |")
            lines.append("|---|---:|---:|---|")
            for model in gold_models.get("models", []):
                lines.append(
                    f"| {model.get('model')} | {model.get('output_rows')} | "
                    f"{model.get('execution_seconds')} | {model.get('count_scope')} |"
                )
        else:
            lines.append(
                f"Per-model output metrics unavailable: {gold_models.get('reason', 'dbt Gold run artifact not matched to this DAG run')}."
            )
            partial_models = gold_models.get("models", [])
            if partial_models:
                lines += [
                    "",
                    "| Gold model | Output rows | Model seconds | Count error |",
                    "|---|---:|---:|---|",
                ]
                for model in partial_models:
                    lines.append(
                        f"| {model.get('model')} | {model.get('output_rows')} "
                        f"| {model.get('execution_seconds')} | {model.get('count_error', '')} |"
                    )
        if dbt_tests.get("available"):
            lines.append(
                f"Current Gold snapshots: **{gold_details.get('gold_cpi_rows')} CPI rows**, "
                f"**{gold_details.get('gold_nowcasting_feature_rows')} nowcasting feature rows**. "
                f"PIT/data tests: **{dbt_tests.get('passed')}/{dbt_tests.get('test_count')} passed**, "
                f"**{dbt_tests.get('failed')} failed**, **{dbt_tests.get('failure_rows')} failing rows**; "
                f"test execution time sum **{dbt_tests.get('execution_seconds')}s**."
            )
        else:
            lines.append(
                f"Gold table snapshots: {gold_details.get('gold_cpi_rows')} CPI rows and "
                f"{gold_details.get('gold_nowcasting_feature_rows')} feature rows. "
                f"Per-test counts unavailable: {dbt_tests.get('reason', 'dbt test artifact not found or not matched to this run')}"
            )
        lines.append(
            "The feature/CPI counts below are current table sizes. Input rows remain N/A because Gold has multiple "
            "overlapping inputs; rejected rows and acceptance rate are not Silver-style record metrics."
        )
        forecast_details = stage_by_name.get("forecasting", {}).get("details", {})
        quality = forecast_details.get("model_quality_backtest", {})
        lines += ["", "### Forecasting quality metrics", ""]
        if quality.get("available"):
            lines += [
                f"Historical backtest (all common origins: **{quality.get('all_common_origins')}**):",
                "",
                "| Model | MAE (pp) | RMSE (pp) | Direction accuracy |",
                "|---|---:|---:|---:|",
            ]
            for model_name, label in (
                ("naive_last_cpi", "Naïve"),
                ("ar_midas_ridge", "AR-MIDAS"),
                ("full_multisource_ridge", "Full multi-source Ridge"),
            ):
                metric = quality.get("all_common_models", {}).get(model_name)
                if metric:
                    lines.append(
                        f"| {label} | {metric.get('mae_pp')} | {metric.get('rmse_pp')} "
                        f"| {metric.get('directional_accuracy_pct')}% |"
                    )
            lines.append(
                f"Full multi-source minus AR-MIDAS MAE: **{quality.get('all_common_mae_delta_full_vs_ar_midas_pp')} pp**."
            )
            recent_models = quality.get("recent_models", {})
            if recent_models:
                lines.append(
                    f"Recent window `{quality.get('recent_window')}` ({quality.get('recent_origins')} origins): "
                    f"Full multi-source MAE **{recent_models.get('full_multisource_ridge', {}).get('mae_pp')} pp**, "
                    f"AR-MIDAS MAE **{recent_models.get('ar_midas_ridge', {}).get('mae_pp')} pp**; "
                    f"delta **{quality.get('recent_mae_delta_full_vs_ar_midas_pp')} pp**."
                )
            lines.append(quality.get("evaluation_note") or "Historical backtest; interpret with its vintage limitations.")
        else:
            lines.append("Forecast quality backtest unavailable: no comparable prediction artifact was found.")
        notes = [
            f"- **{row['stage']}**: {field} — {reason}"
            for row in stage_metrics.get("stages", [])
            for field, reason in row.get("metric_notes", {}).items()
        ]
        p95_specs = [
            f"- **{row['stage']}**: {row['p95_latency_seconds']}s from "
            f"{row['p95_latency_sample_count']} sample(s), basis: {row['p95_latency_basis']}"
            for row in stage_metrics.get("stages", [])
            if row.get("p95_latency_seconds") is not None
        ]
        lines += [
            "",
            "P95 includes its sample basis in the JSON/CSV details; within-run task samples are descriptive and are not long-run SLO estimates. `N/A` means the metric is undefined or has no instrumented sample, not zero. Wider-window values are labeled and must not be read as exact run deltas.",
            "",
            "P95 sample basis:",
            *p95_specs,
            "",
            "Unavailable or inapplicable metrics:",
            *notes,
            "",
            "Task-level start/end, state, duration and retry count are exported separately.",
        ]
    else:
        lines.append("Stage task metrics are unavailable because Airflow metadata for a latest run could not be read.")
    lag = report["metric_slo_attainment"]["kafka_consumer_lag"]
    lines += ["", "## Kafka consumer lag", ""]
    if lag.get("available"):
        lines.append(
            f"Kafka offsets not yet present in Bronze: **{lag.get('total_lag_events')}**; "
            f"expired-offset gaps: **{lag.get('expired_offset_gap_events')}**."
        )
        lines += ["", "| Topic | Lag events | Largest partition lag | Expired offset gap |", "|---|---:|---:|---:|"]
        for row in lag.get("by_topic", []):
            lines.append(
                f"| {row['topic']} | {row.get('lag_events')} | "
                f"{row.get('max_partition_lag_events')} | {row.get('expired_offset_gap_events')} |"
            )
        lines.append("Offset lag is relative to the oldest offset still retained by Kafka; expired gaps are reported separately.")
    else:
        lines.append(f"Unavailable: {lag.get('error', 'Kafka topics were not measurable')}.")

    reliability = report["airflow_reliability"]
    lines += ["", "## Airflow reliability", ""]
    if reliability.get("available"):
        lines += [
            "| Window | Scheduled runs | Success rate | DAG duration p50 (s) | p95 (s) |",
            "|---|---:|---:|---:|---:|",
        ]
        for window, row in reliability["windows"].items():
            lines.append(
                f"| {window} | {row['scheduled_runs']} | {row['success_rate_percent']}% "
                f"({row['successful_runs']}/{row['terminal_runs']} terminal) | "
                f"{row['dag_duration_p50_seconds']} | {row['dag_duration_p95_seconds']} |"
            )
        lines += ["", "| Task | Failures | Retry rate | Duration p50 (s) | p95 (s) |", "|---|---:|---:|---:|---:|"]
        for row in reliability.get("tasks_30d", []):
            lines.append(
                f"| {row['task_id']} | {row['failed_instances']}/{row['terminal_instances']} "
                f"({row['failure_rate_percent']}%) | {row['retry_rate_percent']}% "
                f"({row['retried_instances']} instances) | {row['duration_p50_seconds']} | "
                f"{row['duration_p95_seconds']} |"
            )
    else:
        lines.append(f"Airflow metadata history unavailable: {reliability.get('error')}.")
    backtest = report["nowcast_backtest"]
    lines += ["", "## Primary multi-source nowcast versus baselines", ""]
    if backtest.get("available"):
        for window_name, comparison in backtest.get("windows", {}).items():
            lines += [
                f"### {window_name.replace('_', ' ').title()}",
                "",
                f"Common folds: **{comparison['comparison_months']}**.",
                "| Model | MAE (pp) | RMSE (pp) | Direction accuracy | MAPE %* | sMAPE % |",
                "|---|---:|---:|---:|---:|---:|",
            ]
            for model_name, label in [
                ("naive_last_cpi", "Naïve: previous CPI MoM"),
                ("cpi_only_ridge", "CPI-only ridge"),
                ("ar_midas_ridge", "AR-MIDAS ridge"),
                ("full_multisource_ridge", "Full multi-source Ridge (primary)"),
            ]:
                metrics = comparison["models"][model_name]
                lines.append(
                    f"| {label} | {metrics['mae_pp']} | {metrics['rmse_pp']} "
                    f"| {metrics['directional_accuracy_pct']}% "
                    f"| {metrics['mape_pct_excluding_near_zero']} "
                    f"({metrics['mape_scored_months']} scored/{metrics['mape_excluded_months']} excluded) "
                    f"| {metrics['smape_pct']} |"
                )
            lines.append("\n*MAPE excludes realized CPI MoM values with absolute magnitude below 0.05 percentage points.*")
            naive = comparison["models"]["naive_last_cpi"]
            primary = comparison["models"]["full_multisource_ridge"]
            mae_gain = (
                round(100 * (naive["mae_pp"] - primary["mae_pp"]) / naive["mae_pp"], 2)
                if naive["mae_pp"] else None
            )
            lines.append(
                f"Full multi-source Ridge MAE improvement versus naïve: **{mae_gain}%**; "
                f"directional-accuracy change: **{round(primary['directional_accuracy_pct'] - naive['directional_accuracy_pct'], 2)} pp.**"
            )
            evidence_window = {
                "all_common_months": "all_pairwise_common_origins",
                "most_recent_9_common_months": "latest_9_descriptive_only",
            }.get(window_name)
            if evidence_window:
                paired = backtest.get("paired_comparisons", {}).get(evidence_window, {})
                for baseline_name, label in (
                    ("naive_last_cpi", "naive"),
                    ("ar_midas_ridge", "AR-MIDAS"),
                ):
                    comparison_stats = paired.get(baseline_name)
                    if not comparison_stats:
                        continue
                    ci_low = comparison_stats.get("mae_delta_95ci_low_pp")
                    ci_high = comparison_stats.get("mae_delta_95ci_high_pp")
                    dm_p = comparison_stats.get("dm_pvalue_mae_loss")
                    lines.append(
                        f"Paired vs {label}: ΔMAE **{comparison_stats.get('mae_delta_pp_proposed_minus_baseline')} pp** "
                        f"(95% CI [{ci_low}, {ci_high}]); DM p="
                        f"{dm_p if dm_p is not None else 'not reported (<24 origins)'}."
                    )
        lines.append(backtest["evaluation_note"])
    else:
        lines.append(f"Backtest comparison unavailable: {backtest.get('reason')}.")
    proxy_rows = report.get("sourcewise_ablation_proxy", [])
    lines += [
        "",
        "## Sourcewise pseudo-vintage proxy ablation",
        "",
        "These observation-date proxy results are diagnostic only: they do not reconstruct historical source revisions and are not strict PIT evidence.",
    ]
    if proxy_rows:
        lines += [
            "",
            "| Source | Status | Paired origins | Baseline MAE (pp) | Added-source MAE (pp) | MAE delta (pp) | Strict PIT claim |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
        for row in proxy_rows:
            lines.append(
                f"| {row.get('source')} | {row.get('status')} | {row.get('paired_origins')} "
                f"| {row.get('baseline_mae_pp')} | {row.get('source_added_mae_pp')} "
                f"| {row.get('mae_delta_pp_source_minus_baseline')} "
                f"| {row.get('strict_vintage_performance_claim_allowed')} |"
            )
    else:
        lines.append("Proxy artifact is not present yet; the next feature-ablation task run will generate it.")
    throughput = report["throughput"]
    lines += [
        "",
        "## Throughput",
        "",
        f"Bronze unique events persisted in the last 24h: **{throughput['persisted_events_24h']}**.",
        f"Latest observed burst: **{throughput['latest_burst']['persisted_events'] if throughput['latest_burst'] else 0} events**, effective rate **{throughput['latest_burst']['effective_persisted_events_per_second'] if throughput['latest_burst'] else None} events/s**.",
        f"Largest historical burst (backfill/replay may dominate; not steady-state throughput): **{throughput['peak_burst']['persisted_events'] if throughput['peak_burst'] else 0} events**, effective rate **{throughput['peak_burst']['effective_persisted_events_per_second'] if throughput['peak_burst'] else None} events/s**.",
        f"New or changed source records in the latest collector run: **{throughput['latest_collector_run_changed_records']}**; collector-window rate **{throughput['latest_collector_run_records_per_second']} records/s**.",
        "Rates are calculated within bursts separated by at most 60 seconds, so long idle periods do not dilute throughput. Bronze counts are deduplicated event keys, not raw Kafka delivery counts.",
        "",
        "## Data Quality",
        "",
        "| Silver domain | Rows | Null/NaN value | Missing country | Missing date | Non-positive | Duplicate keys | Integrity checks | Rejected | Missing/non-numeric rejects | Future-date policy rejects |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    reject_by_domain = {row["domain"]: row for row in report["data_quality"]["rejected_by_domain"]}
    integrity_by_domain = {
        row["domain"]: row for row in report["data_quality"]["integrity_by_domain"]
    }
    for row in report["data_quality"]["silver_by_domain"]:
        rejected = reject_by_domain.get(row["domain"], {})
        integrity = integrity_by_domain.get(row["domain"], {})
        lines.append(
            f"| {row['domain']} | {row['row_count']} | {row['null_or_nan_values']} "
            f"| {row['missing_country']} | {row['missing_observation_date']} "
            f"| {row['nonpositive_values'] if row['nonpositive_values'] is not None else 'N/A'} | {row['duplicate_business_keys']} "
            f"| {integrity.get('passed_checks', 0)}/{integrity.get('measured_checks', 0)} ({integrity.get('achievement_percent')}%) "
            f"| {rejected.get('rejected_events') if rejected.get('rejected_events') is not None else 'N/A'} "
            f"| {rejected.get('missing_or_non_numeric_events') if rejected.get('missing_or_non_numeric_events') is not None else 'N/A'} "
            f"| {rejected.get('policy_excluded_events') if rejected.get('policy_excluded_events') is not None else 'N/A'} |"
        )
    lines += [
        "",
        "Non-positive values are checked for CPI, CPI components, Brent, FX, fuel, and the monthly IIP index level. Reject counts are deduplicated by Kafka topic/partition/offset. The invalid share uses quarantined events as its denominator; accepted rows are deduplicated by business key, so they are not mixed into that percentage.",
        "",
        "| Domain | Rejected events | Missing/non-numeric | Invalid share of quarantined events | Future-date policy rejects |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in report["data_quality"]["rejection_metrics"]:
        invalid_share = row["invalid_share_of_rejects_percent"]
        rejected_events = row["rejected_events"] if row["rejection_table_available"] else "N/A (table missing)"
        invalid_events = row["invalid_or_non_numeric_events"] if row["rejection_table_available"] else "N/A"
        policy_events = row["policy_excluded_events"] if row["rejection_table_available"] else "N/A"
        lines.append(
            f"| {row['domain']} | {rejected_events} "
            f"| {invalid_events} "
            f"| {f'{invalid_share}%' if invalid_share is not None else 'N/A'} "
            f"| {policy_events} |"
        )
    unavailable_tables = report["data_quality"]["unavailable_rejection_tables"]
    if unavailable_tables:
        lines.append(
            "\nRejection counts are **not measured** for missing Silver rejection "
            f"tables: {', '.join(unavailable_tables)}. They are not reported as zero."
        )
    lines += [
        "",
        "### Rejection reasons",
        "",
        "| Domain | Reason | Events | Share of domain rejects |",
        "|---|---|---:|---:|",
    ]
    for row in report["data_quality"]["rejection_reason_metrics"]:
        lines.append(
            f"| {row['domain']} | `{row['rejection_reason']}` | {row['rejected_events']} | "
            f"{row['share_of_domain_rejects_percent']}% |"
        )
    bronze_quarantine = report["data_quality"]["bronze_envelope_quarantine"]
    lines += ["", "### Malformed Kafka envelopes", ""]
    if bronze_quarantine.get("available"):
        lines.append(
            f"Bronze quarantine events: **{bronze_quarantine.get('rejected_events')}** "
            f"({bronze_quarantine.get('rejected_events_24h')} in the last 24 hours)."
        )
    else:
        lines.append("No Bronze envelope quarantine table exists yet; malformed Kafka events will be retained there when encountered.")
    topology = report["trino_topology"]
    lines += [
        "",
        "## Trino topology",
        "",
        f"Active Trino nodes: **{topology.get('active_nodes')}**; coordinators: **{topology.get('active_coordinators')}**; dedicated workers: **{topology.get('active_workers')}**.",
        "The coordinator can also execute queries when `node-scheduler.include-coordinator=true`; the Trino UI may therefore show one active worker with this single-node setup.",
        "",
        "## Gold feature coverage",
        "",
        f"Latest feature month: **{report['gold_feature_coverage'].get('latest_target_month')}**; latest CPI observation: **{report['gold_feature_coverage'].get('latest_cpi_observation_month')}** ({report['gold_feature_coverage'].get('latest_cpi_age_months')} months old); five-source coverage: **{report['gold_feature_coverage'].get('latest_target_source_coverage_pct')}%**; mean across feature rows: **{report['gold_feature_coverage'].get('mean_source_coverage_pct')}%**.",
        f"Latest feature input ages: CPI {report['gold_feature_coverage'].get('latest_cpi_age_months')} month(s), Brent {report['gold_feature_coverage'].get('latest_brent_age_days')} day(s), USD/VND {report['gold_feature_coverage'].get('latest_fx_age_days')} day(s), fuel {report['gold_feature_coverage'].get('latest_fuel_age_days')} day(s), IIP {report['gold_feature_coverage'].get('latest_iip_age_months')} month(s).",
        "Coverage is the share of five inputs present: latest CPI before target month, Brent, USD/VND, fuel, and latest available prior-month Vietnam IIP. It measures availability, not predictive accuracy.",
    ]
    lines += ["", "## Metric limitations and recommended additions", ""]
    for item in report["recommended_additional_metrics"]:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-file", type=Path, default=Path("/opt/airflow/audit/source_checks.jsonl"))
    parser.add_argument(
        "--kafka-publish-audit-file",
        type=Path,
        default=Path("/opt/airflow/audit/kafka_publish_runs.jsonl"),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("/opt/airflow/audit/reports"))
    parser.add_argument("--trino-host", default=os.getenv("TRINO_HOST", "trino"))
    parser.add_argument("--trino-port", type=int, default=int(os.getenv("TRINO_PORT", "8080")))
    parser.add_argument("--trino-user", default=os.getenv("TRINO_USER", "nowcpi"))
    parser.add_argument("--kafka-broker", default=os.getenv("KAFKA_BROKER", "kafka:9092"))
    parser.add_argument(
        "--forecasting-output-dir",
        type=Path,
        default=Path("/opt/airflow/forecasting/outputs"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    now = datetime.now(timezone.utc)
    audit_records = read_audit(args.audit_file)
    conn = connect(
        host=args.trino_host,
        port=args.trino_port,
        user=args.trino_user,
        catalog="delta",
        schema="silver",
        http_scheme="http",
    )
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT table_name FROM delta.information_schema.tables "
            "WHERE table_schema = 'silver'"
        )
        existing_silver_tables = {
            str(row[0]) for row in cursor.fetchall()
        }
        quality_sql, rejected_sql, rejection_reasons_sql, latency_sql = build_sql(
            existing_silver_tables
        )
        cursor.execute(quality_sql)
        silver_quality = rows_as_dicts(cursor)
        cursor.execute(rejected_sql)
        rejected_quality = rows_as_dicts(cursor)
        available_rejection_domains = {row["domain"] for row in rejected_quality}
        for domain, table in REJECTED_TABLES.items():
            if domain not in available_rejection_domains:
                rejected_quality.append(
                    {
                        "domain": domain,
                        "rejected_events": None,
                        "missing_or_non_numeric_events": None,
                        "policy_excluded_events": None,
                        "rejection_table_available": False,
                        "rejection_table": table,
                    }
                )
            else:
                for row in rejected_quality:
                    if row["domain"] == domain:
                        row["rejection_table_available"] = True
                        row["rejection_table"] = table
        cursor.execute(rejection_reasons_sql)
        rejection_reason_counts = rows_as_dicts(cursor)
        cursor.execute(latency_sql)
        latencies = rows_as_dicts(cursor)
        cursor.execute(
            """SELECT kafka_topic, kafka_partition,
                max(kafka_offset) AS max_kafka_offset
              FROM delta.bronze.kafka_events
              GROUP BY kafka_topic, kafka_partition"""
        )
        bronze_offsets = rows_as_dicts(cursor)
        cursor.execute(
            """SELECT kafka_topic, kafka_timestamp, bronze_ingested_at
              FROM delta.bronze.kafka_events
              WHERE kafka_timestamp >= current_timestamp - INTERVAL '24' HOUR
              ORDER BY kafka_timestamp"""
        )
        recent_event_rows = rows_as_dicts(cursor)
        cursor.execute(
            """SELECT count(*) AS gold_cpi_rows,
                count_if(cpi_headline IS NULL) AS missing_headline_rows,
                max(month_start) AS latest_cpi_month
              FROM delta.gold.gold_cpi_monthly"""
        )
        gold_quality = rows_as_dicts(cursor)[0]
        cursor.execute("DESCRIBE delta.gold.gold_nowcasting_features")
        gold_feature_columns = {
            str(row.get("Column", row.get("column", ""))).lower()
            for row in rows_as_dicts(cursor)
        }
        age_columns = {
            "brent_observation_age_days",
            "fx_observation_age_days",
            "fuel_observation_age_days",
            "iip_observation_age_months",
        }
        if age_columns.issubset(gold_feature_columns):
            gold_feature_age_select = """
                max_by(brent_latest_observation_date, target_month)
                    AS latest_brent_observation_date,
                max_by(brent_observation_age_days, target_month)
                    AS latest_brent_age_days,
                max_by(fx_latest_observation_date, target_month)
                    AS latest_fx_observation_date,
                max_by(fx_observation_age_days, target_month)
                    AS latest_fx_age_days,
                max_by(fuel_latest_observation_date, target_month)
                    AS latest_fuel_observation_date,
                max_by(fuel_observation_age_days, target_month)
                    AS latest_fuel_age_days,
                max_by(iip_observation_month, target_month)
                    AS latest_iip_observation_month,
                max_by(iip_observation_age_months, target_month)
                    AS latest_iip_age_months,"""
        else:
            gold_feature_age_select = """
                CAST(NULL AS date) AS latest_brent_observation_date,
                CAST(NULL AS bigint) AS latest_brent_age_days,
                CAST(NULL AS date) AS latest_fx_observation_date,
                CAST(NULL AS bigint) AS latest_fx_age_days,
                CAST(NULL AS date) AS latest_fuel_observation_date,
                CAST(NULL AS bigint) AS latest_fuel_age_days,
                CAST(NULL AS date) AS latest_iip_observation_month,
                CAST(NULL AS integer) AS latest_iip_age_months,"""
        if "gold_built_at" in gold_feature_columns:
            gold_build_select = """
                max(gold_built_at) AS latest_gold_built_at,
                count(DISTINCT gold_build_id) AS gold_build_ids,"""
        else:
            gold_build_select = """
                CAST(NULL AS timestamp) AS latest_gold_built_at,
                CAST(NULL AS bigint) AS gold_build_ids,"""
        cursor.execute(
            f"""SELECT count(*) AS feature_rows,
                max(target_month) AS latest_target_month,
                max_by(cpi_observation_month, target_month)
                    AS latest_cpi_observation_month,
                max_by(cpi_observation_age_months, target_month)
                    AS latest_cpi_age_months,
                {gold_feature_age_select}
                {gold_build_select}
                max_by(feature_source_coverage_pct, target_month)
                    AS latest_target_source_coverage_pct,
                round(avg(feature_source_coverage_pct), 2)
                    AS mean_source_coverage_pct,
                count_if(feature_source_coverage_pct = 100)
                    AS fully_covered_feature_rows
              FROM delta.gold.gold_nowcasting_features"""
        )
        gold_feature_coverage = rows_as_dicts(cursor)[0]
        cursor.execute("SHOW TABLES FROM delta.bronze")
        bronze_tables = {row[0] for row in cursor.fetchall()}
        bronze_quarantine_metrics = {"available": False, "rejected_events": 0, "by_reason": []}
        if "kafka_events_quarantine" in bronze_tables:
            cursor.execute(
                """SELECT count(*) AS rejected_events,
                    count_if(quarantined_at >= current_timestamp - INTERVAL '24' HOUR)
                        AS rejected_events_24h
                  FROM delta.bronze.kafka_events_quarantine"""
            )
            bronze_quarantine_metrics.update(rows_as_dicts(cursor)[0])
            cursor.execute(
                """SELECT rejection_reason, count(*) AS rejected_events
                  FROM delta.bronze.kafka_events_quarantine
                  GROUP BY rejection_reason
                  ORDER BY rejected_events DESC"""
            )
            bronze_quarantine_metrics["by_reason"] = rows_as_dicts(cursor)
            bronze_quarantine_metrics["available"] = True
        bronze_batch_metrics = {"available": False, "window_hours": 24}
        if "kafka_batch_audit" in bronze_tables:
            cursor.execute(
                """SELECT count(*) AS batch_count_24h,
                    sum(input_events) AS input_events_24h,
                    sum(valid_events) AS valid_events_24h,
                    sum(rejected_events) AS rejected_events_24h,
                    sum(persisted_candidates) AS persisted_candidates_24h,
                    sum(processing_duration_seconds) AS processing_seconds_24h,
                    approx_percentile(processing_duration_seconds, 0.95)
                        AS p95_processing_seconds
                  FROM delta.bronze.kafka_batch_audit
                  WHERE batch_committed_at >= current_timestamp - INTERVAL '24' HOUR"""
            )
            bronze_batch_metrics.update(rows_as_dicts(cursor)[0])
            bronze_batch_metrics["available"] = True
            input_events_24h = bronze_batch_metrics.get("input_events_24h")
            processing_seconds_24h = bronze_batch_metrics.get("processing_seconds_24h")
            valid_events_24h = bronze_batch_metrics.get("valid_events_24h")
            if input_events_24h:
                bronze_batch_metrics["acceptance_rate_percent"] = round(
                    100 * float(valid_events_24h or 0) / float(input_events_24h), 2
                )
            else:
                bronze_batch_metrics["acceptance_rate_percent"] = None
            bronze_batch_metrics["throughput_records_per_second"] = (
                round(
                    float(bronze_batch_metrics.get("persisted_candidates_24h") or 0)
                    / float(processing_seconds_24h),
                    3,
                )
                if processing_seconds_24h and float(processing_seconds_24h) > 0
                else None
            )
        cursor.execute("SHOW TABLES FROM delta.silver")
        silver_tables = {str(row[0]).lower() for row in cursor.fetchall()}
        silver_stage_metrics = {"available": False, "latest_runs": []}
        if "stage_run_audit" in silver_tables:
            cursor.execute(
                """SELECT stage, kafka_topic, run_id, task_id, input_events,
                    accepted_events, rejected_events, stage_started_at, stage_committed_at,
                    date_diff('millisecond', stage_started_at, stage_committed_at) / 1000.0
                        AS duration_seconds
                  FROM delta.silver.stage_run_audit
                  ORDER BY stage_committed_at DESC
                  LIMIT 200"""
            )
            silver_stage_metrics = {
                "available": True,
                "latest_runs": rows_as_dicts(cursor),
            }
        cursor.execute(
            """SELECT count_if(state = 'active') AS active_nodes,
                count_if(state = 'active' AND coordinator) AS active_coordinators,
                count_if(state = 'active' AND NOT coordinator) AS active_workers
              FROM system.runtime.nodes"""
        )
        trino_topology = rows_as_dicts(cursor)[0]
    finally:
        conn.close()

    kafka_bronze_lag = measure_kafka_bronze_lag(args.kafka_broker, bronze_offsets)
    airflow_reliability = airflow_reliability_metrics(now)
    nowcast_backtest = read_nowcast_backtest(args.forecasting_output_dir)
    nowcast_artifact = _read_json_if_present(
        args.forecasting_output_dir
        / "production_models"
        / "full_multisource_ridge"
        / "latest.json"
    )
    ablation_manifest = _read_json_if_present(
        args.forecasting_output_dir / "feature_ablation_run_manifest.json"
    )
    proxy_sourcewise_ablation = _read_csv_records_if_present(
        args.forecasting_output_dir / "feature_ablation_sourcewise_proxy.csv"
    )

    event_count, bursts = summarize_bursts(recent_event_rows)
    latest_burst = max(bursts, key=lambda row: row["last_bronze_commit_at"], default=None)
    rate_bursts = [
        row for row in bursts if row["effective_persisted_events_per_second"] is not None
    ]
    peak_burst = max(
        rate_bursts,
        key=lambda row: row["effective_persisted_events_per_second"],
        default=None,
    )
    latest_dag = get_latest_dag_run()
    collector_group = latest_collector_group(
        audit_records, (latest_dag or {}).get("run_id")
    )
    changed_records = sum(int(row.get("changed_records_written") or 0) for row in collector_group)
    collector_starts = [parse_time(row.get("check_started_at")) for row in collector_group]
    collector_ends = [parse_time(row.get("check_completed_at")) for row in collector_group]
    collector_starts = [value for value in collector_starts if value]
    collector_ends = [value for value in collector_ends if value]
    collector_window = (
        (max(collector_ends) - min(collector_starts)).total_seconds()
        if collector_starts and collector_ends
        else None
    )
    collector_rate = (
        round(changed_records / collector_window, 3)
        if collector_window and collector_window > 0
        else None
    )
    source_freshness = latest_source_checks(audit_records, now)
    freshness_measured = [
        row for row in source_freshness
        if row["freshness_slo_days"] is not None and row["observation_age_days"] is not None
    ]
    freshness_passed = sum(bool(row["freshness_slo_met"]) for row in freshness_measured)
    freshness_pct = round(100 * freshness_passed / len(freshness_measured), 2) if freshness_measured else None
    scheduled_dag = get_latest_dag_run(scheduled_only=True)
    dag_duration = scheduled_dag.get("duration_seconds") if scheduled_dag else None
    dag_latency_met = bool(
        scheduled_dag
        and scheduled_dag.get("state") == "success"
        and dag_duration is not None
        and dag_duration <= DAG_LATENCY_SLO_SECONDS
    )
    latest_run_id = (latest_dag or {}).get("run_id")
    latest_run_task_map = {
        row["task_id"]: row
        for row in airflow_reliability.get("latest_run_tasks", [])
        if row.get("run_id") == latest_run_id and row.get("task_id")
    }
    dbt_target_dir = Path(os.getenv("DBT_TARGET_DIR", "/opt/airflow/dbt/target"))
    gold_model_metrics = read_dbt_gold_model_metrics(
        dbt_target_dir, latest_run_task_map.get("dbt_gold")
    )
    pipeline_stage_metrics = build_pipeline_stage_metrics(
        now=now,
        dag_run=latest_dag,
        airflow_reliability=airflow_reliability,
        audit_records=audit_records,
        silver_stage_runs=silver_stage_metrics.get("latest_runs", []),
        kafka_bronze_latency=latencies,
        kafka_publish_record=next(
            (
                record for record in reversed(read_audit(args.kafka_publish_audit_file))
                if record.get("dag_run_id") == (latest_dag or {}).get("run_id")
            ),
            {},
        ),
        bronze_batch_metrics=bronze_batch_metrics,
        bronze_events_24h=event_count,
        bronze_quarantine=bronze_quarantine_metrics,
        kafka_lag=kafka_bronze_lag,
        gold_cpi=gold_quality,
        gold_features=gold_feature_coverage,
        gold_model_metrics=gold_model_metrics,
        nowcast=nowcast_artifact,
        nowcast_backtest=nowcast_backtest,
        ablation=ablation_manifest,
    )
    latency_measured = [
        row for row in latencies if row.get("p95_kafka_to_bronze_seconds") is not None
    ]
    latency_passed = sum(
        float(row["p95_kafka_to_bronze_seconds"]) <= KAFKA_BRONZE_P95_SLO_SECONDS
        for row in latency_measured
    )
    latency_pct = round(100 * latency_passed / len(latency_measured), 2) if latency_measured else None
    base_integrity_checks = [
        "non_empty_table",
        "no_null_or_nan_values",
        "no_missing_country",
        "no_missing_observation_date",
        "no_duplicate_business_keys",
    ]
    integrity_passed = 0
    integrity_measured = 0
    integrity_by_domain = []
    for row in silver_quality:
        checks = [
            int(row.get("row_count") or 0) > 0,
            int(row.get("null_or_nan_values") or 0) == 0,
            int(row.get("missing_country") or 0) == 0,
            int(row.get("missing_observation_date") or 0) == 0,
        ]
        domain_check_names = list(base_integrity_checks)
        if row["domain"] in POSITIVE_VALUE_DOMAINS:
            checks.append(int(row.get("nonpositive_values") or 0) == 0)
        checks.append(int(row.get("duplicate_business_keys") or 0) == 0)
        if row["domain"] in POSITIVE_VALUE_DOMAINS:
            domain_check_names.insert(-1, "no_nonpositive_values")
        checks = [
            bool(check) for check in checks
        ]
        domain_passed = sum(checks)
        domain_measured = len(checks)
        integrity_passed += domain_passed
        integrity_measured += domain_measured
        integrity_by_domain.append(
            {
                "domain": row["domain"],
                "passed_checks": domain_passed,
                "measured_checks": domain_measured,
                "checks": domain_check_names,
                "achievement_percent": round(100 * domain_passed / domain_measured, 2),
            }
        )
    integrity_pct = (
        round(100 * integrity_passed / integrity_measured, 2)
        if integrity_measured
        else None
    )
    silver_by_domain = {row["domain"]: row for row in silver_quality}
    rejected_by_domain = {row["domain"]: row for row in rejected_quality}
    rejection_metrics = []
    for domain in sorted(set(silver_by_domain) | set(rejected_by_domain)):
        rejected = rejected_by_domain.get(domain, {})
        rejected_count = int(rejected.get("rejected_events") or 0)
        invalid_count = int(rejected.get("missing_or_non_numeric_events") or 0)
        policy_excluded = int(rejected.get("policy_excluded_events") or 0)
        rejection_metrics.append(
            {
                "domain": domain,
                "rejected_events": rejected.get("rejected_events"),
                "invalid_or_non_numeric_events": rejected.get("missing_or_non_numeric_events"),
                "policy_excluded_events": rejected.get("policy_excluded_events"),
                "rejection_table_available": rejected.get("rejection_table_available", False),
                "rejection_table": rejected.get("rejection_table"),
                "invalid_share_of_rejects_percent": round(100 * invalid_count / rejected_count, 2)
                if rejected_count
                else None,
            }
        )
    rejection_totals = {
        row["domain"]: int(row.get("rejected_events") or 0)
        for row in rejected_quality
    }
    rejection_reason_metrics = [
        {
            **row,
            "share_of_domain_rejects_percent": round(
                100 * int(row["rejected_events"]) / rejection_totals[row["domain"]], 2
            )
            if rejection_totals.get(row["domain"])
            else None,
        }
        for row in rejection_reason_counts
    ]

    report = {
        "generated_at": isoformat_now(now),
        "metric_slo_attainment": {
            "freshness": {
                "passed_sources": freshness_passed,
                "measured_sources": len(freshness_measured),
                "achievement_percent": freshness_pct,
                "target_days_by_frequency": FRESHNESS_SLO_DAYS,
            },
            "dag_latency": {
                "met": dag_latency_met,
                "passed_runs": int(dag_latency_met) if scheduled_dag else 0,
                "measured_runs": 1 if scheduled_dag else 0,
                "achievement_percent": 100.0 if dag_latency_met else 0.0 if scheduled_dag else None,
                "run_id": scheduled_dag.get("run_id") if scheduled_dag else None,
                "target_seconds": DAG_LATENCY_SLO_SECONDS,
                "duration_seconds": dag_duration,
            },
            "kafka_bronze_latency": {
                "passed_topics": latency_passed,
                "measured_topics": len(latency_measured),
                "achievement_percent": latency_pct,
                "target_seconds": KAFKA_BRONZE_P95_SLO_SECONDS,
            },
            "silver_integrity": {
                "passed_checks": integrity_passed,
                "measured_checks": integrity_measured,
                "achievement_percent": integrity_pct,
                "checks_per_domain": {
                    item["domain"]: item["checks"] for item in integrity_by_domain
                },
            },
            "kafka_consumer_lag": kafka_bronze_lag,
        },
        "data_freshness": {
            "measurement_scope": "latest_stored_observation_age_and_collector_check_status",
            "publication_to_ingestion_latency_measured": False,
            "publication_latency_limitation": (
                "Requires trustworthy source release timestamps or a maintained release calendar."
            ),
            "sources": source_freshness,
        },
        "pipeline_latency": {
            "latest_dag_run": latest_dag,
            "latest_dag_duration_seconds": latest_dag.get("duration_seconds") if latest_dag else None,
            "kafka_to_bronze_by_topic": latencies,
            "airflow_task_durations_30d": airflow_reliability.get("tasks_30d", []),
            "silver_stage_runs": silver_stage_metrics,
            "stage_metrics": pipeline_stage_metrics,
        },
        "throughput": {
            "window": "last_24_hours",
            "persisted_events_24h": event_count,
            "latest_burst": latest_burst,
            "peak_burst": peak_burst,
            "burst_gap_threshold_seconds": 60,
            "latest_collector_run_changed_records": changed_records,
            "latest_collector_run_records_per_second": collector_rate,
            "by_topic": latencies,
        },
        "data_quality": {
            "silver_by_domain": silver_quality,
            "integrity_by_domain": integrity_by_domain,
            "rejected_by_domain": rejected_quality,
            "rejection_metrics": rejection_metrics,
            "rejection_reason_metrics": rejection_reason_metrics,
            "unavailable_rejection_tables": [
                row["rejection_table"]
                for row in rejected_quality
                if not row.get("rejection_table_available", False)
            ],
            "bronze_envelope_quarantine": bronze_quarantine_metrics,
            "gold_cpi": gold_quality,
        },
        "trino_topology": trino_topology,
        "airflow_reliability": airflow_reliability,
        "gold_feature_coverage": gold_feature_coverage,
        "nowcast_backtest": nowcast_backtest,
        "sourcewise_ablation_proxy": proxy_sourcewise_ablation,
        "recommended_additional_metrics": [
            "Source release-calendar/SLO checks: freshness now includes age since the latest observation period end and expected frequency, but publication lag needs release timestamps.",
            "Separate source-scope filtering from data-quality failures; a large out-of-scope share can be expected when collectors keep broad raw history.",
        "Run-scoped Silver row counts are recorded by stage audit; dbt model-level row deltas and source publication timestamps remain useful additions.",
            "Source publication timestamp or release calendar; current IMF CPI events have no release_ts, so publication delay cannot be calculated yet.",
            "Prediction interval coverage and calibration against subsequently published CPI values.",
        ],
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "pipeline_metrics_latest.json"
    markdown_path = args.output_dir / "pipeline_metrics_latest.md"
    stage_csv_path = args.output_dir / "pipeline_stage_metrics_latest.csv"
    task_csv_path = args.output_dir / "pipeline_stage_tasks_latest.csv"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )
    markdown_path.write_text(markdown_report(report), encoding="utf-8")
    stage_columns = [
        "run_id", "stage", "status", "task_count", "succeeded_tasks", "failed_tasks",
        "not_started_tasks", "retried_tasks", "task_success_percent", "stage_wall_seconds",
        "task_duration_sum_seconds", "records_in", "records_out", "rejected_records",
        "acceptance_rate_percent", "throughput_records_per_second", "p95_latency_seconds",
        "p95_latency_sample_count", "p95_latency_basis", "metric_scope", "metric_notes", "details",
    ]
    with stage_csv_path.open("w", encoding="utf-8-sig", newline="") as stage_file:
        writer = csv.DictWriter(stage_file, fieldnames=stage_columns, extrasaction="ignore")
        writer.writeheader()
        for row in pipeline_stage_metrics.get("stages", []):
            writer.writerow({
                **row,
                "metric_notes": json.dumps(row.get("metric_notes", {}), ensure_ascii=False),
                "details": json.dumps(
                    row.get("details", {}), ensure_ascii=False, default=_json_default
                ),
            })
    task_columns = [
        "run_id", "stage", "task_id", "state", "start_date", "end_date",
        "duration_seconds", "try_number",
    ]
    with task_csv_path.open("w", encoding="utf-8-sig", newline="") as task_file:
        writer = csv.DictWriter(task_file, fieldnames=task_columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(pipeline_stage_metrics.get("tasks", []))
    if push_stage_metrics is not None:
        for stage_row in pipeline_stage_metrics.get("stages", []):
            push_stage_metrics(
                component="pipeline_summary",
                stage=stage_row["stage"],
                source="all",
                status=stage_row["status"],
                values={
                    key: stage_row.get(key)
                    for key in (
                        "task_success_percent",
                        "stage_wall_seconds",
                        "task_duration_sum_seconds",
                        "records_in",
                        "records_out",
                        "rejected_records",
                        "acceptance_rate_percent",
                        "throughput_records_per_second",
                        "p95_latency_seconds",
                        "p95_latency_sample_count",
                    )
                },
            )
    print(f"JSON report: {json_path}")
    print(f"Markdown report: {markdown_path}")
    print(f"Stage metrics CSV: {stage_csv_path}")
    print(f"Task metrics CSV: {task_csv_path}")
    print(markdown_report(report))


def isoformat_now(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


if __name__ == "__main__":
    main()
