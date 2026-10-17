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
    "worldbank_iip_vietnam": "iip",
}
TOPIC_SOURCES = {
    "nowcpi.cpi": "cpi",
    "nowcpi.cpi_components": "cpi_components",
    "nowcpi.brent_oil": "brent",
    "nowcpi.usd_vnd": "usd_vnd",
    "nowcpi.vn_fuel": "vn_fuel",
    "nowcpi.ppi_iip": "iip",
}
SOURCE_FREQUENCIES = {
    "imf_cpi_vietnam": "monthly",
    "imf_cpi_components_vietnam": "monthly",
    "brent_oil_daily": "daily",
    "usd_vnd_daily": "daily",
    "vn_fuel_e10_ron95": "daily",
    "worldbank_iip_vietnam": "annual",
}
# Provisional, visible monitoring objectives. Adjust these to the seminar's
# agreed service levels; no throughput target is set without a workload target.
FRESHNESS_SLO_DAYS = {"daily": 3, "monthly": 75, "annual": 400}
DAG_LATENCY_SLO_SECONDS = 600
KAFKA_BRONZE_P95_SLO_SECONDS = 60
# CPI indices and market prices cannot be zero or negative. IIP growth is a
# growth rate and may legitimately be zero or negative during contraction.
POSITIVE_VALUE_DOMAINS = set(SILVER_TABLES) - {"iip"}


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
        completed = parse_time(record.get("check_completed_at"))
        observation_end = observation_period_end(record.get("latest_stored_observation_period"))
        result.append(
            {
                "source": source,
                "domain": AUDIT_SOURCES.get(source, source),
                "last_check_status": record.get("status"),
                "warnings": record.get("warnings", []),
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
                    "state": str(task.state).lower(),
                    "duration_seconds": float(task.duration)
                    if task.duration is not None
                    else None,
                    "try_number": int(task.try_number or 0),
                    "start_date": task.start_date,
                }
                for task in task_instances
            ]
    except Exception as exc:  # Airflow DB visibility may depend on runtime role.
        return {"available": False, "error": str(exc), "windows": {}, "tasks_30d": []}

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
    return {"available": True, "windows": windows, "tasks_30d": task_metrics}


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
    """Compare MIDAS with the CPI-only baseline on identical backtest months."""
    path = output_dir / "midas_backtest_predictions.csv"
    if not path.is_file():
        return {"available": False, "reason": f"Missing {path.name}"}
    rows: list[dict[str, float | str]] = []
    with path.open(encoding="utf-8", newline="") as source:
        for row in csv.DictReader(source):
            try:
                actual = float(row["actual_cpi_mom_percent"])
                baseline = float(row["cpi_only_ridge"])
                model = float(row["ar_midas_ridge"])
            except (KeyError, TypeError, ValueError):
                continue
            values = (actual, baseline, model)
            if all(value == value and abs(value) != float("inf") for value in values):
                rows.append(
                    {
                        "target_month": row.get("target_month", ""),
                        "actual": actual,
                        "baseline": baseline,
                        "model": model,
                    }
                )
    if not rows:
        return {"available": False, "reason": "No common finite baseline/model predictions"}
    actuals = [float(row["actual"]) for row in rows]
    baseline_predictions = [float(row["baseline"]) for row in rows]
    model_predictions = [float(row["model"]) for row in rows]

    def metrics(predictions: list[float]) -> dict[str, float]:
        errors = [actual - predicted for actual, predicted in zip(actuals, predictions)]
        return {
            "mae_pp": round(sum(abs(error) for error in errors) / len(errors), 6),
            "rmse_pp": round((sum(error * error for error in errors) / len(errors)) ** 0.5, 6),
            "directional_accuracy_pct": round(
                100 * sum(
                    (actual >= 0) == (predicted >= 0)
                    for actual, predicted in zip(actuals, predictions)
                ) / len(errors),
                2,
            ),
        }

    baseline_metrics = metrics(baseline_predictions)
    model_metrics = metrics(model_predictions)
    return {
        "available": True,
        "comparison_months": len(rows),
        "baseline": {"model": "cpi_only_ridge", **baseline_metrics},
        "model": {"model": "ar_midas_ridge", **model_metrics},
        "mae_improvement_percent": round(
            100 * (baseline_metrics["mae_pp"] - model_metrics["mae_pp"])
            / baseline_metrics["mae_pp"], 2
        ) if baseline_metrics["mae_pp"] else None,
        "rmse_improvement_percent": round(
            100 * (baseline_metrics["rmse_pp"] - model_metrics["rmse_pp"])
            / baseline_metrics["rmse_pp"], 2
        ) if baseline_metrics["rmse_pp"] else None,
        "directional_accuracy_delta_pp": round(
            model_metrics["directional_accuracy_pct"]
            - baseline_metrics["directional_accuracy_pct"], 2
        ),
        "evaluation_note": "Pseudo-out-of-sample using calendar-month-end information sets; not vintage-accurate.",
    }


def latest_collector_group(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group the most recent parallel collector checks for one DAG run."""
    if not records:
        return []
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


def build_sql() -> tuple[str, str, str, str]:
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
    rejected_sql = " UNION ALL ".join(rejected_selects)
    rejection_reasons_sql = " UNION ALL ".join(rejection_reason_selects)
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
        f"- Silver integrity checks: **{report['metric_slo_attainment']['silver_integrity']['passed_checks']}/{report['metric_slo_attainment']['silver_integrity']['measured_checks']} ({report['metric_slo_attainment']['silver_integrity']['achievement_percent']}%)**; checks are domain-aware. Non-positive values are disallowed for price/index domains, while negative IIP growth is valid.",
        "- Throughput: **not scored**; an expected workload/throughput target has not been agreed, so the report shows observed rates only.",
        "These are provisional targets for transparent tracking, not externally established guarantees.",
        "",
        "## Data Freshness",
        "",
        "Check age measures the most recent successful or failed source check. The observation period is the latest vintage stored in raw JSON, not the last check time.",
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
    lines += ["", "## Nowcast backtest versus CPI-only baseline", ""]
    if backtest.get("available"):
        lines += [
            f"Common evaluation months: **{backtest['comparison_months']}**.",
            f"AR-MIDAS MAE improvement: **{backtest['mae_improvement_percent']}%**; "
            f"RMSE improvement: **{backtest['rmse_improvement_percent']}%**; "
            f"directional-accuracy change: **{backtest['directional_accuracy_delta_pp']} percentage points**.",
            "| Model | MAE (pp) | RMSE (pp) | Direction accuracy |",
            "|---|---:|---:|---:|",
            f"| CPI-only ridge | {backtest['baseline']['mae_pp']} | {backtest['baseline']['rmse_pp']} | {backtest['baseline']['directional_accuracy_pct']}% |",
            f"| AR-MIDAS ridge | {backtest['model']['mae_pp']} | {backtest['model']['rmse_pp']} | {backtest['model']['directional_accuracy_pct']}% |",
            backtest["evaluation_note"],
        ]
    else:
        lines.append(f"Backtest comparison unavailable: {backtest.get('reason')}.")
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
            f"| {rejected.get('rejected_events', 0)} "
            f"| {rejected.get('missing_or_non_numeric_events', 0)} "
            f"| {rejected.get('policy_excluded_events', 0)} |"
        )
    lines += [
        "",
        "Non-positive values are checked for CPI, CPI components, Brent, FX, and fuel. IIP is a growth rate and may be negative; its sign check is not applicable (N/A). Reject counts are deduplicated by Kafka topic/partition/offset. The invalid share uses quarantined events as its denominator; accepted rows are deduplicated by business key, so they are not mixed into that percentage.",
        "",
        "| Domain | Rejected events | Missing/non-numeric | Invalid share of quarantined events | Future-date policy rejects |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in report["data_quality"]["rejection_metrics"]:
        invalid_share = row["invalid_share_of_rejects_percent"]
        lines.append(
            f"| {row['domain']} | {row['rejected_events']} "
            f"| {row['invalid_or_non_numeric_events']} "
            f"| {f'{invalid_share}%' if invalid_share is not None else 'N/A'} "
            f"| {row['policy_excluded_events']} |"
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
        f"Latest feature input ages: CPI {report['gold_feature_coverage'].get('latest_cpi_age_months')} month(s), Brent {report['gold_feature_coverage'].get('latest_brent_age_days')} day(s), USD/VND {report['gold_feature_coverage'].get('latest_fx_age_days')} day(s), fuel {report['gold_feature_coverage'].get('latest_fuel_age_days')} day(s), IIP {report['gold_feature_coverage'].get('latest_iip_age_years')} year(s).",
        "Coverage is the share of five inputs present: latest CPI before target month, Brent, USD/VND, fuel, and prior-year IIP. It measures availability, not predictive accuracy.",
    ]
    lines += ["", "## Metric limitations and recommended additions", ""]
    for item in report["recommended_additional_metrics"]:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-file", type=Path, default=Path("/opt/airflow/audit/source_checks.jsonl"))
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
    quality_sql, rejected_sql, rejection_reasons_sql, latency_sql = build_sql()
    try:
        cursor = conn.cursor()
        cursor.execute(quality_sql)
        silver_quality = rows_as_dicts(cursor)
        cursor.execute(rejected_sql)
        rejected_quality = rows_as_dicts(cursor)
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
            "iip_observation_age_years",
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
                max_by(iip_observation_year, target_month)
                    AS latest_iip_observation_year,
                max_by(iip_observation_age_years, target_month)
                    AS latest_iip_age_years,"""
        else:
            gold_feature_age_select = """
                CAST(NULL AS date) AS latest_brent_observation_date,
                CAST(NULL AS bigint) AS latest_brent_age_days,
                CAST(NULL AS date) AS latest_fx_observation_date,
                CAST(NULL AS bigint) AS latest_fx_age_days,
                CAST(NULL AS date) AS latest_fuel_observation_date,
                CAST(NULL AS bigint) AS latest_fuel_age_days,
                CAST(NULL AS integer) AS latest_iip_observation_year,
                CAST(NULL AS integer) AS latest_iip_age_years,"""
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
                  LIMIT 30"""
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
    collector_group = latest_collector_group(audit_records)
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
    latest_dag = get_latest_dag_run()
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
                "rejected_events": rejected_count,
                "invalid_or_non_numeric_events": invalid_count,
                "policy_excluded_events": policy_excluded,
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
        "data_freshness": {"sources": source_freshness},
        "pipeline_latency": {
            "latest_dag_run": latest_dag,
            "latest_dag_duration_seconds": latest_dag.get("duration_seconds") if latest_dag else None,
            "kafka_to_bronze_by_topic": latencies,
            "airflow_task_durations_30d": airflow_reliability.get("tasks_30d", []),
            "silver_stage_runs": silver_stage_metrics,
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
            "bronze_envelope_quarantine": bronze_quarantine_metrics,
            "gold_cpi": gold_quality,
        },
        "trino_topology": trino_topology,
        "airflow_reliability": airflow_reliability,
        "gold_feature_coverage": gold_feature_coverage,
        "nowcast_backtest": nowcast_backtest,
        "recommended_additional_metrics": [
            "Source release-calendar/SLO checks: freshness now includes age since the latest observation period end and expected frequency, but publication lag needs release timestamps.",
            "Separate source-scope filtering from data-quality failures; a large out-of-scope share can be expected when collectors keep broad raw history.",
            "Stage-level commit timestamps and run IDs on Silver/Gold tables for record-level lineage; current report measures task durations from Airflow metadata.",
            "Source publication timestamp or release calendar; current IMF CPI events have no release_ts, so publication delay cannot be calculated yet.",
            "Nowcast MAE/RMSE and prediction interval coverage against subsequently published CPI values.",
        ],
    }

    args.output_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.output_dir / "pipeline_metrics_latest.json"
    markdown_path = args.output_dir / "pipeline_metrics_latest.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(markdown_report(report), encoding="utf-8")
    print(f"JSON report: {json_path}")
    print(f"Markdown report: {markdown_path}")
    print(markdown_report(report))


def isoformat_now(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


if __name__ == "__main__":
    main()
