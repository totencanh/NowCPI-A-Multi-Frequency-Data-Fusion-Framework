"""Run one source collector and append a durable source-check audit record."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SOURCE_FAILURE_PATTERNS = (
    re.compile(r"failed download", re.IGNORECASE),
    re.compile(r"yfratelimiterror", re.IGNORECASE),
    re.compile(r"too many requests", re.IGNORECASE),
)
FALLBACK_TRIGGER_PATTERNS = SOURCE_FAILURE_PATTERNS


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def source_files(raw_data_dir: Path, source: str) -> list[Path]:
    source_dir = raw_data_dir / source
    batch_files = sorted(source_dir.glob("*.json")) if source_dir.is_dir() else []
    legacy_file = raw_data_dir / f"{source}.json"
    if legacy_file.is_file():
        batch_files.insert(0, legacy_file)
    return [path for path in batch_files if path.is_file() and path.stat().st_size > 1]


def file_snapshot(paths: list[Path]) -> dict[Path, tuple[int, int]]:
    return {path: (path.stat().st_size, path.stat().st_mtime_ns) for path in paths}


def read_events(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [event for event in payload if isinstance(event, dict)]
    return []


def summarize_source(
    paths: list[Path],
) -> tuple[int, str | None, str | None]:
    """Return unique stored count and release metadata for the latest period."""
    events: dict[str, dict[str, Any]] = {}
    anonymous_index = 0
    for path in paths:
        for event in read_events(path):
            event_id = event.get("event_id")
            if event_id:
                key = str(event_id)
            else:
                anonymous_index += 1
                key = f"__anonymous_{anonymous_index}"
            events[key] = event

    latest_event = max(
        events.values(),
        key=lambda event: str(event.get("observation_period") or ""),
        default=None,
    )
    latest_period = (
        str(latest_event.get("observation_period"))
        if latest_event and latest_event.get("observation_period") is not None
        else None
    )
    release_ts = latest_event.get("release_ts") if latest_event else None
    return (
        len(events),
        latest_period,
        str(release_ts) if release_ts is not None else None,
    )


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    # Multiple Airflow collectors run in parallel; serialize appends to preserve
    # one complete JSON object per line in the shared audit file.
    with path.open("a", encoding="utf-8") as audit_file:
        fcntl.flock(audit_file.fileno(), fcntl.LOCK_EX)
        try:
            audit_file.write(line)
            audit_file.flush()
            os.fsync(audit_file.fileno())
        finally:
            fcntl.flock(audit_file.fileno(), fcntl.LOCK_UN)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, help="Raw-data source directory name")
    parser.add_argument("--raw-data-dir", type=Path, required=True)
    parser.add_argument("--audit-file", type=Path, required=True)
    parser.add_argument("--collector", type=Path, required=True)
    parser.add_argument(
        "--fallback-collector",
        type=Path,
        help="Optional collector to run after a recognized transient source failure.",
    )
    return parser.parse_args()


def run_collector(command: list[str]) -> tuple[int, str | None, list[str]]:
    """Stream a collector's output and return its status and recognized warnings."""
    warnings: list[str] = []
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for output_line in process.stdout:
            print(output_line, end="", flush=True)
            if any(pattern.search(output_line) for pattern in SOURCE_FAILURE_PATTERNS):
                warnings.append(output_line.strip())
        return process.wait(), None, warnings
    except OSError as exc:
        error = str(exc)
        print(f"Collector could not start: {error}", file=sys.stderr, flush=True)
        return 127, error, warnings


def main() -> int:
    args = parse_args()
    started_at = utc_now()
    started_clock = time.monotonic()
    before_paths = source_files(args.raw_data_dir, args.source)
    before_snapshot = file_snapshot(before_paths)

    command = [sys.executable, "-u", str(args.collector)]
    print(
        f"SOURCE_CHECK_START source={args.source} checked_at={isoformat(started_at)}",
        flush=True,
    )
    exit_code, error, warnings = run_collector(command)
    recovered_warnings: list[str] = []
    fallback_used = False
    fallback_source = None
    fallback_warnings: list[str] = []
    should_fallback = (
        args.source == "usd_vnd_daily"
        and args.fallback_collector is not None
        and any(
            pattern.search(line)
            for line in warnings
            for pattern in FALLBACK_TRIGGER_PATTERNS
        )
    )
    if should_fallback:
        recovered_warnings = warnings
        fallback_used = True
        fallback_source = "frankfurter_v2"
        print(
            "Primary USD/VND provider was rate-limited; trying Frankfurter v2 fallback.",
            flush=True,
        )
        exit_code, error, fallback_warnings = run_collector(
            [
                sys.executable,
                "-u",
                str(args.fallback_collector),
                "--raw-data-dir",
                str(args.raw_data_dir),
            ]
        )
        warnings = fallback_warnings

    completed_at = utc_now()
    collector_duration = round(time.monotonic() - started_clock, 3)
    after_paths = source_files(args.raw_data_dir, args.source)
    after_snapshot = file_snapshot(after_paths)
    changed_paths = [
        path
        for path, signature in after_snapshot.items()
        if before_snapshot.get(path) != signature
    ]

    changed_events: dict[str, dict[str, Any]] = {}
    for path in changed_paths:
        for index, event in enumerate(read_events(path)):
            key = str(event.get("event_id") or f"{path.name}:{index}")
            changed_events[key] = event

    (
        stored_event_count,
        latest_observation_period,
        latest_release_ts,
    ) = summarize_source(after_paths)
    record = {
        "source": args.source,
        "dag_id": os.getenv("AIRFLOW_CTX_DAG_ID"),
        "dag_run_id": os.getenv("AIRFLOW_CTX_DAG_RUN_ID"),
        "task_id": os.getenv("AIRFLOW_CTX_TASK_ID"),
        "check_started_at": isoformat(started_at),
        "check_completed_at": isoformat(completed_at),
        "check_duration_seconds": collector_duration,
        "status": (
            "failed" if exit_code != 0 else "degraded" if warnings else "success"
        ),
        "collector_exit_code": exit_code,
        "error": error,
        "warnings": warnings,
        "recovered_warnings": recovered_warnings,
        "fallback_used": fallback_used,
        "fallback_source": fallback_source,
        "batch_files_written": len(changed_paths),
        "changed_records_written": len(changed_events),
        "stored_unique_records": stored_event_count,
        "latest_stored_observation_period": latest_observation_period,
        "latest_stored_release_ts": latest_release_ts,
    }
    append_jsonl(args.audit_file, record)
    try:
        from spark.common.prometheus_metrics import push_stage_metrics

        push_stage_metrics(
            component="collector",
            stage="ingestion",
            source=args.source,
            status=record["status"],
            values={
                "check_duration_seconds": collector_duration,
                "changed_records_written": len(changed_events),
                "stored_unique_records": stored_event_count,
                "batch_files_written": len(changed_paths),
                "collector_success": exit_code == 0,
            },
        )
    except ImportError:
        pass
    print("SOURCE_CHECK_AUDIT " + json.dumps(record, ensure_ascii=False), flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
