"""Small, failure-tolerant Pushgateway helper for short-lived NowCPI stages."""

from __future__ import annotations

import logging
import math
import os
import time
from typing import Any

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

logger = logging.getLogger(__name__)


def push_stage_metrics(
    *,
    component: str,
    stage: str,
    values: dict[str, Any],
    source: str = "all",
    status: str | None = None,
) -> None:
    """Push latest stage gauges; durable run history remains in JSONL/Delta audit."""
    endpoint = os.getenv("PROMETHEUS_PUSHGATEWAY_URL", "").strip()
    if not endpoint:
        return

    registry = CollectorRegistry()
    metric = Gauge(
        "nowcpi_stage_metric",
        "Latest measured value for a NowCPI pipeline stage.",
        ["metric"],
        registry=registry,
    )
    last_updated = Gauge(
        "nowcpi_stage_last_updated_timestamp_seconds",
        "Unix timestamp of the latest stage metric update.",
        registry=registry,
    )
    stage_status = Gauge(
        "nowcpi_stage_last_status",
        "Latest stage outcome: 1=success, 0=failed, -1=partial/running, -2=not started.",
        registry=registry,
    )

    for name, value in values.items():
        if isinstance(value, bool):
            numeric = float(value)
        elif isinstance(value, (int, float)):
            numeric = float(value)
        else:
            continue
        if math.isfinite(numeric):
            metric.labels(metric=name).set(numeric)

    last_updated.set(time.time())
    if status is not None:
        status_value = {
            "success": 1,
            "failed": 0,
            "running": -1,
            "partial": -1,
            "not_started": -2,
        }.get(status, -1)
        stage_status.set(status_value)

    grouping_key = {"component": component, "stage": stage, "source": source}
    try:
        push_to_gateway(
            endpoint,
            job="nowcpi",
            grouping_key=grouping_key,
            registry=registry,
            timeout=2,
        )
    except Exception as exc:  # Monitoring outages must not fail data processing.
        logger.warning("Could not publish NowCPI metrics to Pushgateway: %s", exc)
