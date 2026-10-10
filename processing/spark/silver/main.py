"""Run Silver normalization for one or more topics in the Bronze Delta table."""

from __future__ import annotations

import argparse

from spark.silver.common import TOPIC_TABLES, run_silver


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--topic",
        action="append",
        choices=sorted(TOPIC_TABLES),
        help=(
            "Kafka topic to reconcile. Repeat to select multiple topics. "
            "The default processes all configured topics."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    run_silver(parse_args().topic)
