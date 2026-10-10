"""Normalize CPI, CPI component, and IIP Bronze events into Silver."""

from spark.silver.common import CPI_TOPICS, run_domain


if __name__ == "__main__":
    run_domain(CPI_TOPICS, "NowCPI-Silver-CPI")
