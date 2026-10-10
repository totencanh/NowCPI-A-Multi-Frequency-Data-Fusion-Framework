"""Normalize market and fuel Bronze events into Silver."""

from spark.silver.common import MARKET_TOPICS, run_domain


if __name__ == "__main__":
    run_domain(MARKET_TOPICS, "NowCPI-Silver-Market")
