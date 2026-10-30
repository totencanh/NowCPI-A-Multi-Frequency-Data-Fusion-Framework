# Isolated streaming benchmark

This benchmark sends synthetic timestamped events through an isolated Kafka →
Bronze → Silver → Gold path. It estimates processing latency for the benchmark
workload; it does not measure how old CPI/market observations are, whether a
source published new data, or the delay from official publication to NowCPI.

`evaluate_benchmark.py` reports:

- `kafka_to_bronze_latency_sec`: synthetic event emission to Bronze write return.
- `synthetic_end_to_end_latency_sec`: synthetic event emission to Gold write
  return. Gold readiness is not independently checked through Trino.
- P50/P95/P99 and a synthetic 60-minute latency threshold percentage.

These numbers are useful for controlled pipeline latency comparisons, not as
production source-freshness SLOs. For source observation age, use the pipeline
metrics report and its latest stored observation period. Publication delay
requires a reliable source release timestamp or a maintained publication
calendar; current IMF CPI records do not consistently provide one.

The benchmark uses a simplified transformation and dedicated topic, paths, and
checkpoint. Do not compare its latency directly with production daily-DAG
duration or describe its synthetic SLA percentage as real-source freshness.
