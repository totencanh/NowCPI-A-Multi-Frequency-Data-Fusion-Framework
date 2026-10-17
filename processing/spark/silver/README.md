# NowCPI Silver

Silver reads `s3a://<MINIO_BUCKET>/bronze/kafka_events`, validates and
normalizes observations, and writes one compact Delta table per indicator
domain. It preserves event/source IDs, source release/event times, source and
Bronze ingestion times, Kafka coordinates, business key, and payload hash. Invalid
events are persisted in per-domain quarantine tables with Kafka coordinates and
rejection reasons.

## Tables

Each observation table retains normalized indicator values and the lineage
metadata above. Bronze retains the event envelope, original payload, Kafka
metadata, and ingestion timestamps for replay and audit.

- `cpi_observations`
- `cpi_component_observations`
- `iip_observations` (annual IIP growth only; PPI is excluded)
- `brent_oil_observations`
- `usd_vnd_observations`
- `vn_fuel_observations`

All valid historical observations are retained; only future-dated observations
and invalid records are rejected. Revisions are merged using the natural key
`(series_id, country, frequency, observation_date, unit)`. Fuel history is split into RON
95 and E10 RON 95 based on the source product title. Malformed metadata is
quarantined per Kafka offset before natural-key deduplication, so unrelated
records with missing keys are not collapsed into one rejection.

Each successful topic transformation appends a row to
`silver.stage_run_audit` with its Airflow run/task IDs, input/accepted/rejected
counts and stage start/commit timestamps. The audit write completes before the
offset checkpoint advances, so a failed audit does not silently skip a Bronze
batch.

The Silver jobs bootstrap a table from full Bronze history when the table is
missing or has the old schema. A transform-version checkpoint also triggers a
one-time full reconciliation after the historical-date policy changes, restoring
older records from Bronze and replacing stale reject records. Later runs use
the Kafka-offset checkpoint at
`s3a://<MINIO_BUCKET>/silver/_checkpoints/kafka_offsets`; set
`SILVER_FULL_REFRESH=1` to force another rebuild from Bronze.
Normal scheduled jobs set `SILVER_DEFER_MIGRATION_REBUILD=1`; when a schema or
transform-version migration requires a full rebuild, they fail clearly and
direct the operator to trigger `nowcpi_full_history_rebuild` explicitly.
That DAG is manual and unscheduled: it is **not** automatically invoked when
the daily pipeline fails. Ordinary task failures use Airflow retries and must
be investigated from that task's log. The full-history DAG reprocesses Bronze
history, then rebuilds Gold and refreshes the forecast; use it for an intentional
backfill or migration, not as the default retry button.

## Jobs

- `cpi.py`: CPI headline, CPI components, and IIP growth.
- `market.py`: Brent, USD/VND, and Vietnam fuel prices.
- `main.py`: run all configured topics or select topics for backfills.
- `news.py`: deferred; no news events currently reach Bronze.

Run with `docker compose exec spark-master ... /opt/spark/app/spark/silver/cpi.py`
or `market.py` after Bronze has received source events.
