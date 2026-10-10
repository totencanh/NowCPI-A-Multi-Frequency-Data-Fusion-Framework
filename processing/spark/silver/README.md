# NowCPI Silver

Silver reads `s3a://<MINIO_BUCKET>/bronze/kafka_events`, validates and
normalizes observations, and writes one compact Delta table per indicator
domain. It does not retain source names or event IDs or create quarantine tables; invalid
events are counted by rejection reason in the Spark logs.

## Tables

Each table contains only `series_id`, `country`, `observation_date`,
`frequency`, `value`, and `unit`. Bronze retains the event envelope, original
payload, source name, Kafka metadata, and ingestion timestamps for traceability.

- `cpi_observations`
- `cpi_component_observations`
- `iip_observations` (annual IIP growth only; PPI is excluded)
- `brent_oil_observations`
- `usd_vnd_observations`
- `vn_fuel_observations`

Only observations from `2025-01-01` through the current Vietnam date are
retained. Existing out-of-range rows are pruned on each run. Revisions are
merged using the natural key `(series_id, country, observation_date, unit)`.
Fuel history is split into RON 95 and E10 RON 95 based on the source product
title. Missing or non-numeric measurements are filled with the average of
existing valid observations for the same series, country, frequency, and unit.
If no matching observations exist, the row is skipped. Other invalid events are
summarized in logs rather than persisted to quarantine tables.

The Silver jobs bootstrap a table from full Bronze history when the table is
missing or has the old schema. Obsolete quarantine tables and the former
combined PPI/IIP table are removed after a successful domain run. Incremental
runs use the checkpoint at
`s3a://<MINIO_BUCKET>/silver/_checkpoints/kafka_offsets`; set
`SILVER_FULL_REFRESH=1` to rebuild from Bronze.

## Jobs

- `cpi.py`: CPI headline, CPI components, and IIP growth.
- `market.py`: Brent, USD/VND, and Vietnam fuel prices.
- `main.py`: run all configured topics or select topics for backfills.
- `news.py`: deferred; no news events currently reach Bronze.

Run with `docker compose exec spark-master ... /opt/spark/app/spark/silver/cpi.py`
or `market.py` after Bronze has received source events.
