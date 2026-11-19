# NowCPI Silver

Silver reads `s3a://<MINIO_BUCKET>/bronze/kafka_events`, validates and
normalizes observations, and writes one compact Delta table per indicator
domain. Topic rules come from the shared versioned contract in
`processing/spark/common/schemas.py`; Silver also checks required metadata,
value validity, source/series/country/frequency/unit consistency, and future
observation dates. Invalid events are persisted in per-domain quarantine tables
with Kafka coordinates and rejection reasons.

## Tables

Each observation table retains normalized indicator values and the lineage
metadata above. Bronze retains the event envelope, original payload, Kafka
metadata, and ingestion timestamps for replay and audit.

- `cpi_observations`
- `cpi_component_observations`
- `iip_observations` (monthly NSO Vietnam aggregate IIP index; legacy annual World Bank growth events are rejected)
- `observation_versions` (append-only normalized revisions with first-seen availability and Kafka coordinates)
- `brent_oil_observations`
- `usd_vnd_observations`
- `vn_fuel_observations`

The observation period is the economic reference date. Event time is computed
from source release time, otherwise collector ingestion time, otherwise Kafka
receipt time; old Bronze rows are normalized with this same rule even if their
transport `event_time` had previously been set to the observation period.
`source_event_time_basis`, source-to-Kafka delay, and a late-arrival flag are
retained in Silver. Arrivals later than the configurable 24-hour default are
flagged, never dropped; no Spark watermark is applied because old economic
observations and revisions must remain recoverable.

All valid historical observations are retained; only future-dated observations
and invalid records are rejected. Revisions are merged using the natural key
`(series_id, country, frequency, observation_date, unit)`. Fuel history is split into RON
95 and E10 RON 95 based on the source product title. Malformed metadata is
quarantined per Kafka offset before natural-key deduplication, so unrelated
records with missing keys are not collapsed into one rejection.

Each successful topic transformation appends a row to
`silver.stage_run_audit` with its Airflow run/task IDs, input/accepted/rejected
counts, repeated event IDs removed, rows collapsed to one current value per
business key, metadata-valid counts, and stage start/commit timestamps. A row
collapsed by business key may be a legitimate source revision, not a corrupt
duplicate; `observation_versions` retains the version history. This separates
missing/invalid metadata from duplicates collapsed into the latest curated row.
The audit write completes before the offset checkpoint advances, so a failed
audit does not silently skip a Bronze batch.

Every valid new Bronze observation version is also merged into
`silver.observation_versions` using `(business_key, payload_hash)` as its version
identity. The table retains the first `available_at` for each content version;
source release time is preferred, then collector `source_ingested_at`, Kafka
receipt time, and Bronze ingestion time. The current curated tables still expose
the latest value by business key, while the version table preserves revisions
for point-in-time joins.

The Silver transform version is currently 7. The added event-time lineage
columns cause a one-time Silver reconciliation from Bronze history; the normal
daily DAG performs that migration automatically. The Silver jobs bootstrap a table from full Bronze history when the table is
missing or has the old schema. A transform-version checkpoint also triggers a
one-time full reconciliation after the historical-date policy changes, restoring
older records from Bronze and replacing stale reject records. Later runs use
the Kafka-offset checkpoint at
`s3a://<MINIO_BUCKET>/silver/_checkpoints/kafka_offsets`; set
`SILVER_FULL_REFRESH=1` to force another rebuild from Bronze.
The daily DAG lets Silver perform this migration rebuild automatically when it
detects a schema or transform-version change. This prevents the first scheduled
run after a code change from failing only to request a separate manual rebuild.
The manual `nowcpi_full_history_rebuild` DAG remains available for deliberate
backfills; ordinary execution errors still use Airflow retries and should be
diagnosed from the task log.

## Jobs

- `cpi.py`: CPI headline, CPI components, and IIP growth.
- `market.py`: Brent, USD/VND, and Vietnam fuel prices.
- `main.py`: run all configured topics or select topics for backfills.
- `news.py`: deferred; no news events currently reach Bronze.

Run with `docker compose exec spark-master ... /opt/spark/app/spark/silver/cpi.py`
or `market.py` after Bronze has received source events.
