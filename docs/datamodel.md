# NowCPI event and lakehouse model

## Ingestion event

Each collector wraps one source observation in a common event envelope:

| Field | Purpose |
|---|---|
| `schema_version` | Envelope version |
| `event_id` | Stable SHA-256 identity from source and source record ID |
| `source`, `series_id`, `country` | Source and economic series identifiers |
| `observation_period`, `frequency` | Period represented by the observation |
| `value`, `unit` | Measured value and unit |
| `release_ts`, `ingested_at` | Publication time when known and collection time |
| `source_record_id` | Source-level observation identity |
| `raw_payload` | Original source row/object retained for traceability |

## Medallion layers

- **Raw files:** The initial JSON files remain in `data/raw/`; each subsequent
  run writes a timestamped batch containing only new or revised events under
  `data/raw/<source-file-stem>/`.
- **Bronze:** The Kafka stream consumer stores the original `event_json`, parsed
  common envelope fields, `raw_payload_json`, Kafka topic/partition/offset, and
  `bronze_ingested_at` in `bronze.kafka_events`. Silver preserves source,
  event, Kafka offset, release, ingestion, and derived `available_at` metadata;
  invalid/out-of-scope records are written to matching `silver.*_rejected`
  Delta tables instead of being imputed. It merges observations by the stable
  series/country/date/unit business key.
  (falling back to Kafka position for malformed IDs) under
  `s3a://<MINIO_BUCKET>/bronze/kafka_events`.
- **Batch backfills:** `processing/spark/bronze/cpi.py` and `market.py` remain
  available to load JSON directly into source-specific Bronze Delta tables.
- **Silver:** Spark validates the event contract, normalizes observation dates,
  frequencies, values, units, and fuel product identity, and retains event,
  source, Kafka lineage, release time, ingestion time, and `available_at` next
  to the standardized measurement. Missing or invalid values are not imputed;
  invalid and out-of-scope events are persisted in topic-specific
  `silver.*_rejected` tables. Only observation dates from 2025-01-01 through
  the current Vietnam date are included in the curated observation tables.
  PPI is excluded; annual IIP growth remains. Silver does not pivot or
  aggregate indicators.
- **Gold:** dbt publishes subject-specific analytical tables, monthly CPI
  labels, and a separate nowcasting feature table. The feature table uses
  target-month market-to-date observations and applies an `available_at`
  cutoff to market data and lagged CPI/IIP inputs. CPI labels are kept out of
  model features. Annual IIP remains annual and is not interpolated into
  monthly data.

Do not combine observations at different frequencies before a temporal
alignment policy is defined. The current annual World Bank industrial value
added growth series is an activity proxy, not a monthly IIP series.
