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
  `bronze_ingested_at` in `bronze.kafka_events`. It merges by stable `event_id`
  (falling back to Kafka position for malformed IDs) under
  `s3a://<MINIO_BUCKET>/bronze/kafka_events`.
- **Batch backfills:** `processing/spark/bronze/cpi.py` and `market.py` remain
  available to load JSON directly into source-specific Bronze Delta tables.
- **Silver:** planned; will validate types, normalize period/frequency fields,
  and apply series-specific quality rules.
- **Gold:** planned through dbt; will expose aligned macro/market features and
  nowcasting input tables.

Do not combine observations at different frequencies before a temporal
alignment policy is defined. The current annual World Bank industrial value
added growth series is an activity proxy, not a monthly IIP series.
