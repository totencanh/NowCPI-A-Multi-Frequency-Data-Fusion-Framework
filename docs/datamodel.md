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

- **Raw files:** JSON snapshots produced by collectors in `data/raw/`.
- **Bronze:** Spark preserves every event field and `raw_payload`, adds
  `bronze_ingested_at`, `bronze_source_file`, and `bronze_batch_id`, then
  merges on `event_id` into one Delta table at `s3a://lakehouse/bronze/cpi`.
- **Silver:** planned; will validate types, normalize period/frequency fields,
  and apply series-specific quality rules.
- **Gold:** planned through dbt; will expose aligned macro/market features and
  nowcasting input tables.

Do not combine observations at different frequencies before a temporal
alignment policy is defined. The current annual World Bank industrial value
added growth series is an activity proxy, not a monthly IIP series.
