# NowCPI Kafka

Kafka runs in KRaft mode. Host-side clients connect to `localhost:9094`;
Compose services use `kafka:9092`. Compose creates these source topics before
the Spark streaming consumer starts:

`nowcpi.cpi`, `nowcpi.cpi_components`, `nowcpi.ppi_iip`,
`nowcpi.brent_oil`, `nowcpi.usd_vnd`, and `nowcpi.vn_fuel`.

Airflow runs `producer.py` after the source collectors complete. The v1 source
contract in `processing/spark/common/schemas.py` validates required fields,
schema version, source/topic/series/country/frequency/unit, observation period,
numeric value, and timestamps. Invalid rows are referenced in
`workspace/pipeline-audit/source_contract_quarantine.jsonl` by batch, row index,
event ID, hash, and reason; raw records stay in `data/raw` and valid rows from
the same batch are still published. The publisher uses stable `event_id` as its
Kafka key and a persistent batch-hash manifest to skip unchanged JSON files.
Spark Bronze repeats contract validation and stores bypassed invalid messages
in `bronze.kafka_events_quarantine`.

For transport event time, NowCPI uses source `release_ts` when present, then
collector `ingested_at`; Kafka time is a labeled fallback. `observation_period`
remains the economic reference date and is never substituted for arrival time.
Bronze computes source-to-Kafka delay and flags arrivals later than
`BRONZE_LATE_ARRIVAL_THRESHOLD_SECONDS` (default 86,400). Late events are kept
and measured, not discarded: CPI revisions and historical backfills are valid
late arrivals for this use case. Bronze writes the flags and delay with the
offset-keyed receipt in `bronze.kafka_events`; Delta `MERGE` makes receipt
replays idempotent.

## Recovery replay

The publisher normally skips batch files whose checksum is already in its
`published_batches.json` state file. If Kafka was reset or its messages were
lost while the state file remained, replay the affected source from the
Airflow container. For example, to replay CPI headline batches:

```powershell
docker compose exec airflow-scheduler sh -lc 'RAW_DATA_DIR=/opt/airflow/data/raw KAFKA_BROKER=kafka:9092 KAFKA_PUBLISHED_STATE=/opt/airflow/kafka-state/published_batches.json python -u /opt/airflow/kafka/producer.py --source imf_cpi_vietnam --force-replay'
```

Supported source names are `imf_cpi_vietnam`,
`imf_cpi_components_vietnam`, `nso_iip_vietnam`,
`brent_oil_daily`, `usd_vnd_daily`, and `vn_fuel_e10_ron95`. Use
`--force-replay` without `--source` to replay every source. Only use forced
replay to recover or backfill data: Kafka can contain repeated messages, while
the Bronze Delta merge deduplicates them by event ID. The normal publisher
state is updated only after Kafka acknowledges delivery.
