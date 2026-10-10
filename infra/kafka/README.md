# NowCPI Kafka

Kafka runs in KRaft mode. Host-side clients connect to `localhost:9094`;
Compose services use `kafka:9092`. Compose creates these source topics before
the Spark streaming consumer starts:

`nowcpi.cpi`, `nowcpi.cpi_components`, `nowcpi.ppi_iip`,
`nowcpi.brent_oil`, `nowcpi.usd_vnd`, and `nowcpi.vn_fuel`.

Airflow runs `producer.py` after the source collectors complete. The publisher
reads only the known JSON source batch directories, sends each event envelope
unchanged, and uses the existing `event_id` as its Kafka key. A persistent
batch-hash manifest prevents unchanged JSON files from being sent again.
Spark Structured Streaming consumes the topics and writes `bronze.kafka_events`
to Delta on MinIO. The Delta merge key makes replayed events idempotent.

## Recovery replay

The publisher normally skips batch files whose checksum is already in its
`published_batches.json` state file. If Kafka was reset or its messages were
lost while the state file remained, replay the affected source from the
Airflow container. For example, to replay CPI headline batches:

```powershell
docker compose exec airflow-scheduler sh -lc 'RAW_DATA_DIR=/opt/airflow/data/raw KAFKA_BROKER=kafka:9092 KAFKA_PUBLISHED_STATE=/opt/airflow/kafka-state/published_batches.json python -u /opt/airflow/kafka/producer.py --source imf_cpi_vietnam --force-replay'
```

Supported source names are `imf_cpi_vietnam`,
`imf_cpi_components_vietnam`, `worldbank_iip_vietnam`,
`brent_oil_daily`, `usd_vnd_daily`, and `vn_fuel_e10_ron95`. Use
`--force-replay` without `--source` to replay every source. Only use forced
replay to recover or backfill data: Kafka can contain repeated messages, while
the Bronze Delta merge deduplicates them by event ID. The normal publisher
state is updated only after Kafka acknowledges delivery.
