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
