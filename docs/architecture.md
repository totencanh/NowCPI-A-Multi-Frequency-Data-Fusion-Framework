# NowCPI architecture

## Implemented ingestion and Bronze flow

```text
Source collectors
  └─ Immutable JSON event batches in data/raw
       └─ Airflow daily publisher (new batches only)
            └─ Kafka topics: nowcpi.*
                 └─ Spark Structured Streaming
                      └─ Delta Bronze: bronze.kafka_events in MinIO
                           ├─ Hive Metastore: table metadata
                           └─ Trino: SQL access through the delta catalog
```

The Bronze streaming consumer records the original event JSON, parsed common
event fields, Kafka topic/partition/offset, and Bronze ingestion time. Stable
`event_id` keys make retries and publisher replays idempotent. The existing
`cpi.py` and `market.py` Spark jobs remain available for direct JSON backfills;
they are not part of the scheduled path.

## Silver and Gold

- Spark Silver normalizes and validates observations from Bronze, writes
  compact Delta tables with no persisted source/event IDs. Invalid events are
  logged and skipped; only dates from 2025 onward are retained. PPI is removed.
- dbt builds conformed Gold dimensions and facts, then publishes pivoted
  analytical marts for OLAP at documented monthly and annual grains.
- Add news collection and NLP, forecasting jobs, and a Superset service after
  the feature tables exist. The news collector is currently empty.

Airflow schedules source collection and Kafka publication daily at 08:00
Vietnam time. Spark Structured Streaming runs as a separate long-running
Compose service and continuously consumes the topics into Bronze.
