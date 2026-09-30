# NowCPI architecture

## Current implementation

```text
Source collectors
  └─ JSON snapshots in data/raw
       └─ Spark batch: processing/spark/bronze/cpi.py
            └─ Delta Bronze table in MinIO: lakehouse/bronze/cpi
                 ├─ Hive Metastore: table metadata
                 └─ Trino: SQL access through the delta catalog
```

Docker Compose currently provides Kafka, MinIO, MariaDB, Hive Metastore,
Spark master/worker, and Trino. Spark's custom image includes the Kafka
connector, Delta Lake, and Hadoop S3A libraries.

## Planned stages

- Publish asynchronous source events to Kafka and consume them with Spark
  Structured Streaming. The current CPI Bronze job is still a JSON batch job.
- Implement Bronze processors for market and news data.
- Implement Silver normalization and validation.
- Add dbt SQL models for staging, intermediate features, and marts.
- Add forecasting/NLP jobs and a Superset service after the feature tables
  exist.

The architecture diagram in this document distinguishes running components
from planned work so the project is not presented as real-time before the
streaming source path is implemented.
