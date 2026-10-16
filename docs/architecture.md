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
- dbt builds Gold subject tables, monthly nowcasting datasets, and
  Power BI-oriented daily/monthly marts. The actual grains remain separate.
- Forecasting currently runs a Ridge U-MIDAS nowcast of monthly headline CPI
  MoM using 12 weekly Brent log returns. Optional CPI-only and AR-MIDAS variants
  are available when lagged CPI is released by the run cutoff.
- USD/VND, fuel, CPI components, and annual industrial activity are available
  for analysis and dashboarding, but are not current MIDAS predictors. News
  collection and NLP are not implemented.

Airflow schedules collection, transformation, and nowcasting daily at 08:00
Vietnam time. Spark Structured Streaming runs as a separate long-running
Compose service and continuously consumes the topics into Bronze. This is a
daily nowcasting workflow over a streaming architecture, not continuous
real-time forecast generation. The current model freezes a data cutoff at run
start and independently respects each source's recorded availability time.

Power BI can connect to Trino and query the dashboard-ready Gold models. It is
not currently deployed as a Compose service. Historical forecast metrics are
pseudo-out-of-sample because archived publication-time vintages are unavailable.
