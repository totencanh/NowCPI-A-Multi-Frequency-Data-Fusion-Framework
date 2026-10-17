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
  compact Delta tables, and retains source/event IDs, release and ingestion
  timestamps, Kafka coordinates, business keys, and payload hashes for lineage.
  Valid historical observations are retained; malformed or future-dated events
  are stored in rejection tables with reasons. The current IIP topic contains
  annual industrial-activity observations; PPI is not part of the current flow.
- dbt builds Gold subject tables, monthly nowcasting datasets, and
  Power BI-oriented daily/monthly marts. The actual grains remain separate.
- Forecasting currently runs AR-MIDAS Ridge for monthly headline CPI MoM,
  combining the latest actual CPI lag available by the run cutoff with up to 12
  weekly Brent log returns. A CPI-only model is the fallback; the Brent-only
  model is diagnostic and is not reported as the main NowCPI forecast.
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

The latest verified full daily-pipeline run completed successfully on
2026-10-17. CPI was available through 2026-07, so the model's one-step target
was 2026-08 and its AR-MIDAS Ridge estimate was **0.181355% MoM**. This is a
delayed estimate for August, not a forecast for October; see
`forecasting/README.md` for the run cutoff and interpretation.
