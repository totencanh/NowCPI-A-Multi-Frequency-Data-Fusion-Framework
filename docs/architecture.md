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
- Forecasting uses Full multi-source Ridge as the primary model for monthly
  headline CPI MoM. Eligible features include CPI lags, weekly Brent returns,
  USD/VND, domestic fuel, monthly NSO IIP, and source-missingness indicators.
  AR-MIDAS and CPI-only remain comparison/fallback models.
- Historical source vintages remain incomplete; selecting the multi-source
  architecture does not prove that each additional source improves accuracy.
  CPI components remain available for analysis and dashboarding. News
  collection and NLP are not implemented.

Airflow schedules collection, transformation, and nowcasting daily at 08:00
Vietnam time. Spark Structured Streaming runs as a separate long-running
Compose service and continuously consumes the topics into Bronze. This is a
daily nowcasting workflow over a streaming architecture, not continuous
real-time forecast generation. The current model freezes a data cutoff at run
start and independently respects each source's recorded availability time.

Power BI Desktop can connect to Trino and query the dashboard-ready Gold
models using the community Trino Power BI custom connector (`.mez`). For local
Trino OSS, this is the documented project path; the generic Starburst ODBC route
may not be compatible. Power BI is not currently deployed as a Compose service.
See `analytics/powerbi/README.md` for installation, connection parameters, and
refresh instructions. Historical forecast metrics are pseudo-out-of-sample
because archived publication-time vintages are unavailable.

The latest verified full daily-pipeline run completed successfully on
2026-10-17. CPI was available through 2026-07, so the model's one-step target
was 2026-08. The current production policy uses Full multi-source Ridge when
eligible, with AR-MIDAS as a comparison/fallback; see
`forecasting/README.md` for the run cutoff and interpretation.
