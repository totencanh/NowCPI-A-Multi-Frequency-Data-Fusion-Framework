# NowCPI

**NowCPI: A Multi-Frequency Data Fusion Framework for Real-Time Inflation Nowcasting in Vietnam via Streaming Lakehouse Architecture under Asynchronous Multi-Source Data**

NowCPI is a research prototype for daily CPI nowcasting in Vietnam. Its streaming lakehouse ingests asynchronous source observations and retains their observation and availability times. The primary forecast model is **Full multi-source Ridge**, using CPI lags, weekly Brent returns, USD/VND, domestic fuel, NSO IIP, and source-missingness indicators when eligible at the point-in-time cutoff. AR-MIDAS is retained as a comparison model. The target month is one month after the latest actual CPI month, not automatically the computer's current month.

Macro sources are used by the primary multi-source model when their point-in-time features are eligible; CPI components are retained for analysis. Historical source vintages are incomplete, so historical ablation does not yet prove the incremental value of every source. News collection and NLP are not implemented. Because source collection and forecasting are scheduled daily, the project demonstrates a daily nowcasting workflow over a streaming lakehouse, not continuous live forecasts.

## Architecture

```text
IMF CPI / CPI components / Brent / USD-VND / fuel / annual activity
                         │
              Source-specific ingestion
                         │
                    JSON events
                         ▼
                       Kafka
                         ▼
             Spark Structured Streaming
                         ▼
                 Bronze (Delta Lake)
                         ▼
        Silver: validation and harmonization
                         ▼
                dbt staging → Gold
                         ▼
       ┌─────────────────┼──────────────────┐
   ▼                 ▼                          ▼
   Power BI         SQL analytics       Full multi-source Ridge
   dashboards       with Trino          CPI MoM nowcast
                                         AR-MIDAS comparison

  MinIO: lakehouse object storage
  Hive Metastore: table catalog
  Airflow: workflow orchestration
```

Sources arrive at different frequencies and times. The current nowcast freezes an information cutoff at run start and filters CPI and Brent independently by their recorded availability timestamps. Historical evaluation is expanding-window pseudo-out-of-sample; the lakehouse does not preserve historical source-release vintages, so it is not a vintage-accurate real-time backtest.

## Project structure

```text
NowCPI/
├── docs/                       # Architecture, data, pipeline, and seminar scope
├── ingestion/                  # Collectors for CPI, components, oil, FX, fuel, IIP
├── data/raw/                   # Raw JSON source data
├── processing/spark/
│   ├── bronze/                 # Ingest source records into the lakehouse
│   ├── silver/                 # Validate, normalize, and harmonize source data
│   └── common/                 # Shared schemas and processing utilities
├── infra/
│   ├── airflow/                # Workflow DAGs and Airflow image configuration
│   ├── dbt/models/
│   │   ├── staging/            # Source-aligned models
│   │   └── gold/               # Analytics, Power BI, and nowcasting tables
│   ├── kafka/                  # Message broker configuration
│   ├── spark/                  # Spark image and runtime configuration
│   ├── hive-metastore/         # Lakehouse catalog configuration
│   ├── trino/                  # SQL query engine configuration
│   ├── superset/               # Inactive visualization scaffold
│   └── jupyter-lab/            # Exploration environment configuration
├── forecasting/                # Multi-source primary nowcast, comparison models, and outputs
├── analytics/powerbi/          # Power BI connection and dashboard guide
├── workspace/                  # Local working and runtime files
├── docker-compose.yaml         # Local service orchestration
└── Makefile                    # Common project commands
```

## Data flow

1. Collectors retrieve CPI, CPI components, Brent, USD/VND, domestic fuel, and annual industrial activity from their respective sources. News is not currently collected.
2. Ingestion serializes source observations as JSON and publishes events to Kafka while retaining source metadata.
3. Spark Structured Streaming consumes asynchronous events and stores the source-level records in the Bronze Delta layer on MinIO.
4. Spark Silver processing validates schemas, handles timestamps and duplicate observations, and harmonizes units and frequencies while retaining source lineage.
5. dbt builds staging views and Gold subject, reporting, and model tables through Trino.
6. The primary full multi-source Ridge model reads point-in-time features from Gold through Trino; AR-MIDAS and other candidates are evaluated as comparison models. Forecasts and evaluation metrics are written under `forecasting/outputs/`.
7. Airflow schedules daily collection, processing, Gold transformations, and nowcasting. Power BI can query Gold through Trino; Power BI is not a Compose service. News NLP remains outside the implemented pipeline.

The forecast target is always the month immediately after the latest actual CPI month. Historical source-release vintages are not fully archived, so historical metrics remain diagnostics rather than strict real-time evidence. See `forecasting/README.md` for model roles and output details. The separate
`nowcpi_full_history_rebuild` DAG is manually triggered for intentional
historical rebuilds or Silver migrations; it does not automatically run after
a daily pipeline failure.

## Main technologies

- **Kafka** for asynchronous event transport
- **Apache Spark** for batch and streaming data processing
- **Delta Lake and MinIO** for versioned lakehouse storage
- **Hive Metastore and Trino** for cataloging and SQL access
- **dbt** for SQL transformations and feature marts
- **Airflow** for workflow orchestration
- **Full multi-source Ridge** for CPI MoM nowcasting; AR-MIDAS is a comparison model
- **Power BI-ready Gold datasets** for monthly CPI/market summaries and daily market trends

