# NowCPI

**NowCPI: A Multi-Frequency Data Fusion Framework for Real-Time Inflation Nowcasting in Vietnam via Streaming Lakehouse Architecture under Asynchronous Multi-Source**

NowCPI is a real-time inflation nowcasting framework for Vietnam. It combines CPI and CPI component data with higher-frequency market indicators—such as oil prices and the USD/VND exchange rate—and economic signals extracted from news. Its streaming lakehouse architecture ingests asynchronously arriving data, preserves source records, and transforms them into features for analysis and inflation nowcasting.

## Architecture

```text
IMF / CPI components / IIP growth / Oil / USD-VND / News
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
          dbt staging → intermediate → marts
                         ▼
       ┌─────────────────┼──────────────────┐
       ▼                 ▼                  ▼
   Trino / SQL      NLP signals       Forecasting
   analytics        from news          CPI nowcasts

  MinIO: lakehouse object storage
  Hive Metastore: table catalog
  Airflow: workflow orchestration
```

Sources arrive at different frequencies and times. The pipeline retains each observation's source and time context, then aligns available signals into common feature tables for analysis and forecasting.

## Project structure

```text
NowCPI/
├── docs/                       # Architecture, data sources, data model, pipeline notes
├── ingestion/                  # Collectors for CPI, components, oil, FX, IIP, and news
├── data/raw/                   # Raw JSON source data
├── processing/spark/
│   ├── bronze/                 # Ingest source records into the lakehouse
│   ├── silver/                 # Validate, normalize, and harmonize source data
│   └── common/                 # Shared schemas and processing utilities
├── infra/
│   ├── airflow/                # Workflow DAGs and Airflow image configuration
│   ├── dbt/models/
│   │   ├── staging/            # Source-aligned models
│   │   ├── intermediate/       # CPI, macro, and market feature models
│   │   └── mart/               # Analytics and nowcasting datasets
│   ├── kafka/                  # Message broker configuration
│   ├── spark/                  # Spark image and runtime configuration
│   ├── hive-metastore/         # Lakehouse catalog configuration
│   ├── trino/                  # SQL query engine configuration
│   ├── schema-registry/        # Event schema management
│   ├── superset/               # Analytics and visualization configuration
│   └── jupyter-lab/            # Exploration environment configuration
├── nlp/
│   ├── preprocessing/          # News text cleaning and preparation
│   ├── sentiment/              # Sentiment analysis
│   ├── embeddings/             # Text representations
│   └── economic_signal/        # News-derived economic indicators
├── forecasting/
│   ├── features/               # Model feature preparation
│   ├── models/                 # Nowcasting models
│   ├── training/               # Model training workflows
│   └── inference/              # Forecast generation
├── analytics/                  # SQL analysis and notebooks
├── static/                     # Static project assets
├── workspace/                  # Local working and runtime files
├── docker-compose.yaml         # Local service orchestration
└── Makefile                    # Common project commands
```

## Data flow

1. Collectors retrieve CPI, CPI components, producer/industrial indicators, oil prices, exchange rates, and news from their respective sources.
2. Ingestion serializes source observations as JSON and publishes events to Kafka while retaining source metadata.
3. Spark Structured Streaming consumes asynchronous events and stores the source-level records in the Bronze Delta layer on MinIO.
4. Spark Silver processing validates schemas, handles timestamps and duplicate observations, and harmonizes units and frequencies while retaining source lineage.
5. dbt builds staging, intermediate, and mart tables through Trino for CPI analysis and model features.
6. NLP extracts sentiment and economic signals from news; forecasting combines those signals with CPI and market features to produce nowcasts.
7. Airflow orchestrates collection, processing, transformations, and model workflows. Trino and Superset support querying and visualization.

## Main technologies

- **Kafka** for asynchronous event transport
- **Apache Spark** for batch and streaming data processing
- **Delta Lake and MinIO** for versioned lakehouse storage
- **Hive Metastore and Trino** for cataloging and SQL access
- **dbt** for SQL transformations and feature marts
- **Airflow** for workflow orchestration
- **NLP and forecasting components** for news signals and inflation nowcasts

