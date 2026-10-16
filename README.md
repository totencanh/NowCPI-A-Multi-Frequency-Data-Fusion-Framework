# NowCPI

**NowCPI: A Streaming Lakehouse for Asynchronous Multi-Frequency CPI Nowcasting in Vietnam: A U-MIDAS Application with Weekly Brent and Monthly CPI**

NowCPI is a research prototype for daily CPI nowcasting in Vietnam. Its streaming lakehouse ingests asynchronous source observations and retains their observation and availability times. The current forecast model predicts **monthly headline CPI MoM** using a Ridge-regularized U-MIDAS model with 12 weekly Brent log returns. CPI is the monthly target; Brent is the high-frequency predictor block.

USD/VND, domestic fuel prices, CPI components, and annual industrial activity are retained for dashboard analysis and can be evaluated as future model inputs. They are not currently predictors in the MIDAS nowcast. News collection and NLP are not implemented. Because source collection and forecasting are scheduled daily, the project demonstrates a daily nowcasting workflow over a streaming lakehouse, not continuous live forecasts.

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
       ▼                 ▼                  ▼
   Power BI         SQL analytics      U-MIDAS Ridge
   dashboards       with Trino         CPI MoM nowcast

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
├── forecasting/                # MIDAS nowcast, benchmarks, and outputs
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
6. The current MIDAS model reads CPI labels and Brent history from Gold through Trino and writes its nowcast and evaluation metrics under `forecasting/outputs/`.
7. Airflow schedules daily collection, processing, Gold transformations, and nowcasting. Power BI can query Gold through Trino; Power BI is not a Compose service. News NLP remains outside the implemented pipeline.

## Main technologies

- **Kafka** for asynchronous event transport
- **Apache Spark** for batch and streaming data processing
- **Delta Lake and MinIO** for versioned lakehouse storage
- **Hive Metastore and Trino** for cataloging and SQL access
- **dbt** for SQL transformations and feature marts
- **Airflow** for workflow orchestration
- **U-MIDAS Ridge** for CPI MoM nowcasting with weekly Brent predictors
- **Power BI-ready Gold datasets** for monthly CPI/market summaries and daily market trends

