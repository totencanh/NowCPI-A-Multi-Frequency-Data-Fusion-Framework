# NowCPI Airflow

The DAG `nowcpi_daily_ingestion` runs every day at 08:00 Vietnam time:

1. Run the CPI, CPI components, PPI/IIP, Brent, USD/VND, and Vietnam fuel collectors.
2. Publish new JSON batches to their `nowcpi.*` Kafka topics.

The `spark-bronze-streaming` Compose service runs continuously, consumes those
topics, and upserts source events into `delta.bronze.kafka_events`. Airflow does
not start or stop this Spark service. The old `nowcpi_dbt_pipeline` DAG remains a
separate manual workflow; its dbt models are still scaffolds. Silver is not yet
part of the pipeline, and the news collector is currently empty.

The image includes the source collector and Kafka publisher dependencies. Start
the stack from this project root with:

```powershell
docker compose --profile airflow up -d --build
```

Open `http://localhost:8085`. The local development login defaults to `admin` / 
`admin`; set `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` before startup
to override it. The Airflow services use a separate PostgreSQL volume and connect
to the existing Trino service over the Compose network.
