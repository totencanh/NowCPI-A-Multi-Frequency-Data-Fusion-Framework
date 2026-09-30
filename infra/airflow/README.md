# NowCPI Airflow

The DAG `nowcpi_dbt_pipeline` follows the project's dbt layers:

1. Install dbt packages.
2. Build staging models.
3. Build intermediate macro and market features.
4. Build CPI feature and nowcasting marts.

It is manually triggered (`schedule=None`) because CPI, market, and news sources
arrive at different frequencies and the ingestion cadence has not been finalized.
The DAG starts at dbt: source collection, Kafka consumption, and Spark Bronze/Silver
processing are not orchestrated by this DAG yet. The dbt SQL files are currently
scaffolds, so the transformation tasks become runnable after those models are filled
in and their source tables are available in Trino.

The image uses the Airflow/Cosmos and dbt version split from the reference
Data-lakehouse Airflow image, with NowCPI's own dbt project mounted into it. Start
the stack from the NowCPI project root with:

```powershell
docker compose --profile airflow up -d --build
```

Open `http://localhost:8085`. The local development login defaults to `admin` / 
`admin`; set `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` before startup
to override it. The Airflow services use a separate PostgreSQL volume and connect
to the existing Trino service over the Compose network.
