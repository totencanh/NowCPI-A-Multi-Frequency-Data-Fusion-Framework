# NowCPI Airflow

The DAG `nowcpi_daily_pipeline` runs every day at 08:00 Vietnam time:

1. Run the CPI, CPI components, IIP, Brent, USD/VND, and Vietnam fuel collectors.
2. Publish new JSON batches to their `nowcpi.*` Kafka topics.
3. Wait until the continuously running Spark Bronze consumer has reached Kafka's
   current end offsets, then run the CPI/IIP and market Silver jobs in sequence.
4. Run dbt staging views, remove the retired Gold tables, then build the
   subject-specific dashboard tables in Trino.

The `spark-bronze-streaming` Compose service runs continuously, consumes those
topics, and upserts source events into `delta.bronze.kafka_events`. Airflow does
not start or stop this Spark service. The daily DAG waits for the Kafka end
offsets to appear in Bronze before it normalizes CPI, CPI components, IIP,
Brent, USD/VND, and Vietnam fuel into compact Silver Delta tables. You
can also trigger `nowcpi_silver_pipeline` manually for backfills. The news
collector is currently empty. The daily DAG runs the dbt staging and Gold tasks
after Silver. The `nowcpi_dbt_pipeline` DAG is also available for a standalone
manual rebuild of those dbt models.

Each source collector is wrapped by `infra/airflow/scripts/source_check_audit.py`.
Every attempt appends one JSON line to
`workspace/pipeline-audit/source_checks.jsonl`, including successful checks
with no changed records and failed checks. Records include check start/end and
duration, status, newly written batch/record counts, the number of unique
records already stored, and the latest observation period and release timestamp
in the raw archive.
`latest_stored_observation_period` describes the data vintage; it is not the
time the source was last checked. Source checks without a changed batch still
appear in the audit file.

After changing the Spark or Airflow image, rebuild the Airflow services and
restart the Bronze consumer so both pick up the Silver runtime and Bronze
schema update:

```powershell
docker compose --profile airflow up -d --build airflow-scheduler airflow-webserver
docker compose restart spark-bronze-streaming
```

The daily DAG now runs Bronze catch-up and Silver after publishing. In the
Airflow UI, trigger `nowcpi_silver_pipeline` separately only when you need a
manual Silver backfill.

The image includes the source collector and Kafka publisher dependencies. Start
the stack from this project root with:

```powershell
docker compose --profile airflow up -d --build
```

Open `http://localhost:8085`. The local development login defaults to `admin` / 
`admin`; set `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` before startup
to override it. The Airflow services use a separate PostgreSQL volume and connect
to the existing Trino service over the Compose network.
