# NowCPI Airflow

The DAG `nowcpi_daily_pipeline` runs every day at 08:00 Vietnam time:

1. Run the CPI, CPI components, IIP, Brent, USD/VND, and Vietnam fuel collectors.
2. Publish new JSON batches to their `nowcpi.*` Kafka topics.
3. Wait until the continuously running Spark Bronze consumer has reached Kafka's
   current end offsets, then run the CPI/IIP and market Silver jobs in sequence.
4. Run dbt staging views, remove the retired Gold tables, then build the
   subject-specific dashboard tables in Trino.

The `spark-bronze-streaming` Compose service runs continuously, consumes those
topics, and records source events by Kafka offset in `delta.bronze.kafka_events`. Airflow does
not start or stop this Spark service. The daily DAG waits for the Kafka end
offsets to appear in Bronze before it normalizes CPI, CPI components, IIP,
Brent, USD/VND, and Vietnam fuel into compact Silver Delta tables. The news
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

`nowcpi_full_history_rebuild` is a separate, manual backfill DAG. It reads the
existing Bronze lake, forces a complete Silver rebuild, rebuilds Gold, and
refreshes the current MIDAS nowcast. It does not call source collectors or
publish to Kafka. **It does not run automatically when the daily pipeline fails.**
It has no schedule and must be triggered deliberately in Airflow. A normal daily
task failure follows that task's retry policy (two retries, five minutes apart);
it does not start a full-history rebuild. Use the rebuild DAG only for an
intentional historical replay or a Silver schema/transform migration. It reads
all available Bronze history, so it can take substantially longer than the
incremental daily run. If the daily Silver task detects a required migration,
it fails with an instruction to run this DAG rather than silently doing the
expensive rebuild inside the daily schedule. After the rebuild succeeds, the
daily DAG can resume incremental processing.

The most recent verified run was `manual__2026-10-17T08:47:02+00:00`:
collectors, Kafka publication, Bronze catch-up, both Silver jobs, dbt staging,
dbt Gold, and MIDAS all succeeded. The forecast output and its interpretation
are recorded in `forecasting/README.md`.

Bronze receipts are immutable and keyed by Kafka topic, partition, and offset.
Malformed JSON or events without `event_id` are retained in
`bronze.kafka_events_quarantine` with the original event text, hash, offset,
and rejection reason. The Kafka copy of source events includes `event_time`, a
stable `business_key`, and `payload_hash`; raw JSON batch files are unchanged.

Generate a current metrics report from the audit file and Delta tables with:

```powershell
docker compose --profile airflow exec -T airflow-scheduler python /opt/airflow/scripts/evaluate_pipeline_metrics.py
```

The report is written to `workspace/pipeline-audit/reports/` in JSON and
Markdown formats. It measures source-check freshness, Kafka-to-Bronze latency,
recent persisted-event throughput, Silver quality/rejection reasons, Kafka
consumer lag and expired-offset gaps, Airflow success/retry/duration percentiles
over 7/30-day windows, and Gold feature age/coverage. Silver writes
`silver.stage_run_audit` rows with DAG/task IDs, input/accepted/rejected counts,
and stage start/commit timestamps. Publication lag still cannot be measured
when a source does not provide a reliable release timestamp.

After changing the Spark or Airflow image, rebuild the Airflow services and
restart the Bronze consumer so both pick up the Silver runtime and Bronze
schema update:

```powershell
docker compose --profile airflow up -d --build airflow-scheduler airflow-webserver
docker compose restart spark-bronze-streaming
```

The daily DAG runs Bronze catch-up and incremental Silver after publishing.
For a failed task, inspect its Airflow log and retry/clear that task as
appropriate; do not treat `nowcpi_full_history_rebuild` as automatic failure
recovery. Trigger it only for an intentional full-history rebuild or migration.
Trigger `nowcpi_dbt_pipeline` for a standalone dbt rebuild.

The image includes the source collector and Kafka publisher dependencies. Start
the stack from this project root with:

```powershell
docker compose --profile airflow up -d --build
```

Open `http://localhost:8085`. The local development login defaults to `admin` / 
`admin`; set `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` before startup
to override it. The Airflow services use a separate PostgreSQL volume and connect
to the existing Trino service over the Compose network.
