# NowCPI Airflow

The DAG `nowcpi_daily_pipeline` runs every day at 08:00 Vietnam time:

1. Run the CPI, CPI components, IIP, Brent, USD/VND, and Vietnam fuel collectors.
2. Publish new JSON batches to their `nowcpi.*` Kafka topics.
3. Query Kafka's current end offsets and poll Bronze through Trino until those
   offsets are persisted, then run the CPI/IIP and market Silver jobs in sequence.
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

The Bronze catch-up barrier uses Kafka metadata plus a Trino aggregation over
`delta.bronze.kafka_events`; it does not submit a Spark job or occupy the Spark
worker pool. This avoids an unbounded wait inside Spark when no worker has
registered or accepted the offset-check job. It reports the lagging topic
partitions and has a wall-clock timeout. The separate `wait_for_bronze.py` Spark
implementation is retained only for reference and is no longer called by the
daily or full-history DAGs.

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

If Yahoo Finance rate-limits the USD/VND collector, the audit wrapper runs the
Frankfurter v2 fallback for the recent gap and records the recovered warning
and provider. Frankfurter supplies a blended reference rate; it is not an SBV
or commercial-bank quote. It does not provide a release timestamp in this
collector, so fallback events use ingestion time for availability and do not
resolve historical point-in-time vintage coverage.

`nowcpi_full_history_rebuild` is a separate, manual backfill DAG. It reads the
existing Bronze lake, forces a complete Silver rebuild, rebuilds Gold, and
refreshes the current full multi-source nowcast. It does not call source collectors or
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
dbt Gold, and the nowcast task all succeeded. The forecast output and its
interpretation are recorded in `forecasting/README.md`.

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
Markdown formats. The daily DAG also runs `pipeline_stage_metrics` after the
nowcast and ablation branches finish (including when either branch fails), so
the reports are refreshed automatically at the end of each run. In addition to
the JSON/Markdown report, `pipeline_stage_metrics_latest.csv` summarizes each
stage and `pipeline_stage_tasks_latest.csv` lists task status, retries, and
duration for the measured run. `pipeline_stage_events_latest.csv` contains
run-scoped dbt model and forecasting events: status, runtime, available output
and fold counts, nowcast value, feature coverage, and prediction interval.
The same events are appended to `workspace/pipeline-audit/stage_metrics.jsonl`;
the report filters them by the latest Airflow DAG run ID. Stages are ingestion,
Kafka, Bronze, Silver, Gold/dbt, and forecasting. Run-specific collector and Silver counts are joined
by Airflow DAG run ID; Kafka/Bronze 24-hour observations and current Gold row
counts are labeled with their measurement scope instead of being presented as
run-level deltas. Gold model outputs are counted per successful dbt model and
joined to that model's dbt execution time; the aggregate materialization rate
sums rows across Gold tables, so overlapping rows are possible. The append-only
feature-vintage model counts only rows recorded during that Gold task. Gold
input rows, record rejections, and acceptance rate remain N/A because Gold has
multiple overlapping inputs and dbt tests validate constraints rather than
rejecting records. A `null` metric means it was unavailable, not zero.

The Kafka publisher writes one `kafka_publish_runs.jsonl` audit row per DAG run
with candidate/delivered event counts, send-to-ack latency samples, acceptance,
and throughput. The Bronze stream writes idempotent per-microbatch counts and
processing durations to `bronze.kafka_batch_audit`; these Bronze figures use a
rolling 24-hour scope because the consumer is a continuous service rather than
a task started by each DAG run. Restart `spark-bronze-streaming` after updating
its code; the new Bronze metrics become available after it processes a
non-empty Kafka microbatch. P95 rows identify their basis and sample count;
task-duration P95 for Gold and forecasting is descriptive with only a few tasks,
not a long-run latency SLO. `N/A` in Markdown means undefined or uninstrumented.

The daily DAG records Gold model results immediately after `dbt_gold` and model
runtime/fold eligibility immediately after nowcast and ablation. Ingestion,
Kafka, Bronze, and Silver keep their existing stage-local audit files/tables;
the final metrics task combines these stage measurements with Airflow task
timings. No Airflow callback is required for these artifact measurements.
There is no need to delete existing raw data or historical audit rows before a
new run: the append-only stage ledger keeps prior run IDs separate, while the
`*_latest` reports are refreshed for the newest run.

The shared v1 event contract is enforced before Kafka publish and again in
Bronze. Invalid source rows are logged by reference and reason while their raw
JSON remains untouched; valid rows from a partially invalid batch continue
through Kafka. Bronze/Silver measure event time from source release time when
available, otherwise from collector ingestion time, and separately retain the
economic observation period. A 24-hour late-arrival threshold flags and counts
late records without dropping them; this is deliberate for macroeconomic
revisions and historical backfills. Source-to-Kafka lag from collector time is
a proxy when a source has no reliable release timestamp, and the report shows
release-timestamp coverage so that proxy is not mistaken for publication lag.

For live monitoring during execution, use the optional Prometheus/Grafana stack
documented in [`infra/monitoring/README.md`](../monitoring/README.md). Airflow
task metrics flow through StatsD; collector/Kafka/Silver summaries are pushed
after each short-lived stage; the continuous Bronze consumer exposes a scrape
endpoint and histogram. Run IDs remain in the durable audit files/tables rather
than Prometheus labels.

Its source freshness section measures the age of the latest
stored observation and the latest collector-check status; it does not claim
publication-to-ingestion lag without a reliable source release timestamp. It
also measures Kafka-to-Bronze latency,
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

After dbt Gold, the daily DAG also runs `dbt_pit_feature_tests` followed by
`feature_ablation_evaluation`. The PIT test enforces observation and source
availability cutoffs. The ablation task writes model comparisons, per-source
historical eligibility, runtime/RSS, failure counts, and error cases under
`forecasting/outputs/feature_ablation_*`. It runs independently from
`multisource_inflation_nowcast`; an ablation failure does not block the nowcast task.
