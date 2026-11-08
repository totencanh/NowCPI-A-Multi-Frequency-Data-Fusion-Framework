# NowCPI monitoring

This optional local stack follows a pull-based Prometheus setup:

- Airflow emits task and DAG metrics over StatsD to `statsd-exporter`.
- Airflow task callbacks publish each task's running, success, retry, or failed
  state immediately to Pushgateway; task duration and attempt count are added
  when Airflow has measured them.
- Short-lived collector, Kafka publisher, Silver, and final stage-summary code
  pushes its latest bounded-cardinality gauges to Pushgateway. Durable per-run
  history remains in JSONL/Delta audit with `dag_run_id`.
- The long-running Spark Bronze consumer exposes `/metrics` directly for
  Prometheus scraping; its micro-batch audit remains in Delta.
- Prometheus retains 30 days of samples; Grafana provisions the NowCPI
  dashboard automatically.

Prometheus recommends Pushgateway for short-lived batch jobs, not as the scrape
mechanism for long-running services. That is why Bronze exposes a scrape
endpoint. See the [Pushgateway guidance](https://prometheus.io/docs/practices/pushing/)
and [Airflow 2.9 metrics configuration](https://airflow.apache.org/docs/apache-airflow/2.9.2/configurations-ref.html#metrics).

## Start the monitoring stack

From the repository root in PowerShell:

```powershell
$env:PROMETHEUS_PUSHGATEWAY_URL = "http://pushgateway:9091"
docker compose --profile airflow --profile monitoring up -d --build airflow-webserver airflow-scheduler spark-bronze-streaming statsd-exporter pushgateway prometheus grafana
```

`PROMETHEUS_PUSHGATEWAY_URL` lets short-lived tasks send their latest stage
metrics. For persistent configuration, add it to the project `.env` file.
Prometheus labels deliberately omit `dag_run_id` and batch IDs; these high-cardinality
identifiers stay in audit records. The dashboard shows the latest stage gauges
and live Bronze histogram metrics.

## Local endpoints

- Grafana: `http://localhost:3000` (default local credentials: `admin` / `admin`;
  override with `GRAFANA_ADMIN_USER` and `GRAFANA_ADMIN_PASSWORD` in `.env`)
- Prometheus: `http://localhost:9090`
- Pushgateway: `http://localhost:9091`
- Bronze metrics: `http://localhost:9404/metrics`

The Bronze exporter and Pushgateway hold current/process-level metrics. The
JSON, Markdown, CSV, and Delta audit outputs remain the source for run history
and run-level analysis.
