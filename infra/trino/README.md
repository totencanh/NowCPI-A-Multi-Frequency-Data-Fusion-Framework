# Trino for NowCPI

Trino provides SQL access to Delta tables stored in MinIO. Its `delta` catalog
uses the Hive Metastore service for table metadata and the Delta Lake connector
for table transactions.

The service is defined in the root `docker-compose.yaml`; its configuration is
mounted from `conf/`. The web UI and SQL endpoint are available at
`http://localhost:8080`.

Tables become visible to Trino after Spark registers them in Hive Metastore.
The Kafka streaming Bronze consumer registers `delta.bronze.kafka_events` in
MinIO. Spark Silver jobs register source-oriented tables such as
`delta.silver.cpi_observations` and `delta.silver.vn_fuel_observations`.
Silver keeps a compact observation schema; use Bronze for raw payload and
Kafka-level lineage. dbt publishes separate dashboard tables in `delta.gold`:
`gold_cpi_monthly`, `gold_brent_daily`, `gold_usd_vnd_daily`,
`gold_fuel_daily`, and `gold_iip_annual`.
The file-based CPI and market jobs remain available for development and
backfills.
