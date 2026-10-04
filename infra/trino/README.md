# Trino for NowCPI

Trino provides SQL access to Delta tables stored in MinIO. Its `delta` catalog
uses the Hive Metastore service for table metadata and the Delta Lake connector
for table transactions.

The service is defined in the root `docker-compose.yaml`; its configuration is
mounted from `conf/`. The web UI and SQL endpoint are available at
`http://localhost:8080`.

Tables become visible to Trino after Spark registers them in Hive Metastore.
The Kafka streaming Bronze consumer registers `delta.bronze.kafka_events` in
MinIO. The file-based CPI and market jobs remain available for development and
backfills. Silver, intermediate, and mart models are not implemented yet.
