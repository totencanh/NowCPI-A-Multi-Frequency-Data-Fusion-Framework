# NowCPI Spark

This image follows the Spark setup in `Data-lakehouse`, using the same Spark
3.5.0 line with Delta Lake, Kafka Structured Streaming, and Hadoop S3A support
for MinIO. JARs are pinned in the Dockerfile; the Spark and Delta Python package
versions in the repository requirements are aligned with the image.

`spark-defaults.conf` configures Delta, MinIO, and the Hive Metastore. Compose
runs a long-lived `spark-bronze-streaming` service that consumes the `nowcpi.*`
Kafka topics and writes `bronze.kafka_events`. Its durable Structured
Streaming checkpoint is stored under `workspace/checkpoints/` on the host.
The Spark master/worker also mount raw JSON and project processing jobs for
manual batch backfills.

Build and start the processing services from `NowCPI` with:

```powershell
docker compose up -d --build minio minio-init mariadb-hms hive-metastore kafka kafka-init spark-master spark-worker spark-bronze-streaming trino
```
