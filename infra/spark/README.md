# NowCPI Spark

This image follows the Spark setup in `Data-lakehouse`, using the same Spark
3.5.0 line with Delta Lake, Kafka Structured Streaming, and Hadoop S3A support
for MinIO. JARs are pinned in the Dockerfile; the Spark and Delta Python package
versions in the repository requirements are aligned with the image.

`spark-defaults.conf` configures Delta, MinIO, and the Hive Metastore. Compose
mounts raw input at `/opt/spark/data/raw` and project processing jobs at
`/opt/spark/app` in both the master and worker containers.

Build and start the processing services from `NowCPI` with:

```powershell
docker compose up -d --build minio minio-init mariadb hive-metastore kafka spark-master spark-worker trino
```
