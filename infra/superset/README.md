# NowCPI Superset

Superset configuration and image files are scaffolded here. Superset is not
currently included as a service in `docker-compose.yaml`; the active pipeline
ends at MinIO, Spark, Hive Metastore, and Trino. Add a dashboard service after
the CPI mart models and Trino datasets are available.
