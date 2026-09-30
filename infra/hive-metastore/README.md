# NowCPI Hive Metastore

Hive Metastore stores the catalog metadata used by Spark and Trino. The
`hive-metastore` Compose service uses MariaDB for its backend and MinIO for the
warehouse location. Credentials are supplied through the root `.env` file.

The service exposes its Thrift endpoint on port `9083`. Spark registers the
Bronze CPI Delta table in the `bronze` database so that Trino can query it via
the `delta` catalog.
