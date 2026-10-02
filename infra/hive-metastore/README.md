# NowCPI Hive Metastore

Hive Metastore 3.1.3 stores the catalog metadata used by Spark 3.5 and Trino.
The `hive-metastore` Compose service uses a dedicated MariaDB database and MinIO
for the warehouse location. Credentials are supplied through the root `.env`
file. A new metadata database is used to avoid downgrading the old Hive 4 schema.

The service exposes its Thrift endpoint on port `9083`. Spark registers the
Bronze CPI Delta table in the `bronze` database so that Trino can query it via
the `delta` catalog. The previous Hive 4-backed MariaDB volume is retained and
is not deleted by this configuration change.
