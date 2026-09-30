# NowCPI pipeline: CPI Bronze

## Current flow

The current CPI job is a batch load from `data/raw/imf_cpi_vietnam.json`. It
does not consume Kafka yet. Spark preserves every input field and `raw_payload`,
adds Bronze metadata, and merges by stable `event_id` into one Delta table at
`s3a://lakehouse/bronze/cpi`. Spark registers `bronze.cpi` in Hive Metastore so
Trino can query it as `delta.bronze.cpi`.

## Start services

Run from the `NowCPI` directory:

```powershell
# Only do this if .env does not exist yet; edit the placeholder secrets first.
Copy-Item .env.example .env

docker compose up -d --build
docker compose ps
```

Wait until MariaDB, MinIO, Kafka, Hive Metastore, Spark, and Trino are running.
The first build downloads the pinned Spark connector JARs.

## Run CPI Bronze

```powershell
docker compose exec spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark/app/spark/bronze/cpi.py
```

The Spark image and local Python requirements both use Spark 3.5.0 and Delta
3.0.0. MinIO and MariaDB credentials are loaded from the local `.env` file;
`.env` and raw JSON files are excluded by `.gitignore`.

## Check the table

Open Trino at `http://localhost:8080`, or run:

```powershell
docker compose exec trino trino --execute "SELECT observation_period, value, unit FROM delta.bronze.cpi ORDER BY observation_period DESC LIMIT 10"
```

The Delta transaction log is at `lakehouse/bronze/cpi/_delta_log` in MinIO.
Repeated runs update matching `event_id` rows and insert new observations.

## Next stages

`processing/spark/silver/` and the dbt SQL models are still empty scaffolds.
Implement Silver validation first, then staging/intermediate/mart models. The
Kafka connector is installed, but collectors have not been wired to publish
events and Spark has not yet been configured as a streaming consumer.

The dbt project and Trino profile are configured. Run `dbt run` only after the
SQL model files are implemented; the current empty SQL placeholders are not
runnable models.
