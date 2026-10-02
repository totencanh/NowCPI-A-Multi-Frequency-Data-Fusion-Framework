# NowCPI pipeline: CPI Bronze

## Current flow

The current Bronze jobs are batch loads from `data/raw/`; they do not consume
Kafka yet. The CPI job processes headline CPI, CPI components, and PPI/IIP into
separate Delta tables. The market job processes Brent, USD/VND, and domestic
fuel into separate tables. Both jobs read the retained flat JSON baseline plus
timestamped batches, keep the newest version of each `event_id`, preserve
`raw_payload`, add Bronze metadata, and upsert into Delta on MinIO. News remains
unimplemented because its crawler and raw input are empty.

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

## Run CPI and market Bronze

```powershell
docker compose exec spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark/app/spark/bronze/cpi.py

docker compose exec spark-master /opt/bitnami/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  /opt/spark/app/spark/bronze/market.py
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
Tables are registered as `bronze.cpi`, `bronze.cpi_components`,
`bronze.ppi_iip`, `bronze.brent_oil`, `bronze.usd_vnd`, and `bronze.vn_fuel`.

## Next stages

`processing/spark/silver/` and the dbt SQL models are still empty scaffolds.
Implement Silver validation first, then staging/intermediate/mart models. The
Kafka connector is installed, but collectors have not been wired to publish
events and Spark has not yet been configured as a streaming consumer.

The dbt project and Trino profile are configured. Run `dbt run` only after the
SQL model files are implemented; the current empty SQL placeholders are not
runnable models.
