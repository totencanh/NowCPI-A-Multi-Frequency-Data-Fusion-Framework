# NowCPI pipeline: JSON → Kafka → Spark Bronze

## Scheduled streaming flow

```text
Airflow collectors (daily 08:00 Vietnam time)
    → append immutable event batches to data/raw
    → publisher sends new batches to nowcpi.* Kafka topics
    → spark-bronze-streaming consumes every 10 seconds
    → Delta table bronze.kafka_events on MinIO
```

The publisher preserves each event envelope and uses its stable `event_id` as
the Kafka key. It tracks batch hashes in `workspace/kafka-publisher/`, so an
unchanged batch is not republished on the next daily run. Spark uses a durable
checkpoint in `workspace/checkpoints/bronze-kafka-events/`; its Delta merge is
safe to replay. Each Bronze row keeps the event JSON, common event fields, and
Kafka topic/partition/offset metadata.

The older `processing/spark/bronze/cpi.py` and `market.py` jobs still read JSON
directly for manual development/backfills; Airflow no longer invokes them.
Silver remains a later step, and news is omitted because its collector is empty.

## Start services

Run from this project's directory:

```powershell
# Only do this if .env does not exist yet; edit the placeholder secrets first.
Copy-Item .env.example .env

docker compose up -d --build
docker compose ps
```

Wait until Kafka, MinIO, Hive Metastore, Spark master/worker, and
`spark-bronze-streaming` are running. Compose creates the six source topics
before the streaming consumer starts. Airflow must also be started with its
profile enabled for daily source collection and publication.

## Start Airflow scheduling

```powershell
docker compose --profile airflow up -d --build airflow-init airflow-webserver airflow-scheduler
```

The DAG collects sources at 08:00 Vietnam time, then publishes new JSON batches
to Kafka. The streaming Spark service consumes continuously once the Compose
stack is up. Use Airflow's **Trigger DAG** action to run collection now instead
of waiting for the schedule.

## Check the table

Open Trino at `http://localhost:8080`, or run:

```powershell
docker compose exec trino trino --execute "SELECT kafka_topic, observation_period, series_id, value FROM delta.bronze.kafka_events ORDER BY bronze_ingested_at DESC LIMIT 20"
```

Check `docker compose logs -f spark-bronze-streaming` for consumer progress and
`docker compose logs airflow-scheduler` for scheduled collector/publisher tasks.

## Next stages

`processing/spark/silver/` and the dbt SQL models are still empty scaffolds.
Implement Silver validation next, then staging/intermediate/mart models.

The dbt project and Trino profile are configured. Run `dbt run` only after the
SQL model files are implemented; the current empty SQL placeholders are not
runnable models.
