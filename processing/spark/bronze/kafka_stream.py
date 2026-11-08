"""Consume NowCPI source events from Kafka and upsert raw events into Bronze."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from prometheus_client import Counter, Gauge, Histogram, start_http_server


TOPICS = [
    "nowcpi.cpi",
    "nowcpi.cpi_components",
    "nowcpi.ppi_iip",
    "nowcpi.brent_oil",
    "nowcpi.usd_vnd",
    "nowcpi.vn_fuel",
]

EVENT_ENVELOPE_SCHEMA = StructType(
    [StructField("event_id", StringType(), True)]
)
PROMETHEUS_METRICS_PORT = int(os.getenv("PROMETHEUS_METRICS_PORT", "9404"))
BRONZE_BATCH_RECORDS = Counter(
    "nowcpi_bronze_records",
    "Bronze micro-batch record counts by outcome.",
    ["outcome"],
)
BRONZE_BATCH_PROCESSING = Histogram(
    "nowcpi_bronze_batch_processing_seconds",
    "Bronze micro-batch processing duration in seconds.",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
)
BRONZE_LATEST_ACCEPTANCE = Gauge(
    "nowcpi_bronze_latest_batch_acceptance_ratio",
    "Valid envelopes divided by input records for the latest non-empty batch.",
)
BRONZE_LATEST_THROUGHPUT = Gauge(
    "nowcpi_bronze_latest_batch_throughput_records_per_second",
    "Deduplicated valid Bronze candidates divided by latest batch processing time.",
)
BRONZE_LAST_BATCH_TIMESTAMP = Gauge(
    "nowcpi_bronze_last_batch_completion_timestamp_seconds",
    "Unix timestamp when the last non-empty Bronze micro-batch completed.",
)
BRONZE_STREAM_UP = Gauge(
    "nowcpi_bronze_stream_up",
    "Whether the Bronze streaming driver initialized its metrics endpoint.",
)


def _record_bronze_metrics(
    input_events: int,
    valid_events: int,
    rejected_events: int,
    persisted_candidates: int,
    processing_duration_seconds: float,
) -> None:
    for outcome, count in (
        ("input", input_events),
        ("valid", valid_events),
        ("rejected", rejected_events),
        ("persisted_candidate", persisted_candidates),
    ):
        BRONZE_BATCH_RECORDS.labels(outcome=outcome).inc(count)
    BRONZE_BATCH_PROCESSING.observe(max(0.0, processing_duration_seconds))
    if input_events:
        BRONZE_LATEST_ACCEPTANCE.set(valid_events / input_events)
    if processing_duration_seconds > 0:
        BRONZE_LATEST_THROUGHPUT.set(persisted_candidates / processing_duration_seconds)
    BRONZE_LAST_BATCH_TIMESTAMP.set(time.time())


def _write_bronze_batch_audit(
    spark: SparkSession,
    bucket: str,
    batch_id: int,
    input_events: int,
    valid_events: int,
    rejected_events: int,
    persisted_candidates: int,
    batch_started_at: datetime,
    processing_duration_seconds: float,
) -> None:
    """Persist idempotent per-microbatch counts and processing latency."""
    audit_path = f"s3a://{bucket}/bronze/kafka_batch_audit"
    audit = spark.createDataFrame(
        [(
            "nowcpi.kafka_bronze",
            int(batch_id),
            int(input_events),
            int(valid_events),
            int(rejected_events),
            int(persisted_candidates),
            batch_started_at,
            round(float(processing_duration_seconds), 6),
        )],
        "stream_id STRING, batch_id LONG, input_events LONG, valid_events LONG, "
        "rejected_events LONG, persisted_candidates LONG, batch_started_at TIMESTAMP, "
        "processing_duration_seconds DOUBLE",
    ).withColumn("batch_committed_at", F.current_timestamp())

    delta_log = spark._jvm.org.apache.hadoop.fs.Path(
        f"{audit_path.rstrip('/')}/_delta_log"
    )
    filesystem = delta_log.getFileSystem(spark._jsc.hadoopConfiguration())
    if filesystem.exists(delta_log):
        audit.createOrReplaceGlobalTempView("nowcpi_bronze_batch_audit")
        try:
            spark.sql(
                f"""MERGE INTO delta.`{audit_path}` AS target
                    USING global_temp.nowcpi_bronze_batch_audit AS incoming
                    ON target.stream_id = incoming.stream_id
                       AND target.batch_id = incoming.batch_id
                    WHEN MATCHED THEN UPDATE SET
                      input_events = incoming.input_events,
                      valid_events = incoming.valid_events,
                      rejected_events = incoming.rejected_events,
                      persisted_candidates = incoming.persisted_candidates,
                      batch_started_at = incoming.batch_started_at,
                      batch_committed_at = incoming.batch_committed_at,
                      processing_duration_seconds = incoming.processing_duration_seconds
                    WHEN NOT MATCHED THEN INSERT *"""
            )
        finally:
            spark.catalog.dropGlobalTempView("nowcpi_bronze_batch_audit")
    else:
        audit.write.format("delta").mode("overwrite").save(audit_path)
    spark.sql(
        "CREATE TABLE IF NOT EXISTS bronze.kafka_batch_audit "
        f"USING DELTA LOCATION '{audit_path}'"
    )


def upsert_bronze_batch(spark: SparkSession, batch_df, batch_id: int, output_path: str) -> None:
    batch_started_clock = time.monotonic()
    batch_started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    input_count = batch_df.count()
    if not input_count:
        print(f"Kafka micro-batch {batch_id} is empty.", flush=True)
        return

    decoded = (
        batch_df.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("event_json"),
        )
        .withColumn("_envelope", F.from_json("event_json", EVENT_ENVELOPE_SCHEMA))
    )
    invalid = decoded.filter(
        F.col("_envelope").isNull()
        | F.col("_envelope.event_id").isNull()
        | (F.length(F.trim(F.col("_envelope.event_id"))) == 0)
    )
    invalid_count = invalid.count()
    if invalid_count:
        bucket = os.getenv("MINIO_BUCKET", "nowcpi")
        quarantine_path = f"s3a://{bucket}/bronze/kafka_events_quarantine"
        quarantine = invalid.select(
            F.concat_ws(
                ":",
                F.col("kafka_topic"),
                F.col("kafka_partition").cast("string"),
                F.col("kafka_offset").cast("string"),
            ).alias("kafka_record_key"),
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            F.col("event_json").alias("raw_event_json"),
            F.sha2(F.col("event_json"), 256).alias("payload_hash"),
            F.when(F.col("_envelope").isNull(), "invalid_json")
            .otherwise("missing_event_id")
            .alias("rejection_reason"),
            F.current_timestamp().alias("quarantined_at"),
        )
        quarantine_log = spark._jvm.org.apache.hadoop.fs.Path(
            f"{quarantine_path.rstrip('/')}/_delta_log"
        )
        filesystem = quarantine_log.getFileSystem(spark._jsc.hadoopConfiguration())
        if filesystem.exists(quarantine_log):
            quarantine.createOrReplaceGlobalTempView("nowcpi_bronze_quarantine_batch")
            try:
                spark.sql(
                    f"""MERGE INTO delta.`{quarantine_path}` AS target
                        USING global_temp.nowcpi_bronze_quarantine_batch AS incoming
                        ON target.kafka_record_key = incoming.kafka_record_key
                        WHEN NOT MATCHED THEN INSERT *"""
                )
            finally:
                spark.catalog.dropGlobalTempView("nowcpi_bronze_quarantine_batch")
        else:
            quarantine.write.format("delta").mode("overwrite").save(quarantine_path)
        spark.sql(
            "CREATE TABLE IF NOT EXISTS bronze.kafka_events_quarantine "
            f"USING DELTA LOCATION '{quarantine_path}'"
        )
        print(
            f"Persisted {invalid_count} malformed Kafka event(s) in "
            f"bronze.kafka_events_quarantine for micro-batch {batch_id}.",
            flush=True,
        )

    decoded = decoded.filter(
        F.col("_envelope").isNotNull()
        & F.col("_envelope.event_id").isNotNull()
        & (F.length(F.trim(F.col("_envelope.event_id"))) > 0)
    ).drop("_envelope")
    if not decoded.head(1):
        processing_duration = time.monotonic() - batch_started_clock
        _write_bronze_batch_audit(
            spark, os.getenv("MINIO_BUCKET", "nowcpi"), batch_id,
            input_count, 0, invalid_count, 0, batch_started_at,
            processing_duration,
        )
        _record_bronze_metrics(
            input_count, 0, invalid_count, 0, processing_duration
        )
        print(
            f"Kafka micro-batch {batch_id} contains no valid event envelopes.",
            flush=True,
        )
        return

    events = (
        decoded
        .withColumn("event_id", F.get_json_object("event_json", "$.event_id"))
        .withColumn(
            "schema_version",
            F.expr("try_cast(get_json_object(event_json, '$.schema_version') AS INT)"),
        )
        .withColumn("source", F.get_json_object("event_json", "$.source"))
        .withColumn("series_id", F.get_json_object("event_json", "$.series_id"))
        .withColumn("country", F.get_json_object("event_json", "$.country"))
        .withColumn(
            "observation_period",
            F.get_json_object("event_json", "$.observation_period"),
        )
        .withColumn("frequency", F.get_json_object("event_json", "$.frequency"))
        .withColumn("value", F.get_json_object("event_json", "$.value"))
        .withColumn("unit", F.get_json_object("event_json", "$.unit"))
        .withColumn("release_ts", F.get_json_object("event_json", "$.release_ts"))
        .withColumn("ingested_at", F.get_json_object("event_json", "$.ingested_at"))
        .withColumn(
            "source_record_id",
            F.get_json_object("event_json", "$.source_record_id"),
        )
        .withColumn("business_key", F.get_json_object("event_json", "$.business_key"))
        .withColumn("payload_hash", F.get_json_object("event_json", "$.payload_hash"))
        .withColumn(
            "event_time",
            F.try_to_timestamp(F.get_json_object("event_json", "$.event_time")),
        )
        .withColumn(
            "business_key",
            F.coalesce(
                F.col("business_key"),
                F.to_json(
                    F.struct(
                        "source",
                        "country",
                        "series_id",
                        "frequency",
                        "observation_period",
                        "unit",
                    )
                ),
            ),
        )
        .withColumn(
            "raw_payload_json",
            F.get_json_object("event_json", "$.raw_payload"),
        )
        .withColumn(
            # Bronze is an immutable Kafka receipt log: replayed logical events
            # retain separate offset-addressable records. Silver deduplicates
            # observations by business key.
            "bronze_key",
            F.concat_ws(
                ":",
                F.col("kafka_topic"),
                F.col("kafka_partition").cast("string"),
                F.col("kafka_offset").cast("string"),
            ),
        )
        .withColumn(
            "payload_hash",
            F.coalesce(F.col("payload_hash"), F.sha2(F.col("event_json"), 256)),
        )
        .withColumn("bronze_ingested_at", F.current_timestamp())
    )

    # A topic/partition/offset identifies one Kafka receipt deterministically.
    batch = events.dropDuplicates(["bronze_key"])
    delta_log = spark._jvm.org.apache.hadoop.fs.Path(
        f"{output_path.rstrip('/')}/_delta_log"
    )
    filesystem = delta_log.getFileSystem(spark._jsc.hadoopConfiguration())
    if filesystem.exists(delta_log):
        # Use a global temporary view because foreachBatch callbacks can resolve
        # SQL in a different session scope from a regular temporary view.
        view_name = "nowcpi_kafka_bronze_batch"
        batch.createOrReplaceGlobalTempView(view_name)
        try:
            spark.sql(
                f"""
                MERGE INTO delta.`{output_path}` AS target
                USING global_temp.{view_name} AS incoming
                ON target.bronze_key = incoming.bronze_key
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        finally:
            spark.catalog.dropGlobalTempView(view_name)
    else:
        batch.write.format("delta").mode("overwrite").save(output_path)

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS bronze.kafka_events "
        f"USING DELTA LOCATION '{output_path}'"
    )
    persisted_candidates = batch.count()
    processing_duration = time.monotonic() - batch_started_clock
    print(
        f"Upserted {persisted_candidates} event(s) from Kafka micro-batch {batch_id} "
        f"into bronze.kafka_events.",
        flush=True,
    )
    _write_bronze_batch_audit(
        spark,
        os.getenv("MINIO_BUCKET", "nowcpi"),
        batch_id,
        input_count,
        input_count - invalid_count,
        invalid_count,
        persisted_candidates,
        batch_started_at,
        processing_duration,
    )
    _record_bronze_metrics(
        input_count,
        input_count - invalid_count,
        invalid_count,
        persisted_candidates,
        processing_duration,
    )


def main() -> None:
    broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    bucket = os.getenv("MINIO_BUCKET", "nowcpi")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "nowcpi")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "nowcpi-local-password")
    output_path = f"s3a://{bucket}/bronze/kafka_events"
    checkpoint = os.getenv(
        "BRONZE_KAFKA_CHECKPOINT", "/opt/spark/work-dir/checkpoints/bronze-kafka-events"
    )

    try:
        start_http_server(PROMETHEUS_METRICS_PORT, addr="0.0.0.0")
        BRONZE_STREAM_UP.set(1)
        print(
            f"Prometheus metrics endpoint listening on :{PROMETHEUS_METRICS_PORT}/metrics",
            flush=True,
        )
    except OSError as exc:
        BRONZE_STREAM_UP.set(0)
        print(f"Prometheus metrics endpoint unavailable: {exc}", flush=True)

    spark = (
        SparkSession.builder.appName("NowCPI-Bronze-Kafka-Streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.driver.host", os.getenv("SPARK_DRIVER_HOST", "spark-bronze-streaming"))
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "7078")
        .config("spark.blockManager.port", "7079")
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze LOCATION 's3a://{bucket}/bronze'")
        kafka_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", broker)
            .option("subscribe", ",".join(TOPICS))
            .option("startingOffsets", "earliest")
            .load()
        )

        query = (
            kafka_stream.writeStream.foreachBatch(
                lambda batch_df, batch_id: upsert_bronze_batch(
                    spark, batch_df, batch_id, output_path
                )
            )
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime="10 seconds")
            .start()
        )
        print(
            f"Consuming {', '.join(TOPICS)} into {output_path}; "
            f"checkpoint={checkpoint}",
            flush=True,
        )
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
