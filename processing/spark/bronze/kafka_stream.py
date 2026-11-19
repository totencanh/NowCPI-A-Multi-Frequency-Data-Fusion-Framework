"""Consume NowCPI source events from Kafka and upsert raw events into Bronze."""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from prometheus_client import Counter, Gauge, Histogram, start_http_server

_processing_root = Path(__file__).resolve().parents[2]
if str(_processing_root) not in sys.path:
    sys.path.insert(0, str(_processing_root))
from spark.common.schemas import SCHEMA_VERSION, TOPIC_CONTRACTS


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
BRONZE_LATEST_LATE_EVENTS = Gauge(
    "nowcpi_bronze_latest_late_events",
    "Late arrivals in the latest non-empty Bronze micro-batch; retained, never dropped.",
)
BRONZE_LATEST_SOURCE_TO_KAFKA_P95 = Gauge(
    "nowcpi_bronze_latest_p95_source_to_kafka_seconds",
    "P95 source-availability-to-Kafka delay for the latest Bronze micro-batch.",
)
BRONZE_STREAM_UP = Gauge(
    "nowcpi_bronze_stream_up",
    "Whether the Bronze streaming driver initialized its metrics endpoint.",
)


def _append_delta_batch_once(frame, path: str, table_name: str, batch_id: int) -> None:
    """Append one streaming batch once, including when Spark replays a batch.

    Bronze is an append-only Kafka receipt log. Delta's transaction app/version
    pair provides replay idempotence without building a costly MERGE plan for
    every micro-batch. The checkpoint generation is included in appId so a
    deliberate checkpoint migration can use a fresh transaction namespace.
    """
    checkpoint_generation = Path(
        os.getenv("BRONZE_KAFKA_CHECKPOINT", "bronze-kafka-events-v5")
    ).name
    app_id = f"nowcpi-bronze-{checkpoint_generation}-{table_name}"
    (
        frame.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", int(batch_id))
        .save(path)
    )


def _record_bronze_metrics(
    input_events: int,
    valid_events: int,
    rejected_events: int,
    persisted_candidates: int,
    processing_duration_seconds: float,
    late_events: int = 0,
    p95_source_to_kafka_seconds: float | None = None,
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
    BRONZE_LATEST_LATE_EVENTS.set(late_events)
    if p95_source_to_kafka_seconds is not None:
        BRONZE_LATEST_SOURCE_TO_KAFKA_P95.set(p95_source_to_kafka_seconds)


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
    late_events: int = 0,
    p95_source_to_kafka_seconds: float | None = None,
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
            int(late_events),
            p95_source_to_kafka_seconds,
            batch_started_at,
            round(float(processing_duration_seconds), 6),
        )],
        "stream_id STRING, batch_id LONG, input_events LONG, valid_events LONG, "
        "rejected_events LONG, persisted_candidates LONG, late_events LONG, "
        "p95_source_to_kafka_seconds DOUBLE, batch_started_at TIMESTAMP, "
        "processing_duration_seconds DOUBLE",
    ).withColumn("batch_committed_at", F.current_timestamp())

    _append_delta_batch_once(audit, audit_path, "batch-audit", batch_id)
    spark.sql(
        "CREATE TABLE IF NOT EXISTS bronze.kafka_batch_audit "
        f"USING DELTA LOCATION '{audit_path}'"
    )


def _with_contract_reasons(decoded):
    """Validate the full event contract after JSON decoding, by Kafka topic."""
    extracted = (
        decoded
        .withColumn("_contract_schema_version", F.expr("try_cast(get_json_object(event_json, '$.schema_version') AS INT)"))
        .withColumn("_contract_source", F.get_json_object("event_json", "$.source"))
        .withColumn("_contract_series", F.get_json_object("event_json", "$.series_id"))
        .withColumn("_contract_country", F.get_json_object("event_json", "$.country"))
        .withColumn("_contract_period", F.get_json_object("event_json", "$.observation_period"))
        .withColumn("_contract_frequency", F.get_json_object("event_json", "$.frequency"))
        .withColumn("_contract_value", F.expr("try_cast(get_json_object(event_json, '$.value') AS DOUBLE)"))
        .withColumn("_contract_unit", F.get_json_object("event_json", "$.unit"))
        .withColumn("_contract_ingested_at", F.get_json_object("event_json", "$.ingested_at"))
        .withColumn("_contract_release_ts", F.get_json_object("event_json", "$.release_ts"))
        .withColumn("_contract_source_record_id", F.get_json_object("event_json", "$.source_record_id"))
    )
    topic = F.col("kafka_topic")
    schema_version = F.col("_contract_schema_version")
    source = F.col("_contract_source")
    series = F.col("_contract_series")
    country = F.col("_contract_country")
    period = F.col("_contract_period")
    frequency = F.col("_contract_frequency")
    value = F.col("_contract_value")
    unit = F.col("_contract_unit")
    ingested_at = F.col("_contract_ingested_at")
    release_ts = F.col("_contract_release_ts")
    source_record_id = F.col("_contract_source_record_id")

    topic_issue = F.lit(None).cast("string")
    for topic_name, contract in TOPIC_CONTRACTS.items():
        if contract["period_frequency"] == "month":
            period_valid = period.rlike(r"^\d{4}-(0[1-9]|1[0-2])$")
        else:
            period_valid = (
                period.rlike(r"^\d{4}-\d{2}-\d{2}([T ].*)?$")
                & F.try_to_timestamp(period).isNotNull()
            )
        number_valid = (
            value.isNotNull()
            & ~F.isnan(value)
            & (F.abs(value) != F.lit(float("inf")))
        )
        checks = [
            F.when(~source.isin(*sorted(contract["sources"])), "source_not_allowed_for_topic"),
            F.when(~series.rlike(contract["series_pattern"]), "series_not_allowed_for_topic"),
            F.when(country != contract["country"], "country_not_allowed_for_topic"),
            F.when(frequency != contract["frequency"], "frequency_mismatch"),
            F.when(~unit.isin(*sorted(contract["units"])), "unit_mismatch"),
            F.when(~F.coalesce(period_valid, F.lit(False)), "invalid_observation_period"),
            F.when(~F.coalesce(number_valid, F.lit(False)), "value_must_be_finite_numeric"),
        ]
        if contract.get("positive_value"):
            checks.append(F.when(value <= 0, "value_must_be_positive"))
        topic_reason = F.concat_ws(";", *checks)
        topic_issue = F.when(
            topic == topic_name,
            F.when(F.length(topic_reason) > 0, topic_reason),
        ).otherwise(topic_issue)

    common_issues = [
        F.when(schema_version.isNull() | (schema_version != SCHEMA_VERSION), "unsupported_schema_version"),
        F.when(source.isNull() | (F.length(F.trim(source)) == 0), "missing_source"),
        F.when(series.isNull() | (F.length(F.trim(series)) == 0), "missing_series_id"),
        F.when(country.isNull() | (F.length(F.trim(country)) == 0), "missing_country"),
        F.when(frequency.isNull() | (F.length(F.trim(frequency)) == 0), "missing_frequency"),
        F.when(unit.isNull() | (F.length(F.trim(unit)) == 0), "missing_unit"),
        F.when(source_record_id.isNull() | (F.length(F.trim(source_record_id)) == 0), "missing_source_record_id"),
        F.when(F.try_to_timestamp(ingested_at).isNull(), "invalid_ingested_at"),
        F.when(release_ts.isNotNull() & F.try_to_timestamp(release_ts).isNull(), "invalid_release_ts"),
    ]
    common_reason = F.concat_ws(";", *common_issues)
    base_reason = F.when(F.length(common_reason) > 0, common_reason)
    unknown_topic = ~topic.isin(*sorted(TOPIC_CONTRACTS))
    contract_reason = F.when(
        F.col("_envelope").isNull(), F.lit("invalid_json")
    ).when(
        F.col("_envelope.event_id").isNull()
        | (F.length(F.trim(F.col("_envelope.event_id"))) == 0),
        F.lit("missing_event_id"),
    ).when(
        ~F.col("_envelope.event_id").rlike(r"^[a-f0-9]{64}$"),
        F.lit("event_id_must_be_sha256_hex"),
    ).otherwise(
        F.concat_ws(
            ";",
            base_reason,
            F.when(unknown_topic, "unknown_topic"),
            F.when(F.length(topic_issue) > 0, topic_issue),
        )
    )
    return extracted.withColumn("_contract_rejection_reason", contract_reason)


def upsert_bronze_batch(spark: SparkSession, batch_df, batch_id: int, output_path: str) -> None:
    batch_started_clock = time.monotonic()
    batch_started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    input_count = batch_df.count()
    if not input_count:
        print(f"Kafka micro-batch {batch_id} is empty.", flush=True)
        return

    decoded = _with_contract_reasons(
        batch_df.select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("event_json"),
        )
        .withColumn("_envelope", F.from_json("event_json", EVENT_ENVELOPE_SCHEMA))
    )
    invalid = decoded.filter(F.col("_contract_rejection_reason") != "")
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
            F.col("_contract_rejection_reason").alias("rejection_reason"),
            F.current_timestamp().alias("quarantined_at"),
        )
        _append_delta_batch_once(
            quarantine, quarantine_path, "quarantine", batch_id
        )
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
        F.col("_contract_rejection_reason") == ""
    ).drop("_envelope", "_contract_rejection_reason")
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
            "event_time_contract_version",
            F.expr("try_cast(get_json_object(event_json, '$.event_time_contract_version') AS INT)"),
        )
        .withColumn("event_time_basis", F.get_json_object("event_json", "$.event_time_basis"))
        .withColumn(
            "source_record_id",
            F.get_json_object("event_json", "$.source_record_id"),
        )
        .withColumn("business_key", F.get_json_object("event_json", "$.business_key"))
        .withColumn("payload_hash", F.get_json_object("event_json", "$.payload_hash"))
        .withColumn(
            "event_time",
            F.coalesce(
                F.try_to_timestamp(F.get_json_object("event_json", "$.release_ts")),
                F.when(
                    (F.col("event_time_contract_version") >= 2)
                    & (F.col("event_time_basis") == "source_event_time"),
                    F.try_to_timestamp(F.get_json_object("event_json", "$.event_time")),
                ),
                F.try_to_timestamp(F.get_json_object("event_json", "$.ingested_at")),
                F.col("kafka_timestamp").cast("timestamp"),
            ),
        )
        .withColumn(
            "event_time_basis",
            F.when(
                F.try_to_timestamp(F.get_json_object("event_json", "$.release_ts")).isNotNull(),
                "source_release_ts",
            )
            .when(
                (F.col("event_time_contract_version") >= 2)
                & (F.col("event_time_basis") == "source_event_time")
                & F.try_to_timestamp(F.get_json_object("event_json", "$.event_time")).isNotNull(),
                "source_event_time",
            )
            .when(
                F.try_to_timestamp(F.get_json_object("event_json", "$.ingested_at")).isNotNull(),
                "source_ingested_at",
            )
            .when(F.col("kafka_timestamp").isNotNull(), "kafka_timestamp_proxy")
            .otherwise("unavailable"),
        )
        .withColumn(
            "event_lag_seconds",
            F.unix_timestamp("kafka_timestamp") - F.unix_timestamp("event_time"),
        )
        .withColumn(
            "is_late_arrival",
            F.col("event_lag_seconds")
            > F.lit(int(os.getenv("BRONZE_LATE_ARRIVAL_THRESHOLD_SECONDS", "86400"))),
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
    _append_delta_batch_once(batch, output_path, "kafka-events", batch_id)

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS bronze.kafka_events "
        f"USING DELTA LOCATION '{output_path}'"
    )
    persisted_candidates = batch.count()
    batch_lag = batch.agg(
        F.sum(F.when(F.col("is_late_arrival"), 1).otherwise(0)).alias("late_events"),
        F.expr("percentile_approx(CASE WHEN event_lag_seconds >= 0 "
               "THEN event_lag_seconds END, 0.95)").alias("p95_source_to_kafka_seconds"),
    ).first()
    late_events = int(batch_lag["late_events"] or 0)
    p95_source_to_kafka_seconds = (
        float(batch_lag["p95_source_to_kafka_seconds"])
        if batch_lag["p95_source_to_kafka_seconds"] is not None else None
    )
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
        late_events,
        p95_source_to_kafka_seconds,
    )
    _record_bronze_metrics(
        input_count,
        input_count - invalid_count,
        invalid_count,
        persisted_candidates,
        processing_duration,
        late_events,
        p95_source_to_kafka_seconds,
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
