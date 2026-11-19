"""Shared Bronze-to-Silver normalization and Delta persistence helpers."""

from __future__ import annotations

import os
import re
import json
import sys
import uuid
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

_processing_root = Path(__file__).resolve().parents[2]
if str(_processing_root) not in sys.path:
    sys.path.insert(0, str(_processing_root))
from spark.common.schemas import TOPIC_CONTRACTS

try:
    from spark.common.prometheus_metrics import push_stage_metrics
except ImportError:
    push_stage_metrics = None


# Silver keeps each economic indicator in its own table. These mappings are
# topic-level contracts from ingestion/common.py and bronze/kafka_stream.py.
CPI_TOPICS = {
    "nowcpi.cpi": "cpi_observations",
    "nowcpi.cpi_components": "cpi_component_observations",
    "nowcpi.ppi_iip": "iip_observations",
}
MARKET_TOPICS = {
    "nowcpi.brent_oil": "brent_oil_observations",
    "nowcpi.usd_vnd": "usd_vnd_observations",
    "nowcpi.vn_fuel": "vn_fuel_observations",
}
TOPIC_TABLES = {**CPI_TOPICS, **MARKET_TOPICS}

REQUIRED_BRONZE_COLUMNS = {
    "event_id", "source", "series_id", "country", "observation_period",
    "frequency", "value", "unit", "release_ts", "ingested_at",
    "source_record_id", "kafka_topic", "kafka_partition", "kafka_offset",
    "kafka_timestamp", "bronze_ingested_at",
}

SILVER_COLUMNS = [
    "event_id", "source", "source_record_id", "source_release_ts",
    "source_event_time", "source_event_time_basis", "source_to_kafka_lag_seconds",
    "is_late_arrival", "business_key", "payload_hash",
    "source_ingested_at", "available_at", "bronze_ingested_at",
    "available_at_basis",
    "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
    "series_id", "country", "observation_date", "frequency", "value", "unit",
]
VERSION_COLUMNS = [
    "version_id", "event_id", "source", "source_record_id",
    "source_release_ts", "source_event_time", "business_key", "payload_hash",
    "source_ingested_at", "available_at", "available_at_basis",
    "bronze_ingested_at", "kafka_topic", "kafka_partition", "kafka_offset",
    "kafka_timestamp",
    "series_id", "country", "observation_date", "frequency", "value", "unit",
]
SILVER_BUSINESS_KEY = ["series_id", "country", "frequency", "observation_date", "unit"]
SILVER_TRANSFORM_VERSION = 7

OBSOLETE_SILVER_TABLES = [
    "cpi_observations_quarantine",
    "cpi_component_observations_quarantine",
    "ppi_iip_observations_quarantine",
    "iip_observations_quarantine",
    "brent_oil_observations_quarantine",
    "usd_vnd_observations_quarantine",
    "vn_fuel_observations_quarantine",
]


def _quote_sql_path(path: str) -> str:
    """Escape a path embedded in a Delta SQL string."""
    return path.replace("'", "''")


def create_spark(app_name: str) -> tuple[SparkSession, str]:
    """Create the same Delta/S3A/Hive Spark session used by the Bronze jobs."""
    bucket = os.getenv("MINIO_BUCKET", "nowcpi")
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "nowcpi"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "nowcpi-local-password"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark, bucket


def _optional_col(frame: DataFrame, name: str, data_type: str = "string"):
    """Return a typed null for optional columns missing in older Bronze logs."""
    if name in frame.columns:
        return F.col(name)
    return F.lit(None).cast(data_type)


def _observation_date(period):
    """Parse common ISO dates, timestamps, YYYY-MM months, and YYYY years."""
    trimmed = F.trim(period)
    month_period = trimmed.rlike(r"^\d{4}-\d{2}$")
    year_period = trimmed.rlike(r"^\d{4}$")
    iso_date_or_timestamp = trimmed.rlike(r"^\d{4}-\d{2}-\d{2}($|[T ])")
    date_text = (
        F.when(month_period, F.concat(trimmed, F.lit("-01")))
        .when(year_period, F.concat(trimmed, F.lit("-01-01")))
        .when(iso_date_or_timestamp, F.substring(trimmed, 1, 10))
    )
    valid_shape = month_period | year_period | iso_date_or_timestamp
    # Extracting the ISO date before parsing avoids trying a date-only pattern
    # against timestamp strings, which Spark 3.5 can raise as a parser upgrade
    # exception instead of treating as a non-match.
    parsed_date = F.to_date(F.try_to_timestamp(date_text, F.lit("yyyy-MM-dd")))
    return F.when(valid_shape, parsed_date)


def _frequency(value):
    value = F.lower(F.trim(value))
    return (
        F.when(value.isin("m", "month", "monthly"), "monthly")
        .when(value.isin("d", "day", "daily"), "daily")
        .when(value.isin("a", "y", "year", "yearly", "annual"), "annual")
        .otherwise(value)
    )


def _canonical_unit(series_id, unit):
    """Standardize known market units while retaining all other source units."""
    cleaned = F.regexp_replace(F.trim(unit), r"\s+", " ")
    compact = F.lower(F.regexp_replace(cleaned, r"\s+", ""))
    return (
        F.when(
            (series_id == "brent_front_month")
            & compact.isin("usd/barrel", "usd/bbl", "$/barrel", "usdperbarrel"),
            "USD/barrel",
        )
        .when(
            (series_id == "usd_vnd")
            & compact.isin("vnd/usd", "vndperusd"),
            "VND/USD",
        )
        .when(
            series_id.isin("vn_fuel_ron95", "vn_fuel_e10_ron95")
            & compact.isin("vnd/liter", "vnd/litre", "vndperliter"),
            "VND/liter",
        )
        .otherwise(cleaned)
    )


def _is_positive_series():
    """Return the series predicate whose measurements must be positive."""
    return F.col("series_id").isin(
        "cpi_headline", "brent_front_month", "usd_vnd", "vn_fuel_ron95",
        "vn_fuel_e10_ron95", "iip_total_index"
    ) | F.col("series_id").startswith("cpi_component_")


def normalize_bronze_events(events: DataFrame, topic: str) -> DataFrame:
    """Enforce the Bronze event contract and produce normalized Silver columns."""
    if topic not in TOPIC_TABLES:
        raise ValueError(f"No Silver domain is configured for Kafka topic {topic!r}")
    missing = sorted(REQUIRED_BRONZE_COLUMNS - set(events.columns))
    if missing:
        raise ValueError(f"Bronze table is missing required columns: {', '.join(missing)}")

    raw_period = F.trim(F.col("observation_period"))
    raw_value = F.expr("try_cast(value AS DOUBLE)")
    raw_series = F.lower(F.trim(F.col("series_id")))
    raw_payload = _optional_col(events, "raw_payload_json")
    fuel_title = F.trim(F.get_json_object(raw_payload, "$.title"))
    # The source changed from regular RON 95 to E10 RON 95 in May 2026.
    # Preserve that product distinction in Silver; Bronze keeps the full payload.
    normalized_series = (
        F.when(
            (raw_series == "vn_fuel_e10_ron95")
            & (fuel_title == "Xăng RON 95 Mức 5"),
            "vn_fuel_ron95",
        )
        .otherwise(raw_series)
    )
    freq = _frequency(F.col("frequency"))
    obs_date = _observation_date(raw_period)
    release_time = F.try_to_timestamp(F.col("release_ts"))
    source_ingested_time = F.try_to_timestamp(F.col("ingested_at"))
    kafka_received_time = F.col("kafka_timestamp").cast("timestamp")
    event_time_contract_version = _optional_col(
        events, "event_time_contract_version", "int"
    ).cast("int")
    stored_event_time_basis = F.trim(_optional_col(events, "event_time_basis"))
    explicit_source_event_time = F.when(
        (event_time_contract_version >= 2)
        & (stored_event_time_basis == "source_event_time"),
        F.try_to_timestamp(_optional_col(events, "event_time")),
    )
    source_event_time = F.coalesce(
        release_time, explicit_source_event_time, source_ingested_time, kafka_received_time
    )
    computed_transport_lag = (
        F.unix_timestamp(kafka_received_time) - F.unix_timestamp(source_event_time)
    )
    stored_transport_lag = _optional_col(events, "event_lag_seconds", "double").cast("double")
    source_to_kafka_lag = F.coalesce(stored_transport_lag, computed_transport_lag)
    late_threshold_seconds = int(os.getenv("BRONZE_LATE_ARRIVAL_THRESHOLD_SECONDS", "86400"))
    if "schema_version" in events.columns:
        schema_version = F.expr("try_cast(schema_version AS INT)")
    else:
        schema_version = (
            F.expr("try_cast(get_json_object(event_json, '$.schema_version') AS INT)")
            if "event_json" in events.columns
            else F.lit(1).cast("int")
        )

    normalized = events.select(
        F.trim(F.col("event_id")).alias("event_id"),
        schema_version.alias("schema_version"),
        F.lower(F.trim(F.col("source"))).alias("source"),
        F.trim(F.col("source_record_id")).alias("source_record_id"),
        F.try_to_timestamp(F.col("release_ts")).alias("source_release_ts"),
        normalized_series.alias("series_id"),
        F.upper(F.trim(F.col("country"))).alias("country"),
        obs_date.alias("observation_date"),
        freq.alias("frequency"),
        raw_value.alias("value"),
        _canonical_unit(normalized_series, F.col("unit")).alias("unit"),
        source_event_time.alias("source_event_time"),
        F.when(release_time.isNotNull(), "source_release_ts")
        .when(explicit_source_event_time.isNotNull(), "source_event_time")
        .when(source_ingested_time.isNotNull(), "source_ingested_at")
        .when(kafka_received_time.isNotNull(), "kafka_timestamp_proxy")
        .otherwise("unavailable").alias("source_event_time_basis"),
        source_to_kafka_lag.alias("source_to_kafka_lag_seconds"),
        F.coalesce(
            _optional_col(events, "is_late_arrival", "boolean").cast("boolean"),
            source_to_kafka_lag > F.lit(late_threshold_seconds),
        ).alias("is_late_arrival"),
        F.coalesce(
            F.trim(_optional_col(events, "business_key")),
            F.to_json(
                F.struct(
                    F.lower(F.trim(F.col("source"))).alias("source"),
                    F.upper(F.trim(F.col("country"))).alias("country"),
                    normalized_series.alias("series_id"),
                    freq.alias("frequency"),
                    obs_date.cast("string").alias("observation_period"),
                    _canonical_unit(normalized_series, F.col("unit")).alias("unit"),
                )
            ),
        ).alias("business_key"),
        F.coalesce(
            F.trim(_optional_col(events, "payload_hash")),
            F.sha2(_optional_col(events, "event_json"), 256),
        ).alias("payload_hash"),
        F.try_to_timestamp(F.col("ingested_at")).alias("source_ingested_at"),
        F.col("bronze_ingested_at").cast("timestamp").alias("bronze_ingested_at"),
        F.col("kafka_timestamp").cast("timestamp").alias("kafka_timestamp"),
        F.col("kafka_topic").alias("kafka_topic"),
        F.col("kafka_partition").cast("int").alias("kafka_partition"),
        F.col("kafka_offset").cast("long").alias("kafka_offset"),
    )

    normalized = normalized.withColumn(
        "available_at",
        F.coalesce(
            F.col("source_release_ts"),
            F.col("source_ingested_at"),
            F.col("kafka_timestamp"),
            F.col("bronze_ingested_at"),
        ),
    ).withColumn(
        "available_at_basis",
        F.when(F.col("source_release_ts").isNotNull(), "source_release_ts")
        .when(F.col("source_ingested_at").isNotNull(), "source_ingested_at")
        .when(F.col("kafka_timestamp").isNotNull(), "kafka_timestamp")
        .when(F.col("bronze_ingested_at").isNotNull(), "bronze_ingested_at"),
    )

    contract = TOPIC_CONTRACTS[topic]
    numeric_invalid = (
        F.col("value").isNull()
        | F.isnan("value")
        | (F.abs(F.col("value")) == F.lit(float("inf")))
    )
    positive_series = _is_positive_series()
    metadata_issues = [
        F.when(F.col("event_id").isNull() | (F.col("event_id") == ""), "missing_event_id"),
        F.when(F.col("schema_version").isNull() | (F.col("schema_version") != 1), "unsupported_schema_version"),
        F.when(F.col("source").isNull() | (F.col("source") == ""), "missing_source"),
        F.when(~F.col("source").isin(*sorted(contract["sources"])), "source_not_allowed_for_topic"),
        F.when(F.col("series_id").isNull() | (F.col("series_id") == ""), "missing_series_id"),
        F.when(~F.col("series_id").rlike(contract["series_pattern"]), "series_not_allowed_for_topic"),
        F.when(F.col("country").isNull() | (F.col("country") == ""), "missing_country"),
        F.when(F.col("country") != contract["country"], "country_not_allowed_for_topic"),
        F.when(F.col("observation_date").isNull(), "invalid_observation_period"),
        F.when(F.col("frequency").isNull() | (F.col("frequency") == ""), "missing_frequency"),
        F.when(F.col("frequency") != contract["frequency"], "frequency_mismatch"),
        F.when(F.col("unit").isNull() | ~F.col("unit").isin(*sorted(contract["units"])), "unit_mismatch"),
        F.when(F.col("source_record_id").isNull() | (F.col("source_record_id") == ""), "missing_source_record_id"),
    ]
    normalized = normalized.withColumn(
        "_metadata_rejection_reason", F.concat_ws(";", *metadata_issues)
    )
    return normalized


def _delta_exists(spark: SparkSession, path: str) -> bool:
    log = spark._jvm.org.apache.hadoop.fs.Path(f"{path.rstrip('/')}/_delta_log")
    return bool(log.getFileSystem(spark._jsc.hadoopConfiguration()).exists(log))


def upsert_silver_observations(
    spark: SparkSession,
    frame: DataFrame,
    table_name: str,
    path: str,
    rebuild: bool = False,
) -> int:
    """Merge clean observations by their natural business key."""
    frame = frame.dropDuplicates(SILVER_BUSINESS_KEY)
    count = frame.count()
    path_exists = _delta_exists(spark, path)
    escaped_path = _quote_sql_path(path)

    if rebuild or not path_exists:
        frame.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)
        if path_exists:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{escaped_path}'"
        )
        return count

    existing_columns = spark.read.format("delta").load(path).columns
    if existing_columns != frame.columns:
        raise RuntimeError(
            f"Cannot incrementally update {table_name} with a changed schema; "
            "rebuild it from the complete Bronze topic first."
        )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{escaped_path}'"
    )
    if count == 0:
        return 0

    view = "silver_incoming_" + re.sub(r"\W+", "_", table_name)
    frame.createOrReplaceTempView(view)
    match = " AND ".join(
        f"target.{column} = incoming.{column}" for column in SILVER_BUSINESS_KEY
    )
    try:
        spark.sql(
            f"""MERGE INTO delta.`{escaped_path}` AS target
                USING `{view}` AS incoming
                ON {match}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *"""
        )
    finally:
        spark.catalog.dropTempView(view)
    return count


def persist_observation_versions(
    spark: SparkSession, bucket: str, frame: DataFrame
) -> int:
    """Keep the first point-in-time receipt of every distinct valid observation version."""
    path = f"s3a://{bucket}/silver/observation_versions"
    versioned = (
        frame.withColumn(
            "version_id",
            F.sha2(
                F.concat_ws("|", F.col("business_key"), F.col("payload_hash")),
                256,
            ),
        )
        .select(*VERSION_COLUMNS)
        .withColumn(
            "_version_rank",
            F.row_number().over(
                Window.partitionBy("version_id").orderBy(
                    F.col("available_at").asc_nulls_last(),
                    F.col("kafka_timestamp").asc_nulls_last(),
                    F.col("kafka_offset").asc_nulls_last(),
                )
            ),
        )
        .filter(F.col("_version_rank") == 1)
        .drop("_version_rank")
    )
    count = versioned.count()
    path_exists = _delta_exists(spark, path)
    escaped_path = _quote_sql_path(path)
    if not path_exists:
        versioned.write.format("delta").mode("overwrite").save(path)
        spark.sql(
            "CREATE TABLE IF NOT EXISTS silver.observation_versions "
            f"USING DELTA LOCATION '{escaped_path}'"
        )
        return count

    existing_columns = spark.read.format("delta").load(path).columns
    if existing_columns != VERSION_COLUMNS:
        raise RuntimeError(
            "silver.observation_versions schema changed; rebuild the version table "
            "from Bronze history before continuing."
        )
    spark.sql(
        "CREATE TABLE IF NOT EXISTS silver.observation_versions "
        f"USING DELTA LOCATION '{escaped_path}'"
    )
    if count == 0:
        return 0

    view = "silver_observation_versions_incoming"
    versioned.createOrReplaceTempView(view)
    try:
        spark.sql(
            f"""MERGE INTO delta.`{escaped_path}` AS target
                USING `{view}` AS incoming
                ON target.version_id = incoming.version_id
                    WHEN MATCHED THEN UPDATE SET
                      source_event_time = incoming.source_event_time
                WHEN NOT MATCHED THEN INSERT *"""
        )
    finally:
        spark.catalog.dropTempView(view)
    return count


def _silver_offsets_path(bucket: str) -> str:
    return f"s3a://{bucket}/silver/_checkpoints/kafka_offsets"


def _silver_transform_versions_path(bucket: str) -> str:
    # This is tiny control-plane state, so keep it as a JSON object rather
    # than creating a Delta table for one integer per topic.
    return f"s3a://{bucket}/silver/_checkpoints/transform_versions.json"


def _read_json_object(spark: SparkSession, path: str) -> dict:
    """Read a small JSON control object through Hadoop's configured S3A FS."""
    target = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = target.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.exists(target):
        return {}
    stream = filesystem.open(target)
    reader = spark._jvm.java.io.BufferedReader(
        spark._jvm.java.io.InputStreamReader(stream, "UTF-8")
    )
    try:
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(str(line))
            line = reader.readLine()
        value = json.loads("\n".join(lines))
        if not isinstance(value, dict):
            raise ValueError(f"Expected a JSON object at {path}")
        return value
    finally:
        reader.close()


def _write_json_object(spark: SparkSession, path: str, value: dict) -> None:
    """Atomically replace a small JSON control object in the same S3A bucket."""
    target = spark._jvm.org.apache.hadoop.fs.Path(path)
    temp = spark._jvm.org.apache.hadoop.fs.Path(path + ".tmp")
    filesystem = target.getFileSystem(spark._jsc.hadoopConfiguration())
    if filesystem.exists(temp):
        filesystem.delete(temp, False)
    stream = filesystem.create(temp, True)
    writer = spark._jvm.java.io.OutputStreamWriter(stream, "UTF-8")
    try:
        writer.write(json.dumps(value, sort_keys=True))
        writer.flush()
    finally:
        writer.close()
    if filesystem.exists(target):
        filesystem.delete(target, False)
    if not filesystem.rename(temp, target):
        raise RuntimeError(f"Could not publish Silver control object {path}")


def _silver_policy_requires_rebuild(
    spark: SparkSession, bucket: str, topic: str
) -> bool:
    """Reconcile old checkpoints once when Silver inclusion rules change."""
    path = _silver_transform_versions_path(bucket)
    versions = _read_json_object(spark, path)
    return versions.get(topic) != SILVER_TRANSFORM_VERSION


def _save_silver_transform_version(
    spark: SparkSession, bucket: str, topic: str
) -> None:
    path = _silver_transform_versions_path(bucket)
    versions = _read_json_object(spark, path)
    versions[topic] = SILVER_TRANSFORM_VERSION
    _write_json_object(spark, path, versions)


def _write_silver_run_audit(
    spark: SparkSession,
    bucket: str,
    topic: str,
    input_events: int,
    accepted_events: int,
    rejected_events: int,
    stage_started_at: datetime,
    event_duplicate_records: int = 0,
    business_key_rows_collapsed: int = 0,
    metadata_valid_events: int = 0,
) -> None:
    """Record per-topic Silver processing lineage and commit timing."""
    path = f"s3a://{bucket}/silver/_lineage/stage_runs"
    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    task_id = os.getenv("AIRFLOW_CTX_TASK_ID", "spark_silver")
    audit = spark.createDataFrame(
        [
            (
                str(uuid.uuid4()),
                "silver",
                topic,
                run_id,
                task_id,
                input_events,
                accepted_events,
                rejected_events,
                event_duplicate_records,
                business_key_rows_collapsed,
                metadata_valid_events,
                stage_started_at,
            )
        ],
        "audit_id STRING, stage STRING, kafka_topic STRING, run_id STRING, "
        "task_id STRING, input_events LONG, accepted_events LONG, rejected_events LONG, "
        "event_duplicate_records LONG, business_key_rows_collapsed LONG, "
        "metadata_valid_events LONG, stage_started_at TIMESTAMP",
    )
    audit = audit.withColumn("stage_committed_at", F.current_timestamp())
    audit.write.format("delta").option("mergeSchema", "true").mode("append").save(path)
    spark.sql(
        "CREATE TABLE IF NOT EXISTS silver.stage_run_audit "
        f"USING DELTA LOCATION '{_quote_sql_path(path)}'"
    )
    if push_stage_metrics is not None:
        duration_seconds = max(
            0.0, (datetime.now(timezone.utc) - stage_started_at).total_seconds()
        )
        push_stage_metrics(
            component="silver",
            stage="silver",
            source=topic,
            status="success",
            values={
                "records_in": input_events,
                "records_accepted": accepted_events,
                "records_rejected": rejected_events,
                "event_duplicate_records_removed": event_duplicate_records,
                "business_key_rows_collapsed": business_key_rows_collapsed,
                "metadata_valid_events": metadata_valid_events,
                "completeness_percent": round(100 * metadata_valid_events / input_events, 2)
                if input_events else None,
                "acceptance_rate_percent": round(100 * accepted_events / input_events, 2)
                if input_events else None,
                "throughput_records_per_second": round(input_events / duration_seconds, 3)
                if duration_seconds > 0 else None,
                "stage_duration_seconds": duration_seconds,
            },
        )


def _after_silver_checkpoint(
    spark: SparkSession, source: DataFrame, bucket: str, topic: str
) -> DataFrame:
    """Keep only Bronze events newer than the last successfully published offsets."""
    if os.getenv("SILVER_FULL_REFRESH", "0").lower() in {"1", "true", "yes"}:
        print(f"{topic}: full refresh requested; processing all Bronze events.", flush=True)
        return source

    path = _silver_offsets_path(bucket)
    if not _delta_exists(spark, path):
        print(
            f"{topic}: no Silver offset checkpoint; bootstrapping from full Bronze history.",
            flush=True,
        )
        return source

    offsets = (
        spark.read.format("delta").load(path)
        .filter(F.col("kafka_topic") == topic)
        .select("kafka_partition", "kafka_offset")
        .withColumnRenamed("kafka_offset", "_silver_last_offset")
    )
    return (
        source.join(offsets, "kafka_partition", "left")
        .filter(
            F.col("_silver_last_offset").isNull()
            | (F.col("kafka_offset") > F.col("_silver_last_offset"))
        )
        .drop("_silver_last_offset")
    )


def _save_silver_checkpoint(
    spark: SparkSession, source: DataFrame, bucket: str, topic: str
) -> None:
    """Advance offsets after the topic's Silver table is reconciled."""
    path = _silver_offsets_path(bucket)
    offsets = (
        source.groupBy("kafka_topic", "kafka_partition")
        .agg(F.max(F.col("kafka_offset").cast("long")).alias("kafka_offset"))
    )
    if offsets.limit(1).count() > 0:
        view = "silver_offset_batch"
        offsets.createOrReplaceTempView(view)
        escaped_path = _quote_sql_path(path)
        try:
            if _delta_exists(spark, path):
                spark.sql(
                    f"""MERGE INTO delta.`{escaped_path}` AS target
                        USING `{view}` AS incoming
                        ON target.kafka_topic = incoming.kafka_topic
                           AND target.kafka_partition = incoming.kafka_partition
                        WHEN MATCHED AND incoming.kafka_offset > target.kafka_offset
                          THEN UPDATE SET kafka_offset = incoming.kafka_offset
                        WHEN NOT MATCHED THEN INSERT *"""
                )
            else:
                offsets.write.format("delta").mode("overwrite").save(path)
        finally:
            spark.catalog.dropTempView(view)
    _save_silver_transform_version(spark, bucket, topic)


def _upsert_silver_rejections(
    spark: SparkSession,
    frame: DataFrame,
    table_name: str,
    path: str,
    rebuild: bool = False,
) -> int:
    """Persist invalid or future-dated events for inspection instead of losing them."""
    frame = frame.dropDuplicates(["kafka_topic", "kafka_partition", "kafka_offset"])
    count = frame.count()
    path_exists = _delta_exists(spark, path)
    if count == 0 and not (rebuild and path_exists):
        return 0

    escaped_path = _quote_sql_path(path)
    if rebuild or not path_exists:
        frame.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(path)
    else:
        view = "silver_rejected_" + re.sub(r"\W+", "_", table_name)
        frame.createOrReplaceTempView(view)
        try:
            spark.sql(
                f"""MERGE INTO delta.`{escaped_path}` AS target
                    USING `{view}` AS incoming
                    ON target.kafka_topic = incoming.kafka_topic
                       AND target.kafka_partition = incoming.kafka_partition
                       AND target.kafka_offset = incoming.kafka_offset
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *"""
            )
        finally:
            spark.catalog.dropTempView(view)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS silver.{table_name} "
        f"USING DELTA LOCATION '{escaped_path}'"
    )
    return count


def _prune_silver_date_range(spark: SparkSession, path: str) -> None:
    """Remove future-dated observations but preserve valid economic history."""
    if not _delta_exists(spark, path):
        return
    escaped_path = _quote_sql_path(path)
    spark.sql(
        f"""DELETE FROM delta.`{escaped_path}`
            WHERE observation_date > current_date()"""
    )


def _drop_obsolete_silver_tables(
    spark: SparkSession, bucket: str, drop_legacy_ppi_iip: bool = False
) -> None:
    """Remove unused quarantine tables and optionally the former PPI/IIP table."""
    obsolete_tables = list(OBSOLETE_SILVER_TABLES)
    if drop_legacy_ppi_iip:
        obsolete_tables.append("ppi_iip_observations")
    for table_name in obsolete_tables:
        spark.sql(f"DROP TABLE IF EXISTS silver.{table_name}")
        path = f"s3a://{bucket}/silver/{table_name}"
        delta_log = spark._jvm.org.apache.hadoop.fs.Path(f"{path}/_delta_log")
        filesystem = delta_log.getFileSystem(spark._jsc.hadoopConfiguration())
        if filesystem.exists(delta_log):
            filesystem.delete(spark._jvm.org.apache.hadoop.fs.Path(path), True)


def process_topic(
    spark: SparkSession,
    bucket: str,
    bronze: DataFrame,
    topic: str,
    table_name: str,
) -> tuple[int, int]:
    """Normalize valid events, preserve history, and quarantine future-dated rows."""
    stage_started_at = datetime.now(timezone.utc)
    good_path = f"s3a://{bucket}/silver/{table_name}"
    rejected_table = f"{table_name}_rejected"
    rejected_path = f"s3a://{bucket}/silver/{rejected_table}"
    _prune_silver_date_range(spark, good_path)

    # run_domain passes a cached topic slice so the repeated validation/count/
    # merge actions do not rescan the full Bronze Delta table.
    topic_events = bronze
    if topic_events.limit(1).count() == 0:
        print(
            f"No Bronze rows for {topic}; registering the Silver table if needed.",
            flush=True,
        )
    full_refresh = os.getenv("SILVER_FULL_REFRESH", "0").lower() in {
        "1", "true", "yes"
    }
    policy_rebuild = _silver_policy_requires_rebuild(spark, bucket, topic)
    table_exists = _delta_exists(spark, good_path)
    schema_changed = table_exists and (
        spark.read.format("delta").load(good_path).columns != SILVER_COLUMNS
    )
    rebuild = full_refresh or policy_rebuild or not table_exists or schema_changed
    if policy_rebuild and table_exists and not full_refresh:
        print(
            f"{table_name}: Silver transform policy changed; rebuilding once from full Bronze history.",
            flush=True,
        )
    if schema_changed:
        print(
            f"{table_name}: old Silver schema detected; rebuilding from full Bronze history.",
            flush=True,
        )
    source = (
        topic_events
        if rebuild
        else _after_silver_checkpoint(spark, topic_events, bucket, topic)
    )
    if source.limit(1).count() == 0:
        print(f"{topic}: no new Bronze offsets; Silver is already current.", flush=True)
    input_event_count = source.count()

    normalized_versions = normalize_bronze_events(source, topic)
    event_id_counts = (
        normalized_versions
        .filter(F.col("event_id").isNotNull() & (F.trim(F.col("event_id")) != ""))
        .groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .agg(F.sum(F.col("count") - 1).alias("duplicates"))
        .first()
    )
    event_duplicate_records = int(event_id_counts["duplicates"] or 0)
    version_numeric_valid = (
        F.col("value").isNotNull()
        & ~F.isnan("value")
        & (F.abs(F.col("value")) != F.lit(float("inf")))
    )
    version_positive_valid = ~_is_positive_series() | (F.col("value") > 0)
    version_future_valid = (
        F.col("observation_date").isNull()
        | (F.col("observation_date") <= F.current_date())
    )
    valid_versions = normalized_versions.filter(
        (F.col("_metadata_rejection_reason") == "")
        & version_numeric_valid
        & version_positive_valid
        & version_future_valid
    )
    version_count = persist_observation_versions(spark, bucket, valid_versions)

    latest = Window.partitionBy(
        F.coalesce(
            F.col("event_id"),
            F.concat_ws(
                ":",
                F.col("kafka_topic"),
                F.col("kafka_partition").cast("string"),
                F.col("kafka_offset").cast("string"),
            ),
        )
    ).orderBy(
        F.col("bronze_ingested_at").desc_nulls_last(),
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )
    normalized = (
        normalized_versions
        .withColumn("_row_num", F.row_number().over(latest))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    # Preserve each malformed event for quarantine. Only rows with valid key
    # metadata can be grouped by the natural observation key; otherwise many
    # unrelated null-key rows would collapse into one rejection.
    metadata_valid = F.coalesce(
        F.col("_metadata_rejection_reason") == "", F.lit(False)
    )
    metadata_rejected = normalized.filter(~metadata_valid)
    valid_metadata = normalized.filter(metadata_valid)
    metadata_valid_events = valid_metadata.count()

    # Collapse duplicate source observations within a topic/domain. Stable
    # Bronze IDs break ties after source revision time and Kafka order.
    business_latest = Window.partitionBy(*SILVER_BUSINESS_KEY).orderBy(
        F.col("source_ingested_at").desc_nulls_last(),
        F.col("bronze_ingested_at").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
        F.col("event_id").desc(),
    )
    valid_metadata = (
        valid_metadata.withColumn("_business_row", F.row_number().over(business_latest))
        .filter(F.col("_business_row") == 1)
        .drop("_business_row")
    )
    # This includes corrected source revisions for an existing natural key;
    # observation_versions retains those versions separately.
    business_key_rows_collapsed = max(0, metadata_valid_events - valid_metadata.count())
    normalized = valid_metadata.unionByName(metadata_rejected)

    numeric_invalid = (
        F.col("value").isNull()
        | F.isnan("value")
        | (F.abs(F.col("value")) == F.lit(float("inf")))
    )
    normalized = normalized.withColumn(
        "rejection_reason",
        F.concat_ws(
            ";",
            F.col("_metadata_rejection_reason"),
            F.when(numeric_invalid, "missing_or_non_numeric_value"),
            F.when(
                _is_positive_series() & (F.col("value") <= 0),
                "value_must_be_positive",
            ),
        ),
    ).drop("_metadata_rejection_reason")
    not_future_dated = F.col("observation_date").isNull() | (
        F.col("observation_date") <= F.current_date()
    )
    future_dated = normalized.filter(
        F.col("observation_date").isNotNull() & ~not_future_dated
    )
    future_dated_count = future_dated.count()
    rejected = (
        normalized.filter(
            (F.col("rejection_reason") != "")
            | (F.col("observation_date").isNotNull() & ~not_future_dated)
        )
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                ";",
                F.col("rejection_reason"),
                F.when(~not_future_dated, "future_observation_date"),
            ),
        )
        .select(*SILVER_COLUMNS, "rejection_reason")
    )
    normalized = normalized.filter(not_future_dated)
    valid = normalized.filter(F.col("rejection_reason") == "").select(*SILVER_COLUMNS)
    rejected_counts = (
        normalized.filter(F.col("rejection_reason") != "")
        .groupBy("rejection_reason")
        .count()
        .collect()
    )
    rejected_count = sum(row["count"] for row in rejected_counts)
    for row in rejected_counts:
        print(
            f"{topic}: skipped {row['count']} invalid event(s): {row['rejection_reason']}",
            flush=True,
        )

    good_count = upsert_silver_observations(
        spark, valid, f"silver.{table_name}", good_path, rebuild=rebuild
    )
    persisted_rejections = _upsert_silver_rejections(
        spark, rejected, rejected_table, rejected_path, rebuild=rebuild
    )
    _prune_silver_date_range(spark, good_path)
    _write_silver_run_audit(
        spark,
        bucket,
        topic,
        input_event_count,
        good_count,
        persisted_rejections,
        stage_started_at,
        event_duplicate_records,
        business_key_rows_collapsed,
        metadata_valid_events,
    )
    _save_silver_checkpoint(spark, source, bucket, topic)
    print(
        f"{topic}: upserted {good_count} valid observation(s); "
        f"recorded {version_count} source-version candidate(s); "
        f"persisted {persisted_rejections} rejected event(s); "
        f"{rejected_count} invalid and {future_dated_count} future-dated.",
        flush=True,
    )
    return good_count, rejected_count + future_dated_count


def run_domain(
    topics: Iterable[str],
    app_name: str,
) -> None:
    """Reconcile a selected domain from the existing Bronze Delta table."""
    selected = list(topics)
    unknown = sorted(set(selected) - set(TOPIC_TABLES))
    if unknown:
        raise ValueError(f"Topics are not configured for Silver: {', '.join(unknown)}")

    spark, bucket = create_spark(app_name)
    bronze_path = os.getenv("BRONZE_PATH", f"s3a://{bucket}/bronze/kafka_events")
    try:
        if not _delta_exists(spark, bronze_path):
            raise RuntimeError(f"Bronze Delta table does not exist at {bronze_path}")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze LOCATION 's3a://{bucket}/bronze'")
        spark.sql(f"CREATE DATABASE IF NOT EXISTS silver LOCATION 's3a://{bucket}/silver'")
        bronze = spark.read.format("delta").load(bronze_path).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        missing = sorted(REQUIRED_BRONZE_COLUMNS - set(bronze.columns))
        if missing:
            raise ValueError(f"Bronze table is missing required columns: {', '.join(missing)}")
        cached_rows = bronze.count()
        print(f"Cached {cached_rows} Bronze event(s) for this Silver domain run.", flush=True)
        for topic in selected:
            topic_events = bronze.filter(F.col("kafka_topic") == topic).persist(
                StorageLevel.MEMORY_AND_DISK
            )
            try:
                topic_events.count()
                process_topic(spark, bucket, topic_events, topic, TOPIC_TABLES[topic])
            finally:
                topic_events.unpersist(blocking=False)
        _drop_obsolete_silver_tables(
            spark,
            bucket,
            drop_legacy_ppi_iip="nowcpi.ppi_iip" in selected,
        )
    finally:
        if "bronze" in locals():
            bronze.unpersist(blocking=False)
        spark.stop()


def run_silver(
    topics: Iterable[str] | None = None,
    app_name: str = "NowCPI-Silver-Normalization",
) -> None:
    """Compatibility entry point for manual all-topic/topic-selected runs."""
    run_domain(topics or TOPIC_TABLES.keys(), app_name)
