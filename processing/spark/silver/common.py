"""Shared Bronze-to-Silver normalization and Delta persistence helpers."""

from __future__ import annotations

import os
import re
from collections.abc import Iterable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


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
    "source_ingested_at", "available_at", "bronze_ingested_at",
    "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
    "series_id", "country", "observation_date", "frequency", "value", "unit",
]
SILVER_BUSINESS_KEY = ["series_id", "country", "observation_date", "unit"]
MIN_OBSERVATION_DATE = "2025-01-01"

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
        "vn_fuel_e10_ron95"
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
            F.col("kafka_timestamp"),
            F.col("bronze_ingested_at"),
            F.col("source_ingested_at"),
        ),
    )

    topic_series = {
        "nowcpi.cpi": F.col("series_id") == "cpi_headline",
        "nowcpi.cpi_components": F.col("series_id").startswith("cpi_component_"),
        "nowcpi.ppi_iip": F.col("series_id") == "iip_growth",
        "nowcpi.brent_oil": F.col("series_id") == "brent_front_month",
        "nowcpi.usd_vnd": F.col("series_id") == "usd_vnd",
        "nowcpi.vn_fuel": F.col("series_id").isin("vn_fuel_ron95", "vn_fuel_e10_ron95"),
    }[topic]
    expected_frequency = (
        F.when(F.col("series_id").startswith("cpi_component_"), "monthly")
        .when(F.col("series_id") == "cpi_headline", "monthly")
        .when(F.col("series_id") == "iip_growth", "annual")
        .when(F.col("series_id").isin("brent_front_month", "usd_vnd", "vn_fuel_ron95", "vn_fuel_e10_ron95"), "daily")
    )
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
        F.when(F.col("series_id").isNull() | (F.col("series_id") == ""), "missing_series_id"),
        F.when(F.col("country").isNull() | (F.col("country") == ""), "missing_country"),
        F.when(F.col("observation_date").isNull(), "invalid_observation_period"),
        F.when(F.col("frequency").isNull() | (F.col("frequency") == ""), "missing_frequency"),
        F.when(~topic_series, "series_does_not_match_topic"),
        F.when(expected_frequency.isNotNull() & (F.col("frequency") != expected_frequency), "frequency_does_not_match_series"),
        F.when(F.col("unit").isNull() | (F.col("unit") == ""), "missing_unit"),
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


def _silver_offsets_path(bucket: str) -> str:
    return f"s3a://{bucket}/silver/_checkpoints/kafka_offsets"


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
    if offsets.limit(1).count() == 0:
        return

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


def _upsert_silver_rejections(
    spark: SparkSession, frame: DataFrame, table_name: str, path: str
) -> int:
    """Persist invalid/out-of-scope events for inspection instead of losing them."""
    frame = frame.dropDuplicates(["kafka_topic", "kafka_partition", "kafka_offset"])
    count = frame.count()
    if count == 0:
        return 0

    escaped_path = _quote_sql_path(path)
    if not _delta_exists(spark, path):
        frame.write.format("delta").mode("overwrite").save(path)
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
    """Keep only observations from 2025-01-01 through the current date."""
    if not _delta_exists(spark, path):
        return
    escaped_path = _quote_sql_path(path)
    spark.sql(
        f"""DELETE FROM delta.`{escaped_path}`
            WHERE observation_date < DATE '{MIN_OBSERVATION_DATE}'
               OR observation_date > current_date()"""
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
    """Normalize valid in-scope events and merge them into one compact table."""
    good_path = f"s3a://{bucket}/silver/{table_name}"
    rejected_table = f"{table_name}_rejected"
    rejected_path = f"s3a://{bucket}/silver/{rejected_table}"
    _prune_silver_date_range(spark, good_path)

    topic_events = bronze.filter(F.col("kafka_topic") == topic)
    if topic_events.limit(1).count() == 0:
        print(
            f"No Bronze rows for {topic}; registering the Silver table if needed.",
            flush=True,
        )
    full_refresh = os.getenv("SILVER_FULL_REFRESH", "0").lower() in {
        "1", "true", "yes"
    }
    table_exists = _delta_exists(spark, good_path)
    schema_changed = table_exists and (
        spark.read.format("delta").load(good_path).columns != SILVER_COLUMNS
    )
    rebuild = full_refresh or not table_exists or schema_changed
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

    latest = Window.partitionBy("event_id").orderBy(
        F.col("bronze_ingested_at").desc_nulls_last(),
        F.col("kafka_timestamp").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
    )
    normalized = (
        normalize_bronze_events(source, topic)
        .withColumn("_row_num", F.row_number().over(latest))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    # Collapse duplicate source observations within a topic/domain. Stable
    # Bronze IDs break ties after source revision time and Kafka order.
    business_latest = Window.partitionBy(*SILVER_BUSINESS_KEY).orderBy(
        F.col("source_ingested_at").desc_nulls_last(),
        F.col("bronze_ingested_at").desc_nulls_last(),
        F.col("kafka_offset").desc_nulls_last(),
        F.col("event_id").desc(),
    )
    normalized = (
        normalized.withColumn("_business_row", F.row_number().over(business_latest))
        .filter(F.col("_business_row") == 1)
        .drop("_business_row")
    )

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
    in_scope = F.col("observation_date").isNull() | (
        (F.col("observation_date") >= F.lit(MIN_OBSERVATION_DATE).cast("date"))
        & (F.col("observation_date") <= F.current_date())
    )
    out_of_scope = normalized.filter(
        F.col("observation_date").isNotNull() & ~in_scope
    )
    out_of_scope_count = out_of_scope.count()
    rejected = (
        normalized.filter(
            (F.col("rejection_reason") != "")
            | (F.col("observation_date").isNotNull() & ~in_scope)
        )
        .withColumn(
            "rejection_reason",
            F.concat_ws(
                ";",
                F.col("rejection_reason"),
                F.when(~in_scope, "observation_out_of_scope"),
            ),
        )
        .select(*SILVER_COLUMNS, "rejection_reason")
    )
    normalized = normalized.filter(in_scope)
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
        spark, rejected, rejected_table, rejected_path
    )
    _prune_silver_date_range(spark, good_path)
    _save_silver_checkpoint(spark, source, bucket, topic)
    print(
        f"{topic}: upserted {good_count} valid observation(s); "
        f"persisted {persisted_rejections} rejected event(s); "
        f"{rejected_count} invalid and {out_of_scope_count} out-of-scope.",
        flush=True,
    )
    return good_count, rejected_count + out_of_scope_count


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
        bronze = spark.read.format("delta").load(bronze_path)
        missing = sorted(REQUIRED_BRONZE_COLUMNS - set(bronze.columns))
        if missing:
            raise ValueError(f"Bronze table is missing required columns: {', '.join(missing)}")
        for topic in selected:
            process_topic(spark, bucket, bronze, topic, TOPIC_TABLES[topic])
        _drop_obsolete_silver_tables(
            spark,
            bucket,
            drop_legacy_ppi_iip="nowcpi.ppi_iip" in selected,
        )
    finally:
        spark.stop()


def run_silver(
    topics: Iterable[str] | None = None,
    app_name: str = "NowCPI-Silver-Normalization",
) -> None:
    """Compatibility entry point for manual all-topic/topic-selected runs."""
    run_domain(topics or TOPIC_TABLES.keys(), app_name)
