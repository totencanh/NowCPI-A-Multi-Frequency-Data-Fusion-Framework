"""Consume NowCPI source events from Kafka and upsert raw events into Bronze."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


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


def upsert_bronze_batch(spark: SparkSession, batch_df, batch_id: int, output_path: str) -> None:
    if not batch_df.head(1):
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
    print(
        f"Upserted {batch.count()} event(s) from Kafka micro-batch {batch_id} "
        f"into bronze.kafka_events.",
        flush=True,
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
