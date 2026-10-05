"""Consume NowCPI source events from Kafka and upsert raw events into Bronze."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession, Window
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
        print(
            f"Discarding {invalid_count} malformed Kafka event(s) in micro-batch "
            f"{batch_id}: invalid JSON or missing event_id.",
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
        .withColumn(
            "raw_payload_json",
            F.get_json_object("event_json", "$.raw_payload"),
        )
        .withColumn(
            "bronze_key",
            F.when(
                F.length(F.trim(F.col("event_id"))) > 0,
                F.col("event_id"),
            ).otherwise(
                F.concat_ws(
                    ":",
                    F.col("kafka_topic"),
                    F.col("kafka_partition").cast("string"),
                    F.col("kafka_offset").cast("string"),
                )
            ),
        )
        .withColumn("bronze_ingested_at", F.current_timestamp())
    )

    # Replayed Kafka messages share event_id and resolve to one latest row.
    latest = Window.partitionBy("bronze_key").orderBy(
        F.col("kafka_timestamp").desc(), F.col("kafka_offset").desc()
    )
    batch = (
        events.withColumn("_row_number", F.row_number().over(latest))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
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
                WHEN MATCHED THEN UPDATE SET *
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
