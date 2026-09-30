"""Upsert the raw IMF CPI JSON snapshot into the Bronze Delta table on MinIO."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default="/opt/spark/data/raw/imf_cpi_vietnam.json",
        help="Path to the source JSON file, visible to the Spark driver.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional Delta table path. Defaults to s3a://<bucket>/bronze/cpi.",
    )
    return parser.parse_args()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    if not input_path.is_file():
        raise FileNotFoundError(f"CPI input JSON was not found: {input_path}")

    bucket = os.getenv("MINIO_BUCKET", "nowcpi")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "nowcpi")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "nowcpi-local-password")
    output_path = args.output or f"s3a://{bucket}/bronze/cpi"
    batch_id = file_sha256(input_path)

    spark = (
        SparkSession.builder.appName("NowCPI-Bronze-CPI")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        # The source is a JSON array; multiline mode reads it as one JSON document.
        # No columns or source records are filtered out at the Bronze layer.
        cpi = (
            spark.read.option("multiLine", "true")
            .option("mode", "FAILFAST")
            .json(str(input_path))
            .withColumn("bronze_ingested_at", F.current_timestamp())
            .withColumn("bronze_source_file", F.lit(input_path.name))
            .withColumn("bronze_batch_id", F.lit(batch_id))
        )

        record_count = cpi.count()
        if record_count == 0:
            raise ValueError(f"CPI JSON contains no records: {input_path}")

        # Keep one Delta table. Merge on the stable event ID so rerunning this
        # snapshot updates existing observations rather than creating another
        # independent Delta table for each input-file version.
        delta_log = spark._jvm.org.apache.hadoop.fs.Path(
            f"{output_path.rstrip('/')}/_delta_log"
        )
        filesystem = delta_log.getFileSystem(spark._jsc.hadoopConfiguration())
        if filesystem.exists(delta_log):
            cpi.createOrReplaceTempView("cpi_bronze_batch")
            spark.sql(
                f"""
                MERGE INTO delta.`{output_path}` AS target
                USING cpi_bronze_batch AS source
                ON target.event_id = source.event_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        else:
            cpi.write.format("delta").mode("overwrite").save(output_path)

        database_path = f"s3a://{bucket}/bronze"
        spark.sql(
            f"CREATE DATABASE IF NOT EXISTS bronze LOCATION '{database_path}'"
        )
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS bronze.cpi USING DELTA LOCATION '{output_path}'"
        )
        print(f"Upserted {record_count} CPI records into {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
