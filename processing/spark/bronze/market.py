"""Upsert raw market JSON snapshots into Bronze Delta tables on MinIO."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


INPUTS = {
    "brent_oil": "brent_oil_daily.json",
    "usd_vnd": "usd_vnd_daily.json",
    "vn_fuel": "vn_fuel_e10_ron95.json",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dir",
        default="/opt/spark/data/raw",
        help="Directory containing the raw market JSON snapshots.",
    )
    parser.add_argument(
        "--output-root",
        default=None,
        help="Optional Delta root. Defaults to s3a://<bucket>/bronze/market.",
    )
    return parser.parse_args()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_inputs(input_dir: Path, filename: str) -> list[Path]:
    legacy_file = input_dir / filename
    batch_dir = input_dir / Path(filename).stem
    files = ([legacy_file] if legacy_file.is_file() else []) + [
        path for path in sorted(batch_dir.glob("*.json")) if path.stat().st_size > 2
    ]
    if not files:
        raise FileNotFoundError(
            f"No market JSON found for {filename} in {input_dir} or {batch_dir}"
        )
    return files


def upsert_batches(spark: SparkSession, input_files: list[Path], table_name: str, output_path: str) -> int:
    missing_files = [path for path in input_files if not path.is_file()]
    if missing_files:
        raise FileNotFoundError(f"Market input JSON was not found: {missing_files[0]}")

    # Keep sources in separate tables, but combine their immutable batches for
    # an idempotent upsert into each source's Delta table.
    source = (
        spark.read.option("multiLine", "true")
        .option("mode", "FAILFAST")
        .json([str(path) for path in input_files])
        .withColumn(
            "_source_file",
            F.regexp_extract(F.input_file_name(), r"([^/\\]+\.json)$", 1),
        )
    )
    if "raw_payload" in source.columns:
        source = source.withColumn(
            "raw_payload",
            F.to_json(F.col("raw_payload"), options={"ignoreNullFields": "false"}),
        )
    if table_name == "vn_fuel":
        source_date = F.substring(F.get_json_object("raw_payload", "$.date"), 1, 10)
        observation_date = F.substring(F.col("observation_period"), 1, 10)
        source = source.filter(source_date.isNull() | (source_date == observation_date))

    batch_ids = {path.name: file_sha256(path) for path in input_files}
    batch_order = F.lit(-1)
    for order, path in enumerate(input_files):
        batch_order = F.when(F.col("_source_file") == path.name, F.lit(order)).otherwise(batch_order)
    latest = Window.partitionBy("event_id").orderBy(F.col("_batch_order").desc())
    batch = (
        source.withColumn("_batch_order", batch_order)
        .withColumn("_row_number", F.row_number().over(latest))
        .filter(F.col("_row_number") == 1)
        .withColumn("bronze_ingested_at", F.current_timestamp())
        .withColumnRenamed("_source_file", "bronze_source_file")
        .withColumn(
            "bronze_batch_id",
            F.coalesce(*[
                F.when(F.col("bronze_source_file") == name, F.lit(batch_id))
                for name, batch_id in batch_ids.items()
            ]),
        )
        .drop("_batch_order", "_row_number")
    )
    record_count = batch.count()
    if record_count == 0:
        raise ValueError(f"Market JSON inputs contain no records for {table_name}")

    delta_log = spark._jvm.org.apache.hadoop.fs.Path(f"{output_path.rstrip('/')}/_delta_log")
    filesystem = delta_log.getFileSystem(spark._jsc.hadoopConfiguration())
    if filesystem.exists(delta_log):
        view_name = f"market_bronze_batch_{table_name}"
        batch.createOrReplaceTempView(view_name)
        spark.sql(
            f"""
            MERGE INTO delta.`{output_path}` AS target
            USING {view_name} AS source
            ON target.event_id = source.event_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    else:
        batch.write.format("delta").mode("overwrite").save(output_path)

    spark.sql(
        f"CREATE TABLE IF NOT EXISTS bronze.{table_name} "
        f"USING DELTA LOCATION '{output_path}'"
    )
    return record_count


def main() -> None:
    args = parse_args()
    input_dir = Path(args.input_dir)
    bucket = os.getenv("MINIO_BUCKET", "nowcpi")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "nowcpi")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "nowcpi-local-password")
    output_root = (args.output_root or f"s3a://{bucket}/bronze/market").rstrip("/")

    spark = (
        SparkSession.builder.appName("NowCPI-Bronze-Market")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS bronze LOCATION 's3a://" + bucket + "/bronze'")
        for table_name, filename in INPUTS.items():
            output_path = f"{output_root}/{table_name}"
            input_files = discover_inputs(input_dir, filename)
            count = upsert_batches(spark, input_files, table_name, output_path)
            print(
                f"Upserted {count} {table_name} records from {len(input_files)} JSON file(s) "
                f"into {output_path}"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
