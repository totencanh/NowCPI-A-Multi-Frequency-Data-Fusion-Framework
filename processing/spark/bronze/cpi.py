"""Upsert raw CPI, CPI component, and PPI/IIP JSON batches into Bronze."""

from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


SOURCES = {
    "cpi": "imf_cpi_vietnam.json",
    "cpi_components": "imf_cpi_components_vietnam.json",
    "ppi_iip": "worldbank_ppi_iip_vietnam.json",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        default=None,
        help="Optional single JSON file for the CPI source; otherwise read legacy data and all batches.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional Delta path override for bronze.cpi.",
    )
    return parser.parse_args()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source_file:
        for chunk in iter(lambda: source_file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discover_inputs(filename: str) -> list[Path]:
    raw_dir = Path("/opt/spark/data/raw")
    legacy_file = raw_dir / filename
    batch_files = sorted((raw_dir / Path(filename).stem).glob("*.json"))
    files = ([legacy_file] if legacy_file.is_file() else []) + [
        path for path in batch_files if path.stat().st_size > 2
    ]
    if not files:
        raise FileNotFoundError(f"No JSON found for {filename} in {raw_dir} or its batch directory")
    return files


def upsert_source(
    spark: SparkSession,
    input_files: list[Path],
    table_name: str,
    output_path: str,
) -> int:
    missing_files = [path for path in input_files if not path.is_file()]
    if missing_files:
        raise FileNotFoundError(f"Input JSON was not found: {missing_files[0]}")

    batch_ids = {path.name: file_sha256(path) for path in input_files}
    source = (
        spark.read.option("multiLine", "true")
        .option("mode", "FAILFAST")
        .json([str(path) for path in input_files])
        .withColumn(
            "_source_file",
            F.regexp_extract(F.input_file_name(), r"([^/\\]+\.json)$", 1),
        )
    )

    # The legacy full snapshot comes first; timestamped deltas are sorted by
    # filename, so the newest revision of an event wins during the merge.
    batch_order = F.lit(-1)
    for order, path in enumerate(input_files):
        batch_order = F.when(F.col("_source_file") == path.name, F.lit(order)).otherwise(batch_order)
    source = source.withColumn("_batch_order", batch_order)
    if "raw_payload" in source.columns:
        source = source.withColumn(
            "raw_payload",
            F.to_json(F.col("raw_payload"), options={"ignoreNullFields": "false"}),
        )

    latest = Window.partitionBy("event_id").orderBy(F.col("_batch_order").desc())
    batch = (
        source.withColumn("_row_number", F.row_number().over(latest))
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
        raise ValueError(f"JSON inputs contain no records for {table_name}")

    delta_log = spark._jvm.org.apache.hadoop.fs.Path(f"{output_path.rstrip('/')}/_delta_log")
    filesystem = delta_log.getFileSystem(spark._jsc.hadoopConfiguration())
    if filesystem.exists(delta_log):
        view_name = f"{table_name}_bronze_batch"
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
    bucket = os.getenv("MINIO_BUCKET", "nowcpi")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ROOT_USER", "nowcpi")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "nowcpi-local-password")

    spark = (
        SparkSession.builder.appName("NowCPI-Bronze-CPI-Sources")
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
        spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze LOCATION 's3a://{bucket}/bronze'")
        for table_name, filename in SOURCES.items():
            if args.input and table_name == "cpi":
                input_files = [Path(args.input)]
            else:
                input_files = discover_inputs(filename)
            output_path = (
                args.output
                if table_name == "cpi" and args.output
                else f"s3a://{bucket}/bronze/{table_name}"
            )
            count = upsert_source(spark, input_files, table_name, output_path)
            print(
                f"Upserted {count} {table_name} records from {len(input_files)} JSON file(s) "
                f"into {output_path}"
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
