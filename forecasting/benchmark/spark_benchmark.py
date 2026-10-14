"""Isolated Kafka -> Delta Bronze -> Silver -> Gold benchmark (simplified transformations).

Paths, topic and checkpoint are isolated. This is NOT the production NowCPI ETL.
Audit times are recorded after Delta write returns, as proxies for stage commit
completion; Gold query readiness is not independently checked from Trino.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

TOPIC = os.getenv('BENCH_TOPIC', 'nowcpi.benchmark.events')
BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
BUCKET = os.getenv('MINIO_BUCKET', 'nowcpi')
PREFIX = f's3a://{BUCKET}/benchmark/nowcpi-v1'
CHECKPOINT = '/opt/spark/work-dir/checkpoints/bench-nowcpi-v1'
AUDIT_DIR = Path('/opt/spark/work-dir/benchmark_audit')

schema = T.StructType([
    T.StructField('batch_id', T.StringType()),
    T.StructField('event_id', T.StringType()),
    T.StructField('emitted_at', T.StringType()),
    T.StructField('series_id', T.StringType()),
    T.StructField('value', T.DoubleType()),
    T.StructField('sequence', T.IntegerType()),
])


def now():
    return datetime.now(timezone.utc).isoformat()


def main():
    endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
    spark = (SparkSession.builder.appName('NowCPI-Isolated-Benchmark')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.endpoint', endpoint)
        .config('spark.hadoop.fs.s3a.access.key', os.environ['MINIO_ROOT_USER'])
        .config('spark.hadoop.fs.s3a.secret.key', os.environ['MINIO_ROOT_PASSWORD'])
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
        .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    def process(df, spark_batch_id):
        if df.isEmpty():
            return
        parsed = (df.select(F.from_json(F.col('value').cast('string'), schema).alias('p'))
                    .select('p.*').filter(F.col('event_id').isNotNull()))
        if parsed.isEmpty():
            return
        records = parsed.collect()
        parsed.write.format('delta').mode('append').save(f'{PREFIX}/bronze')
        bronze_ready = now()
        silver = parsed.select('batch_id', 'event_id', 'emitted_at', 'value').filter(F.col('value').isNotNull())
        silver.write.format('delta').mode('append').save(f'{PREFIX}/silver')
        silver_ready = now()
        gold = silver.groupBy('batch_id').agg(F.count('*').alias('event_count'), F.avg('value').alias('avg_value'))
        gold.write.format('delta').mode('append').save(f'{PREFIX}/gold')
        gold_ready = now()
        audit = {'spark_batch_id':spark_batch_id,'bronze_ready_at':bronze_ready,
                 'silver_ready_at':silver_ready,'gold_ready_at':gold_ready,
                 'events':[{'event_id':r['event_id'], 'batch_id':r['batch_id'], 'emitted_at':r['emitted_at']} for r in records]}
        (AUDIT_DIR / f'batch_{spark_batch_id:08d}.json').write_text(json.dumps(audit, indent=2), encoding='utf-8')
        print('BENCH_AUDIT', json.dumps({'spark_batch_id':spark_batch_id,'events':len(records),'gold_ready_at':gold_ready}), flush=True)

    stream = (spark.readStream.format('kafka')
              .option('kafka.bootstrap.servers', BROKER)
              .option('subscribe', TOPIC)
              .option('startingOffsets', 'earliest')
              .load())
    query = (stream.writeStream.foreachBatch(process)
             .option('checkpointLocation', CHECKPOINT)
             .trigger(processingTime='10 seconds').start())
    print(f'BENCH_READY topic={TOPIC} output={PREFIX} checkpoint={CHECKPOINT}', flush=True)
    try:
        query.awaitTermination()
    finally:
        spark.stop()

if __name__ == '__main__':
    main()
