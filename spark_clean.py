import json
import os
import time
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

BASE = Path('/opt/spark-data')
MODE = os.environ.get('A3_MODE', 'sample')
assert MODE in {'sample', 'million', 'full'}
TAG = os.environ.get('A3_RUN_TAG', MODE)
assert TAG.replace('-', '').replace('_', '').isalnum()
TYPES = {
    'VendorID': 'long',
    'tpep_pickup_datetime': 'timestamp',
    'tpep_dropoff_datetime': 'timestamp',
    'passenger_count': 'double',
    'trip_distance': 'double',
    'RatecodeID': 'double',
    'store_and_fwd_flag': 'string',
    'PULocationID': 'long',
    'DOLocationID': 'long',
    'payment_type': 'long',
    'fare_amount': 'double',
    'extra': 'double',
    'mta_tax': 'double',
    'tip_amount': 'double',
    'tolls_amount': 'double',
    'improvement_surcharge': 'double',
    'total_amount': 'double',
    'congestion_surcharge': 'double',
    'airport_fee': 'double',
}


def speed_mph(distance, pickup, dropoff):
    if distance is None or pickup is None or dropoff is None:
        return None
    seconds = (dropoff - pickup).total_seconds()
    if seconds <= 0 or distance < 0:
        return None
    return round(distance * 3600.0 / seconds, 6)


spark = (
    SparkSession.builder.appName(f'A3-Spark-{MODE}')
    .config('spark.sql.session.timeZone', 'UTC')
    .config('spark.sql.shuffle.partitions', '256' if MODE == 'full' else '16')
    .config('spark.sql.autoBroadcastJoinThreshold', '-1')
    .config('spark.sql.adaptive.enabled', 'false')
    .config('spark.sql.constraintPropagation.enabled', 'false')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')
try:
    pipeline_started_unix = time.time()
    started = time.perf_counter()
    input_dir = BASE / {'sample': 'sample_raw', 'million': 'million_raw', 'full': 'raw'}[MODE]
    paths = sorted(input_dir.glob('*.parquet'))
    assert len(paths) == (36 if MODE == 'full' else 2), paths
    frames = []
    for path in paths:
        frame = spark.read.parquet(str(path))
        frames.append(frame.select(*[
            F.col('Airport_fee' if name == 'airport_fee' and 'Airport_fee' in frame.columns else name)
            .cast(dtype).alias(name)
            for name, dtype in TYPES.items()
        ]))
    trips = reduce(lambda left, right: left.unionByName(right), frames)
    trips = trips.na.drop(subset=list(TYPES)).dropDuplicates(list(TYPES))
    trips = trips.filter(
        (F.col('tpep_dropoff_datetime') > F.col('tpep_pickup_datetime'))
        & (F.col('trip_distance') >= 0)
    )
    zones = (
        spark.read.option('header', True)
        .schema('LocationID long, Borough string, Zone string, service_zone string')
        .csv(str(BASE / 'lookup/taxi_zone_lookup.csv'))
        .select(
            F.col('LocationID').alias('PULocationID'),
            F.col('Borough').alias('pickup_borough'),
            F.col('Zone').alias('pickup_zone'),
            F.col('service_zone').alias('pickup_service_zone'),
        )
    )
    result = trips.join(zones.hint('merge'), 'PULocationID', 'inner')
    result = result.withColumn('speed_mph', F.udf(speed_mph, 'double')(
        'trip_distance', 'tpep_pickup_datetime', 'tpep_dropoff_datetime'
    )).withColumn('pickup_hour', F.hour('tpep_pickup_datetime').cast('long'))
    for name in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
        result = result.withColumn(name, F.date_format(name, 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
    result = result.select(*TYPES, 'pickup_borough', 'pickup_zone', 'pickup_service_zone', 'speed_mph', 'pickup_hour')
    plan = result._jdf.queryExecution().executedPlan().toString()
    assert 'SortMergeJoin' in plan, plan
    output = BASE / f'output/spark-{TAG}'
    result.write.mode('overwrite').parquet(str(output))
    elapsed = time.perf_counter() - started
    pipeline_finished_unix = time.time()
    saved = spark.read.parquet(str(output))
    report = {
        'framework': 'spark', 'version': spark.version, 'mode': MODE,
        'input_files': [path.name for path in paths], 'output': str(output),
        'output_rows': saved.count(), 'ingestion_to_export_seconds': elapsed,
        'pipeline_started_unix': pipeline_started_unix, 'pipeline_finished_unix': pipeline_finished_unix,
        'schema': json.loads(saved.schema.json()),
        'purpose': 'full-data run' if MODE == 'full' else 'development check',
    }
    reports = BASE / 'reports'
    reports.mkdir(exist_ok=True)
    (reports / f'spark-{TAG}.json').write_text(json.dumps(report, indent=2) + '\n')
    (reports / f'spark-{TAG}-plan.txt').write_text(plan + '\n')
    print(json.dumps(report, indent=2), flush=True)
    print(f'DONE: Spark {MODE} pipeline exported {report["output_rows"]:,} rows.', flush=True)
finally:
    spark.stop()
