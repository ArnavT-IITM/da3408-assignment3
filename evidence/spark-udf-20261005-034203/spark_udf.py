import json
import os
import statistics
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def speed_mph(distance, pickup, dropoff):
    if distance is None or pickup is None or dropoff is None:
        return None
    seconds = (dropoff - pickup).total_seconds()
    if seconds <= 0 or distance < 0:
        return None
    return round(distance * 3600.0 / seconds, 6)


spark = SparkSession.builder.appName('A3-Spark-UDF').config('spark.sql.session.timeZone', 'UTC').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
try:
    inputs = spark.read.parquet('/opt/spark-data/output/spark-million').select(
        'trip_distance',
        F.to_timestamp('tpep_pickup_datetime', 'yyyy-MM-dd HH:mm:ss.SSSSSS').alias('tpep_pickup_datetime'),
        F.to_timestamp('tpep_dropoff_datetime', 'yyyy-MM-dd HH:mm:ss.SSSSSS').alias('tpep_dropoff_datetime'),
    ).cache()
    rows = inputs.count()
    assert rows == 999636, rows
    trials = []
    for trial in range(3):
        started = time.perf_counter()
        values = inputs.select(F.udf(speed_mph, 'double')(
            'trip_distance', 'tpep_pickup_datetime', 'tpep_dropoff_datetime'
        ).alias('speed_mph')).cache()
        count = values.count()
        elapsed = time.perf_counter() - started
        checksum = values.agg(F.sum('speed_mph')).first()[0]
        assert count == rows
        trials.append({'trial': trial + 1, 'seconds': elapsed, 'rows': count, 'speed_sum': float(checksum)})
        print(json.dumps(trials[-1]), flush=True)
        values.unpersist(blocking=True)
    report = {
        'framework': 'spark', 'version': spark.version, 'input': 'output/spark-million',
        'rows': rows, 'trials': trials, 'median_seconds': statistics.median(t['seconds'] for t in trials),
        'method': 'Speed UDF stage on preloaded timestamp inputs; includes scheduling, conversion and cache materialization/count action. Sum validation is outside timer. Trial 1 includes Python-worker warmup.',
    }
    path = Path('/opt/spark-data/reports') / f"spark-udf-{os.environ['A3_RUN_TAG']}.json"
    path.write_text(json.dumps(report, indent=2) + '\n')
    inputs.unpersist()
    print('DONE: Spark UDF timing saved.', flush=True)
finally:
    spark.stop()
