import json
import os
import statistics
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import ray


def speed_mph(distance, pickup, dropoff):
    if distance is None or pickup is None or dropoff is None:
        return None
    seconds = (dropoff - pickup).total_seconds()
    if seconds <= 0 or distance < 0:
        return None
    return round(distance * 3600.0 / seconds, 6)


def timestamps(batch):
    for name in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
        batch = batch.set_column(batch.schema.get_field_index(name), name, pc.cast(batch[name], pa.timestamp('us')))
    return batch


def compute_speed(batch):
    return pa.table({'speed_mph': [speed_mph(distance, pickup, dropoff) for distance, pickup, dropoff in zip(
        batch['trip_distance'].to_pylist(), batch['tpep_pickup_datetime'].to_pylist(),
        batch['tpep_dropoff_datetime'].to_pylist(),
    )]})


ray.init(address='auto')
try:
    context = ray.data.DataContext.get_current()
    context.target_max_block_size = 16 * 1024 * 1024
    context.enable_progress_bars = False
    inputs = ray.data.read_parquet(
        '/opt/spark-data/output/spark-million',
        columns=['trip_distance', 'tpep_pickup_datetime', 'tpep_dropoff_datetime'],
        num_cpus=1, concurrency=2, memory=256 * 1024 * 1024,
    ).map_batches(timestamps, batch_format='pyarrow', batch_size=8192, num_cpus=1).materialize()
    rows = inputs.count()
    assert rows == 999636, rows
    trials = []
    for trial in range(3):
        started = time.perf_counter()
        values = inputs.map_batches(compute_speed, batch_format='pyarrow', batch_size=8192, num_cpus=1).materialize()
        elapsed = time.perf_counter() - started
        count = values.count()
        checksum = values.sum('speed_mph')
        assert count == rows
        trials.append({'trial': trial + 1, 'seconds': elapsed, 'rows': count, 'speed_sum': float(checksum)})
        print(json.dumps(trials[-1]), flush=True)
        del values
    report = {
        'framework': 'ray', 'version': ray.__version__, 'input': 'output/spark-million',
        'rows': rows, 'trials': trials, 'median_seconds': statistics.median(t['seconds'] for t in trials),
        'method': 'Speed UDF stage on preloaded timestamp inputs; includes scheduling, conversion and materialization. Validation count/sum is outside timer. Trial 1 includes Python-worker warmup.',
    }
    path = Path('/opt/spark-data/reports') / f"ray-udf-{os.environ['A3_RUN_TAG']}.json"
    path.write_text(json.dumps(report, indent=2) + '\n')
    print('DONE: Ray UDF timing saved.', flush=True)
finally:
    ray.shutdown()
