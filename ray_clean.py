import json
import os
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import ray
from ray.data import DataContext
from ray.data.aggregate import Count

BASE = Path('/opt/spark-data')
MODE = os.environ.get('A3_MODE', 'sample')
assert MODE in {'sample', 'million', 'full'}
TAG = os.environ.get('A3_RUN_TAG', MODE)
assert TAG.replace('-', '').replace('_', '').isalnum()
SCHEMA = pa.schema([
    ('VendorID', pa.int64()),
    ('tpep_pickup_datetime', pa.timestamp('us')),
    ('tpep_dropoff_datetime', pa.timestamp('us')),
    ('passenger_count', pa.float64()),
    ('trip_distance', pa.float64()),
    ('RatecodeID', pa.float64()),
    ('store_and_fwd_flag', pa.string()),
    ('PULocationID', pa.int64()),
    ('DOLocationID', pa.int64()),
    ('payment_type', pa.int64()),
    ('fare_amount', pa.float64()),
    ('extra', pa.float64()),
    ('mta_tax', pa.float64()),
    ('tip_amount', pa.float64()),
    ('tolls_amount', pa.float64()),
    ('improvement_surcharge', pa.float64()),
    ('total_amount', pa.float64()),
    ('congestion_surcharge', pa.float64()),
    ('airport_fee', pa.float64()),
])
OUTPUT_SCHEMA = pa.schema([
    (field.name, pa.string() if pa.types.is_timestamp(field.type) else field.type)
    for field in SCHEMA
] + [
    ('pickup_borough', pa.string()), ('pickup_zone', pa.string()),
    ('pickup_service_zone', pa.string()), ('speed_mph', pa.float64()),
    ('pickup_hour', pa.int64()),
])


def speed_mph(distance, pickup, dropoff):
    if distance is None or pickup is None or dropoff is None:
        return None
    seconds = (dropoff - pickup).total_seconds()
    if seconds <= 0 or distance < 0:
        return None
    return round(distance * 3600.0 / seconds, 6)


def normalize(batch):
    batch = batch.rename_columns([
        'airport_fee' if name == 'Airport_fee' else name for name in batch.column_names
    ]).select(SCHEMA.names).cast(SCHEMA)
    valid = pc.is_valid(batch[SCHEMA.names[0]])
    for field in SCHEMA:
        valid = pc.and_kleene(valid, pc.is_valid(batch[field.name]))
        if pa.types.is_floating(field.type):
            valid = pc.and_kleene(valid, pc.invert(pc.is_nan(batch[field.name])))
    valid = pc.and_kleene(valid, pc.greater(batch['tpep_dropoff_datetime'], batch['tpep_pickup_datetime']))
    valid = pc.and_kleene(valid, pc.greater_equal(batch['trip_distance'], 0))
    return batch.filter(valid)


def features(batch):
    frame = batch.to_pandas()
    frame['speed_mph'] = [
        speed_mph(float(distance), pickup.to_pydatetime(), dropoff.to_pydatetime())
        for distance, pickup, dropoff in zip(
            frame.trip_distance, frame.tpep_pickup_datetime, frame.tpep_dropoff_datetime
        )
    ]
    frame['pickup_hour'] = frame.tpep_pickup_datetime.dt.hour.astype('int64')
    for name in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
        frame[name] = frame[name].dt.strftime('%Y-%m-%d %H:%M:%S.%f')
    return pa.Table.from_pandas(frame[OUTPUT_SCHEMA.names], schema=OUTPUT_SCHEMA, preserve_index=False)


def main():
    ray.init(address='auto')
    try:
        context = DataContext.get_current()
        context.target_max_block_size = 16 * 1024 * 1024
        context.enable_progress_bars = False
        context.max_hash_shuffle_aggregators = 2
        context.hash_aggregate_operator_actor_num_cpus_override = 0
        pipeline_started_unix = time.time()
        started = time.perf_counter()
        input_dir = BASE / {'sample': 'sample_raw', 'million': 'million_raw', 'full': 'raw'}[MODE]
        paths = sorted(input_dir.glob('*.parquet'))
        assert len(paths) == (36 if MODE == 'full' else 2), paths
        frames = [
            ray.data.read_parquet(str(path), num_cpus=1, concurrency=2, memory=256 * 1024 * 1024)
            .map_batches(normalize, batch_format='pyarrow', batch_size=8192, num_cpus=1)
            for path in paths
        ]
        trips = frames[0].union(*frames[1:])
        partitions = 256 if MODE == 'full' else 16
        trips = trips.groupby(SCHEMA.names, num_partitions=partitions).aggregate(
            Count(alias_name='_copies')
        ).drop_columns(['_copies'])
        zones = ray.data.read_csv(
            str(BASE / 'lookup/taxi_zone_lookup.csv'), num_cpus=1, concurrency=1,
            convert_options=pacsv.ConvertOptions(column_types={
                'LocationID': pa.int64(), 'Borough': pa.string(),
                'Zone': pa.string(), 'service_zone': pa.string(),
            }, strings_can_be_null=False),
        ).rename_columns({
            'LocationID': 'PULocationID', 'Borough': 'pickup_borough',
            'Zone': 'pickup_zone', 'service_zone': 'pickup_service_zone',
        })
        result = trips.join(
            zones, join_type='inner', num_partitions=partitions, on=('PULocationID',),
            aggregator_ray_remote_args={'num_cpus': 0},
        )
        result = result.map_batches(features, batch_format='pyarrow', batch_size=8192, num_cpus=1)
        output = BASE / f'output/ray-{TAG}'
        result.write_parquet(str(output), mode='overwrite', concurrency=2)
        elapsed = time.perf_counter() - started
        pipeline_finished_unix = time.time()
        files = sorted(output.glob('*.parquet'))
        assert files, output
        report = {
            'framework': 'ray', 'version': ray.__version__, 'mode': MODE,
            'input_files': [path.name for path in paths], 'output': str(output),
            'output_rows': sum(pq.ParquetFile(path).metadata.num_rows for path in files),
            'ingestion_to_export_seconds': elapsed,
            'pipeline_started_unix': pipeline_started_unix, 'pipeline_finished_unix': pipeline_finished_unix,
            'schema': str(pq.ParquetFile(files[0]).schema_arrow),
            'purpose': 'full-data run' if MODE == 'full' else 'development check',
        }
        reports = BASE / 'reports'
        reports.mkdir(exist_ok=True)
        (reports / f'ray-{TAG}.json').write_text(json.dumps(report, indent=2) + '\n')
        (reports / f'ray-{TAG}-stats.txt').write_text(result.stats() + '\n')
        print(json.dumps(report, indent=2), flush=True)
        print(f'DONE: Ray {MODE} pipeline exported {report["output_rows"]:,} rows.', flush=True)
    finally:
        ray.shutdown()


if __name__ == '__main__':
    main()
