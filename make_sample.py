import hashlib
import json
import os
from pathlib import Path

import pyarrow.parquet as pq

from disk_guard import USB, check_space

check_space(None)
device = USB.stat().st_dev
data = USB / 'assignment3/data'
mode = os.environ.get('A3_MODE', 'sample')
assert mode in {'sample', 'million'}
rows_per_file = 500000 if mode == 'million' else 100000
output = data / ('million_raw' if mode == 'million' else 'sample_raw')
output.mkdir(exist_ok=True)
records = []
for name in ['yellow_tripdata_2022-01.parquet', 'yellow_tripdata_2024-01.parquet']:
    source = pq.ParquetFile(data / 'raw' / name)
    target = output / name
    partial = target.with_suffix('.parquet.part')
    rows = 0
    with pq.ParquetWriter(partial, source.schema_arrow) as writer:
        for batch in source.iter_batches(batch_size=10000):
            check_space(device)
            batch = batch.slice(0, min(len(batch), rows_per_file - rows))
            writer.write_batch(batch)
            rows += len(batch)
            if rows == rows_per_file:
                break
    check_space(device)
    assert rows == rows_per_file, name
    partial.replace(target)
    digest = hashlib.sha256()
    with target.open('rb') as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    records.append({
        'source': str(data / 'raw' / name), 'path': str(target),
        'selection': f'first {rows_per_file} rows in file order', 'rows': rows,
        'bytes': target.stat().st_size, 'sha256': digest.hexdigest(),
    })
    print(f'{name}: {rows:,} rows', flush=True)
manifest = {'purpose': 'pipeline development and parity checks, not full-data benchmarking', 'files': records}
(Path(__file__).resolve().parent / f'evidence/{mode}-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(f'DONE: {rows_per_file * 2:,} real trips across both source schema variants. Original files unchanged.')
