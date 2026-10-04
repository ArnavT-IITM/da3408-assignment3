import hashlib
import json
import os
import subprocess
import time
from pathlib import Path

from disk_guard import USB, check_space

root = Path(__file__).resolve().parent
framework = os.environ['A3_FRAMEWORK']
assert framework in {'spark', 'ray'}
check_space(None)
tag = time.strftime('%Y%m%d-%H%M%S')
folder = root / 'evidence' / f'{framework}-udf-{tag}'
folder.mkdir()
command = [item.replace('{tag}', tag).replace('{code_dir}', str(root)).replace('/opt/assignment/spark_clean.py', '/opt/assignment/spark_udf.py')
           for item in json.loads((root / 'measurement_commands.json').read_text())[framework]]
content = (root / f'{framework}_udf.py').read_bytes()
(folder / f'{framework}_udf.py').write_bytes(content)
(folder / 'run.json').write_text(json.dumps({'command': command, 'code_sha256': hashlib.sha256(content).hexdigest()}, indent=2) + '\n')
print(f'UDF evidence: {folder}', flush=True)
source = (root / 'ray_udf.py').open('rb') if framework == 'ray' else None
try:
    with (folder / 'console.txt').open('w') as log:
        process = subprocess.Popen(command, stdin=source, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            print(line, end='', flush=True)
            log.write(line)
            log.flush()
        returncode = process.wait()
finally:
    if source:
        source.close()
if returncode:
    raise SystemExit(f'UDF run failed: exit {returncode}; log preserved in {folder}')
report = USB / f'assignment3/data/reports/{framework}-udf-{tag}.json'
(folder / 'summary.json').write_bytes(report.read_bytes())
print(report.read_text(), flush=True)
