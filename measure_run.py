import hashlib
import json
import os
import re
import subprocess
import threading
import time
from pathlib import Path

from disk_guard import USB, check_space

ROOT = Path(__file__).resolve().parent


def memory_bytes(value):
    number, unit = re.fullmatch(r'([\d.]+)([A-Za-z]+)', value.strip()).groups()
    units = {'B': 1, 'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3,
             'kB': 1000, 'MB': 1000**2, 'GB': 1000**3}
    return float(number) * units[unit]


def main():
    framework = os.environ['A3_FRAMEWORK']
    assert framework in {'spark', 'ray'}
    check_space(None)
    device = USB.stat().st_dev
    tag = 'million-measured-' + time.strftime('%Y%m%d-%H%M%S')
    folder = ROOT / 'evidence' / f'{framework}-{tag}'
    folder.mkdir()
    names = [f'a3-{framework}-{suffix}' for suffix in ['master' if framework == 'spark' else 'head', 'worker-a', 'worker-b']]
    inspected = subprocess.check_output(['docker', 'inspect', *names], text=True)
    config = json.loads(inspected)
    assert all(item['State']['Running'] for item in config), 'Start the cluster first.'
    (folder / 'containers.json').write_text(inspected)
    if framework == 'spark':
        names.append('a3-spark-driver')
    command = [item.replace('{tag}', tag).replace('{code_dir}', str(ROOT)) for item in json.loads((ROOT / 'measurement_commands.json').read_text())[framework]]
    sources = [f'{framework}_clean.py', 'measure_run.py', 'measurement_commands.json', 'evidence/million-manifest.json']
    hashes = {}
    for source in sources:
        content = (ROOT / source).read_bytes()
        (folder / Path(source).name).write_bytes(content)
        hashes[source] = hashlib.sha256(content).hexdigest()
    (folder / 'run.json').write_text(json.dumps({'command': command, 'source_sha256': hashes, 'tag': tag}, indent=2) + '\n')
    done = threading.Event()
    samples = []
    errors = []

    def monitor():
        with (folder / 'resources.jsonl').open('w') as log:
            while not done.is_set():
                try:
                    raw = subprocess.check_output(
                        ['docker', 'stats', '--no-stream', '--format', '{{json .}}'],
                        text=True, timeout=12,
                    )
                    rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
                    rows = [row for row in rows if row['Name'] in names]
                    sample = {'unix_time': time.time(), 'containers': rows,
                              'cpu_percent_sum': sum(float(row['CPUPerc'].rstrip('%')) for row in rows),
                              'memory_bytes_sum': sum(memory_bytes(row['MemUsage'].split('/')[0]) for row in rows)}
                    samples.append(sample)
                    log.write(json.dumps(sample) + '\n')
                    log.flush()
                except Exception as error:
                    errors.append(str(error))
                done.wait(1)

    worker = threading.Thread(target=monitor, daemon=True)
    worker.start()
    process = None
    source = (ROOT / 'ray_clean.py').open('rb') if framework == 'ray' else None
    try:
        check_space(device)
        print(f'Measurement evidence: {folder}', flush=True)
        with (folder / 'console.txt').open('w') as log:
            process = subprocess.Popen(command, stdin=source, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in process.stdout:
                print(line, end='', flush=True)
                log.write(line)
                log.flush()
            returncode = process.wait()
    finally:
        done.set()
        worker.join(timeout=15)
        if source:
            source.close()
    if returncode:
        raise SystemExit(f'Run failed with exit code {returncode}; logs preserved in {folder}')
    reports = USB / 'assignment3/data/reports'
    report = json.loads((reports / f'{framework}-{tag}.json').read_text())
    for path in reports.glob(f'{framework}-{tag}*'):
        (folder / path.name).write_bytes(path.read_bytes())
    selected = [sample for sample in samples if report['pipeline_started_unix'] <= sample['unix_time'] <= report['pipeline_finished_unix'] and sample['containers']]
    assert selected, 'No resource samples were collected inside the pipeline interval.'
    summary = {
        'framework': framework, 'tag': tag, 'output_rows': report['output_rows'],
        'ingestion_to_export_seconds': report['ingestion_to_export_seconds'],
        'sampled_peak_cpu_percent': max(sample['cpu_percent_sum'] for sample in selected),
        'sampled_peak_memory_gib': max(sample['memory_bytes_sum'] for sample in selected) / 1024**3,
        'samples_in_pipeline_interval': len(selected), 'monitor_errors': errors,
        'scope': 'sum of framework containers, including head/master and driver',
        'method': 'Docker stats samples; 100% CPU is one core; RAM excludes reclaimable cache as reported by Docker; observed peaks, not continuous maxima',
        'sampling': 'one-second pause between Docker stats calls; actual timestamps saved; calls add latency',
    }
    (folder / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2), flush=True)
    print('DONE: measured run and resource evidence saved.', flush=True)


if __name__ == '__main__':
    main()
