import csv
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
EVIDENCE = ROOT / 'evidence'
FIGURES = ROOT / 'figures'
FIGURES.mkdir(exist_ok=True)
RUNS = {
    'spark': 'spark-million-measured-20261005-032053',
    'ray': 'ray-million-measured-20261005-032749',
}
UDFS = {'spark': 'spark-udf-20261005-034203', 'ray': 'ray-udf-20261005-033548'}
expected = json.loads((EVIDENCE / 'udf-validation.json').read_text())['expected_speed_sum']
results = {}
for name in RUNS:
    measured = json.loads((EVIDENCE / RUNS[name] / 'summary.json').read_text())
    udf = json.loads((EVIDENCE / UDFS[name] / 'summary.json').read_text())
    assert not measured['monitor_errors']
    assert measured['output_rows'] == udf['rows'] == 999636
    for trial in udf['trials']:
        assert trial['rows'] == 999636
        assert math.isclose(trial['speed_sum'], expected, rel_tol=0, abs_tol=1e-6)
    results[name] = {'pipeline': measured, 'udf': udf, 'run_directory': RUNS[name], 'udf_directory': UDFS[name]}
(EVIDENCE / 'udf-validation.json').write_text(json.dumps({
    'result': 'PASS', 'expected_speed_sum': expected, 'sum_absolute_tolerance': 1e-6,
    'rows_per_trial': 999636, 'trials_checked': 6,
    'scope': 'row counts and speed sums; full pipeline exact parity is recorded separately',
}, indent=2) + '\n')
(EVIDENCE / 'benchmark-results.json').write_text(json.dumps(results, indent=2) + '\n')
with (EVIDENCE / 'benchmark-results.csv').open('w') as file:
    writer = csv.writer(file)
    writer.writerow(['framework', 'wall_seconds', 'sampled_peak_cpu_cores', 'sampled_peak_memory_gib', 'udf_median_seconds'])
    for name, result in results.items():
        row = result['pipeline']
        writer.writerow([name, row['ingestion_to_export_seconds'], row['sampled_peak_cpu_percent'] / 100,
                         row['sampled_peak_memory_gib'], result['udf']['median_seconds']])
plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False})
colors = ['#247ba0', '#df7734']
names = ['spark', 'ray']
labels = ['Spark 3.3.4', 'Ray 2.58.0']
metrics = [
    ('Ingestion to export', 'Seconds', [results[n]['pipeline']['ingestion_to_export_seconds'] for n in names]),
    ('Sampled peak CPU', 'Core equivalents', [results[n]['pipeline']['sampled_peak_cpu_percent'] / 100 for n in names]),
    ('Sampled peak memory', 'GiB', [results[n]['pipeline']['sampled_peak_memory_gib'] for n in names]),
    ('Separate UDF stage', 'Median seconds, 3 trials', [results[n]['udf']['median_seconds'] for n in names]),
]
fig, axes = plt.subplots(2, 2, figsize=(9, 5.8), layout='constrained')
for ax, (title, unit, values) in zip(axes.flat, metrics):
    bars = ax.bar(labels, values, color=colors, width=0.5)
    ax.bar_label(bars, labels=[f'{v:.2f}' for v in values], padding=3)
    ax.set(title=title, ylabel=unit, ylim=(0, max(values) * 1.23))
    ax.set_axisbelow(True)
    ax.grid(axis='y', alpha=0.2)
fig.suptitle('One-million-row subset: observed results', fontsize=13)
fig.savefig(FIGURES / 'benchmark-comparison.pdf')
plt.close(fig)
fig, axes = plt.subplots(2, 1, figsize=(9, 4.7), layout='constrained', sharex=True)
for name, color, label in zip(names, colors, labels):
    result = results[name]['pipeline']
    folder = EVIDENCE / RUNS[name]
    report = json.loads((folder / f"{name}-{result['tag']}.json").read_text())
    start, end = report['pipeline_started_unix'], report['pipeline_finished_unix']
    samples = [json.loads(line) for line in (folder / 'resources.jsonl').read_text().splitlines()]
    samples = [s for s in samples if start <= s['unix_time'] <= end]
    xs = [s['unix_time'] - start for s in samples]
    axes[0].plot(xs, [s['cpu_percent_sum'] / 100 for s in samples], color=color, label=label)
    axes[1].plot(xs, [s['memory_bytes_sum'] / 1024**3 for s in samples], color=color, label=label)
axes[0].set(ylabel='CPU core equivalents', ylim=(0, None))
axes[1].set(ylabel='Memory (GiB)', xlabel='Seconds since pipeline start', ylim=(0, None))
for ax in axes:
    ax.grid(alpha=0.2)
    ax.legend(loc='upper right')
fig.savefig(FIGURES / 'resource-timeline.pdf')
plt.close(fig)
print('PASS: six UDF checksums; benchmark tables and graphs generated from saved measurements.')
