# Assignment 3: Spark vs. Ray

Two-worker Spark and Ray Data clusters run the same NYC taxi preprocessing
pipeline. The [five-page report](writeup.pdf) contains the methods, results,
performance tuning note and AI-first versus BI-first discussion.

## Scope and results

The measured input is **1,000,000 rows (22,408,153 compressed bytes)**: the first
500,000 records from each of two source months. This deterministic subset
**does not meet the brief's approximately 2 GB input scale**. The full download
contains 36 months, 119,136,044 rows and 1,943,844,994 bytes, but was not benchmarked.

| Metric | Spark 3.3.4 | Ray 2.58.0 |
|---|---:|---:|
| Ingestion through export | 132.79 s | 319.17 s |
| Sampled peak CPU, summed containers | 2.12 cores | 3.06 cores |
| Sampled peak RAM, summed containers | 1.89 GiB | 5.34 GiB |
| Separate UDF stage, median of three trials | 5.64 s | 3.02 s |
| Output rows | 999,636 | 999,636 |

Exact comparisons passed for all 24 columns, values, data types and row
multiplicities. Spark was faster for this complete pipeline; Ray was faster
for the isolated Python UDF stage. Resource measurements use Docker stats
rather than top or Prometheus/Grafana. Sampling, runtime allocation differences,
cache effects and UDF timing boundaries are disclosed in the report.

The supplementary Ray run `035625` (run-folder suffix) took 367.26 s,
with sampled peaks of 3.06 cores and 5.34 GiB RAM. Its output also matches Spark.
The fresh Jobs and Cluster screenshots belong to this run. The dashboard API
confirms job `01000000`, driver PID `876`, succeeded. This supplementary run
is reported separately from the original comparison.

The saved Ray operator statistics identify duplicate removal as the dominant
recorded cost: **445.18 s summed remote wall time**, versus **13.57 s for the
join**, **6.57 s for features** and **9.02 s for writing**. Parallel task totals
overlap and cannot be added to reconstruct the 319.17 s elapsed time. The features
stage includes timestamp formatting and pickup hour as well as speed. See
`evidence/ray-operator-breakdown.json` for values extracted from the original log.

The full-width report screenshots use the measured 4 GiB Ray head configuration.
Spark screenshot job times match measured run
032053's console start (21:50:56 UTC). The cropped screenshot omits its URL bar.

## Code and reproduction

| Files | Purpose |
|---|---|
| `spark_clean.py`, `ray_clean.py` | Identical cleaning, global deduplication, pickup-zone join, speed/hour features and Parquet export |
| `spark_udf.py`, `ray_udf.py`, `run_udf.py` | Three UDF trials on common preloaded input, with output count/checksum validation |
| `measure_run.py`, `measurement_commands.json` | Launch jobs and save wall time, sampled CPU/RAM, logs and configuration |
| `download_data.py`, `dataset_plan.json`, `make_sample.py` | Retrieve real source files and construct the measured subset |
| `disk_guard.py` | Monitor the USB mount and free space during execution |
| `validate_measured_parity.py` | Reproduce the exact comparison of the reported measured outputs |
| `build_results.py`, `figures/` | Regenerate report graphs and tables from recorded measurements |
| `SETUP.md`, `infra/`, `requirements.txt` | Manual cluster setup, image provenance and environment dependencies |

Run commands from this repository's root. Follow [SETUP.md](SETUP.md) for manual
cluster configuration. The measured machine used an existing
`cluster-apache-spark:3.3.4` image and a read-only mounted Ray environment at
`/home/arnav/miniforge3/envs/aiops`. The image contains bundled Spark 3.3.4;
launch it through `/opt/spark/bin/spark-submit`. See [image provenance](infra/README.md)
for rebuild limitations.

USB `/run/media/arnav/ESD-USB/assignment3/data` is mounted at `/opt/spark-data`.
Inputs, outputs, Spark scratch files and Ray spills stay on USB. Adapt USB and
conda paths on another machine. Launchers substitute the repository's actual
location for `{code_dir}` in the command template.

1. Run `disk_guard.py` in a separate terminal throughout the experiment.
2. Use `download_data.py`, then `A3_MODE=million` with `make_sample.py`, to prepare inputs.
3. Start only the framework being measured and verify both workers are alive.
4. Run the relevant measurement command below. New outputs and evidence use unique timestamps.
5. Run `run_udf.py` with the matching `A3_FRAMEWORK`. Both UDF scripts use the
   verified `output/spark-million` development output. To recreate that common
   input, run `spark_clean.py` with `A3_MODE=million` and no `A3_RUN_TAG`, using
   the Spark launch arguments in `measurement_commands.json`.
6. `validate_measured_parity.py` and `build_results.py` pin the reported run IDs;
   update those IDs deliberately when validating a new experiment.

```bash
A3_FRAMEWORK=spark /home/arnav/miniforge3/envs/aiops/bin/python measure_run.py
```

```bash
A3_FRAMEWORK=ray /home/arnav/miniforge3/envs/aiops/bin/python measure_run.py
```

Regenerate figures from included evidence without rerunning jobs:

```bash
MPLCONFIGDIR=/tmp/a3-matplotlib /home/arnav/miniforge3/envs/aiops/bin/python build_results.py
```

## Evidence

- `screenshots/`: Spark master and Ray cluster, each with two workers.
- `evidence/spark-million-application-running.png`: live Spark application.
- `evidence/*-million-measured-*/`: measured runs, raw resource traces, exact
  source snapshots, configuration, logs and summaries. The `035625` Ray run also
  contains fresh Jobs/Cluster screenshots, successful job status and parity proof.
- `evidence/*-udf-*/`: UDF trials and their recorded code.
- `evidence/measured-million-parity.json`, `udf-validation.json`: correctness checks.
- `evidence/*manifest.json`, `input-inventory.json`: input provenance and scale.
- `evidence/benchmark-results.json` and `.csv`: combined reported results.

Source snapshots are intentional: they establish which code produced each
measurement. The Git repository was initialized after the experiments, so commit
history does not establish measurement provenance. After the runs, the launchers
parameterized the code bind path as `{code_dir}` so they work from the submission
folder. Recorded launch snapshots retain the original `ass3` path; the measured
pipeline and UDF scripts match their saved source hashes. Large data files are excluded.

See [AI_DISCLOSURE.md](AI_DISCLOSURE.md) for the disclosure. Configuration changes
are documented in the report’s Performance Tuning Note. Real input data comes from
[NYC TLC trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
