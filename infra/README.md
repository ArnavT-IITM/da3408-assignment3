# Spark image reference

Imported from https://github.com/sudarsun/docker-spark-cluster at commit
`c546342fd0b942ad2ee87b6a8df77f7e91890d69`. The Dockerfile was adapted locally
from that revision; start-spark.sh retains the upstream implementation.
The submission is packaged as one commit after the experiments. Measurement
provenance comes from saved code hashes and run snapshots.

The existing image `cluster-apache-spark:3.3.4` was reused for all experiments.
The Dockerfile installs pip PySpark without pinning it, so a future rebuild is
not guaranteed to match the measured image. All Spark jobs explicitly invoke
`/opt/spark/bin/spark-submit` (bundled Spark 3.3.4). Image IDs, limits and mount
configuration are preserved in per-run container inspection records.

On another machine, build only if the required image does not already exist:

```bash
docker build -t cluster-apache-spark:3.3.4 infra
```

This build was not rerun during submission packaging. Ray requires the matching
conda environment mounted as documented in the main README.
