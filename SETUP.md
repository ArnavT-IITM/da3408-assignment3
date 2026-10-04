# Manual cluster setup

Run commands individually, in order, only on a fresh setup. Skip creation
steps if the named containers/network already exist. These commands were
executed manually during the assignment; this file is a final reference.

Prerequisites: Docker, mounted USB, the existing Spark image, and the Ray conda
environment described in README.md. All paths below are from the measured machine.
The image-build reference and its limitations are in infra/README.md.

Create the USB shared-data directory. Ensure the USB is mounted first.

```bash
mkdir -p /run/media/arnav/ESD-USB/assignment3/data
```

Create the assignment Docker network.

```bash
docker network create a3-network
```

Create and start the Spark master.

```bash
docker run -d --name a3-spark-master --hostname a3-spark-master --network a3-network --memory 512m --cpus 1 --log-opt max-size=10m --log-opt max-file=2 -p 127.0.0.1:9090:8080 --mount type=bind,source=/run/media/arnav/ESD-USB/assignment3/data,target=/opt/spark-data -e SPARK_WORKLOAD=master cluster-apache-spark:3.3.4
```

Create and start Spark worker A.

```bash
docker run -d --name a3-spark-worker-a --hostname a3-spark-worker-a --network a3-network --memory 2g --cpus 1 --log-opt max-size=10m --log-opt max-file=2 -p 127.0.0.1:9091:8080 --mount type=bind,source=/run/media/arnav/ESD-USB/assignment3/data,target=/opt/spark-data -e SPARK_WORKLOAD=worker -e SPARK_MASTER=spark://a3-spark-master:7077 -e SPARK_WORKER_CORES=1 -e SPARK_WORKER_MEMORY=1G cluster-apache-spark:3.3.4
```

Create and start Spark worker B.

```bash
docker run -d --name a3-spark-worker-b --hostname a3-spark-worker-b --network a3-network --memory 2g --cpus 1 --log-opt max-size=10m --log-opt max-file=2 -p 127.0.0.1:9092:8080 --mount type=bind,source=/run/media/arnav/ESD-USB/assignment3/data,target=/opt/spark-data -e SPARK_WORKLOAD=worker -e SPARK_MASTER=spark://a3-spark-master:7077 -e SPARK_WORKER_CORES=1 -e SPARK_WORKER_MEMORY=1G cluster-apache-spark:3.3.4
```

Configure Spark worker A to put scratch files and executor work/logs on USB.

```bash
docker exec a3-spark-worker-a sh -c 'mkdir -p /opt/spark-data/spark-scratch/worker-a /opt/spark-data/spark-work/worker-a && printf "\n%s\n" "export SPARK_LOCAL_DIRS=/opt/spark-data/spark-scratch/worker-a" "export SPARK_WORKER_DIR=/opt/spark-data/spark-work/worker-a" >> /opt/spark/conf/spark-env.sh'
```

Configure Spark worker B to put scratch files and executor work/logs on USB.

```bash
docker exec a3-spark-worker-b sh -c 'mkdir -p /opt/spark-data/spark-scratch/worker-b /opt/spark-data/spark-work/worker-b && printf "\n%s\n" "export SPARK_LOCAL_DIRS=/opt/spark-data/spark-scratch/worker-b" "export SPARK_WORKER_DIR=/opt/spark-data/spark-work/worker-b" >> /opt/spark/conf/spark-env.sh'
```

Restart both Spark workers to apply their USB scratch and work-directory settings.

```bash
docker restart a3-spark-worker-a a3-spark-worker-b
```

Stop the assignment Spark containers before preparing Ray.

```bash
docker stop a3-spark-worker-a a3-spark-worker-b a3-spark-master
```

Create separate USB spill directories for the Ray head and two workers.

```bash
mkdir -p /run/media/arnav/ESD-USB/assignment3/ray-spill/{head,worker-a,worker-b}
```

Start the final 4 GiB Ray head. Wait for its dashboard before starting workers.

```bash
docker run -d --name a3-ray-head --hostname a3-ray-head --network a3-network --memory 4g --memory-swap 4g --cpus 1 --shm-size 256m --tmpfs /tmp:rw,size=256m --log-opt max-size=10m --log-opt max-file=2 -p 127.0.0.1:8265:8265 --mount type=bind,source=/home/arnav/miniforge3,target=/home/arnav/miniforge3,readonly --mount type=bind,source=/run/media/arnav/ESD-USB/assignment3/data,target=/opt/spark-data --mount type=bind,source=/run/media/arnav/ESD-USB/assignment3/ray-spill/head,target=/ray-spill -e OPENBLAS_NUM_THREADS=1 --entrypoint /home/arnav/miniforge3/envs/aiops/bin/ray cluster-apache-spark:3.3.4 start --head --port=6379 --num-cpus=0 --num-gpus=0 --object-store-memory=134217728 --object-spilling-directory=/ray-spill --include-dashboard=true --dashboard-host=0.0.0.0 --dashboard-port=8265 --disable-usage-stats --block
```

Start Ray worker A, connecting to the existing head.

```bash
docker run -d --name a3-ray-worker-a --hostname a3-ray-worker-a --network a3-network --memory 2g --memory-swap 2g --cpus 1 --shm-size 256m --tmpfs /tmp:rw,size=256m --log-opt max-size=10m --log-opt max-file=2 --mount type=bind,source=/home/arnav/miniforge3,target=/home/arnav/miniforge3,readonly --mount type=bind,source=/run/media/arnav/ESD-USB/assignment3/data,target=/opt/spark-data --mount type=bind,source=/run/media/arnav/ESD-USB/assignment3/ray-spill/worker-a,target=/ray-spill -e OPENBLAS_NUM_THREADS=1 --entrypoint /home/arnav/miniforge3/envs/aiops/bin/ray cluster-apache-spark:3.3.4 start --address=a3-ray-head:6379 --num-cpus=1 --num-gpus=0 --object-store-memory=134217728 --object-spilling-directory=/ray-spill --disable-usage-stats --block
```

Start Ray worker B with the same limits as worker A.

```bash
docker run -d --name a3-ray-worker-b --hostname a3-ray-worker-b --network a3-network --memory 2g --memory-swap 2g --cpus 1 --shm-size 256m --tmpfs /tmp:rw,size=256m --log-opt max-size=10m --log-opt max-file=2 --mount type=bind,source=/home/arnav/miniforge3,target=/home/arnav/miniforge3,readonly --mount type=bind,source=/run/media/arnav/ESD-USB/assignment3/data,target=/opt/spark-data --mount type=bind,source=/run/media/arnav/ESD-USB/assignment3/ray-spill/worker-b,target=/ray-spill -e OPENBLAS_NUM_THREADS=1 --entrypoint /home/arnav/miniforge3/envs/aiops/bin/ray cluster-apache-spark:3.3.4 start --address=a3-ray-head:6379 --num-cpus=1 --num-gpus=0 --object-store-memory=134217728 --object-spilling-directory=/ray-spill --disable-usage-stats --block
```

After setup, stop Ray before starting Spark for a measurement.
Start the disk guard from the repository root and leave it active in another terminal:

```bash
/home/arnav/miniforge3/envs/aiops/bin/python disk_guard.py
```

Spark readiness: http://localhost:9090 must show two ALIVE workers.
Ray readiness: http://localhost:8265/#/cluster must show three ALIVE nodes.
Ray head startup can take about 30 seconds. Use `docker start` and `docker stop` with the existing container names;
do not recreate the cluster for each run.

The Docker bridge simulates separate nodes on one laptop, not separate physical machines.
