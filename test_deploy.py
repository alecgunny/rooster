import logging
import socket
import sys
import time
from concurrent.futures import as_completed

import ray

from rooster.ray import deploy_ray_cluster

logging.basicConfig(
    stream=sys.stdout,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    level=logging.DEBUG,
)


def compute(x, log_dir):
    node_id = ray.get_runtime_context().get_node_id()
    logging.basicConfig(
        filename=f"{log_dir}/worker-{node_id}.log",
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        level=logging.INFO,
    )

    time.sleep(0.5)
    hostname = socket.gethostname()
    logging.info(f"Hello from {hostname}")
    return x**2 - x + 1


cluster = deploy_ray_cluster(
    "test-deploy", num_clients=4, log_dir=".", output_dir=".", error_dir="."
)
with cluster as cluster:
    cluster.connect()
    fn = ray.remote(num_cpus=1)(compute)

    refs = [fn.remote(i, ".") for i in range(1, 100)]
    futures = [i.future() for i in refs]
    for f in as_completed(futures):
        logging.info(f.result())
