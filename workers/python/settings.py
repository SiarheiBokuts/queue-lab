"""Everything a worker reads from its environment, parsed once at start-up.

The environment is the "worker contract" defined in the backend (backend/app/contract.py). Reading it all here keeps
the rest of the worker free of `os.environ`. Kafka client tuning (batching, acks, timeouts) is NOT decided here: the
backend sends the same values to the Python and the Node.js workers (PRODUCER_CONF / CONSUMER_CONF), so comparisons
between the two languages are fair.
"""

import json
import os

from lib import parse_edges

ROLE = os.environ["ROLE"]  # "producer" or "consumer"
BOOT = os.environ["KAFKA_BOOTSTRAP"]  # broker address
TOPIC = os.environ["TOPIC"]  # the queue
RUN = os.environ["RUN_ID"]  # id of this run; messages of other runs are ignored
WID = os.environ.get("WORKER_ID", "worker")  # container name, identifies this worker in reports
REPORT = os.environ["REPORT_URL"]  # where the once-per-second report is POSTed
# Latency histogram: instead of sending every latency we count messages per bucket. EDGES are the upper edge of each
# bucket in milliseconds (for example 0.5, 1, 2, 5 ... 10000); the backend sends them so they are defined only once.
EDGES = parse_edges(os.environ["HIST_EDGES"])
CONCURRENCY = int(os.environ.get("CONCURRENCY", "1") or 1)  # messages one worker handles at the same time = lanes (consumer only)
ORDERING = os.environ.get("ORDERING", "key")  # "key": one user stays in one lane (in order); "none": any lane (consumer only)


def kafka_conf(variable: str) -> dict:
    """Kafka client properties (librdkafka names) sent by the backend as JSON, e.g. {"property.name": value}."""
    return json.loads(os.environ[variable])
