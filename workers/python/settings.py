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
# The batch size: messages one worker reads and handles at the same time (consumer only).
CONCURRENCY = int(os.environ.get("CONCURRENCY", "1") or 1)
BATCH_WAIT_MS = int(os.environ.get("BATCH_WAIT_MS", "50") or 50)  # the longest wait for a batch to fill (consumer only)
# Messages one worker may compute at the same time (consumer only).
CPU_THREADS = int(os.environ.get("CPU_THREADS", "1") or 1)
# The document of the "json" scenario, the same bytes in every language (consumer only).
JSON_DOC = os.environ.get("JSON_DOC", "{}")


def kafka_conf(variable: str) -> dict:
    """Kafka client properties (librdkafka names) sent by the backend as JSON, e.g. {"property.name": value}."""
    return json.loads(os.environ[variable])
