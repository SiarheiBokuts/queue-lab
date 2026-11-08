"""Single source of truth for every constant used by the backend.

Anything that names a Docker object (labels, prefixes, networks, images), a limit or a default lives here, so it is
defined once and imported everywhere. Workers cannot import Python, so values they need are passed to them as
environment variables by `runs.py` (for example `HIST_EDGES`).
"""

import os
from pathlib import Path

# --------------------------------------------------------------------------------------------- paths
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("DATA_DIR", ROOT / "data"))  # SQLite file lives here (a Docker volume)
STATIC_DIR = Path(os.environ.get("STATIC_DIR", ROOT / "static"))  # the built Vue frontend
WORKERS_DIR = Path(os.environ.get("WORKERS_DIR", ROOT / "workers"))  # worker sources, built into images on demand

# ------------------------------------------------------------------------------------ naming and labels
# The version of the lab, shown in the sidebar. Keep it equal to the version in frontend/package.json and CHANGELOG.md.
VERSION = "1.0.0"
PREFIX = "queuelab"  # prefix of every lab container and image name
DEFAULT_PROJECT = "queue-lab"  # Compose project of the app; the real name is read from the app container at startup

# Docker labels. Every lab object carries them so we can find (and safely remove) only our own containers.
LABEL_LAB = PREFIX  # present on every lab object, value "true"
LABEL_LEVEL = f"{PREFIX}.level"  # which level of the two-level model the object belongs to
LABEL_ROLE = f"{PREFIX}.role"  # broker | producer | consumer
LABEL_QUEUE = f"{PREFIX}.queue"
LABEL_RUN = f"{PREFIX}.run"
LABEL_LANG = f"{PREFIX}.lang"
LABEL_SRC = f"{PREFIX}.src"  # on worker images: hash of the sources, so changed sources trigger a rebuild
LEVEL_SANDBOX = "sandbox"  # everything created from the UI belongs to the sandbox level
SANDBOX_FILTER = f"{LABEL_LEVEL}={LEVEL_SANDBOX}"  # `docker ps --filter` expression that selects the whole sandbox

# Labels Docker Compose uses to group containers into a project (see docker_service.compose_labels).
COMPOSE_PROJECT_LABEL = "com.docker.compose.project"

ROLE_BROKER, ROLE_PRODUCER, ROLE_CONSUMER = "broker", "producer", "consumer"
# Worker languages: each has a folder in workers/ with a producer and a consumer behind the same contract.
LANGS = ("python", "node", "java", "go", "rust")
LANG_NAMES = {"python": "Python", "node": "Node.js", "java": "Java", "go": "Go", "rust": "Rust"}


def sandbox_project(project: str) -> str:
    """Compose project and network name of the sandbox that belongs to the given app project."""
    return f"{project}-{LEVEL_SANDBOX}"


def container_name(queue: str, suffix: str) -> str:
    """Name of a lab container, e.g. `queuelab-orders-w1`."""
    return f"{PREFIX}-{queue}-{suffix}"


def worker_image(lang: str) -> str:
    """Tag of the worker image for a language, e.g. `queuelab-worker-node`."""
    return f"{PREFIX}-worker-{lang}"


def short_name(name: str) -> str:
    """Container name without the lab prefix, for display."""
    return name.removeprefix(f"{PREFIX}-")


# ---------------------------------------------------------------------------------------- app and network
INTERNAL_PORT = 8000  # port of the app inside its container
APP_ALIAS = f"{PREFIX}-app"  # DNS name workers use to reach the app on the sandbox network
REPORT_URL = f"http://{APP_ALIAS}:{INTERNAL_PORT}/internal/report"

# ------------------------------------------------------------------------------------------------- Kafka
BROKER_NAME = f"{PREFIX}-kafka"
BROKER_BOOTSTRAP = f"{BROKER_NAME}:9092"
KAFKA_IMAGE = "apache/kafka:4.4.0"  # always a pinned tag: `latest` can lag behind
KAFKA_NATIVE_IMAGE = "apache/kafka-native:4.4.0"  # lighter GraalVM build, intended for local testing
BROKER_MEM_MB = 1024
BROKER_MEM_LIGHT_MB = 512
PRODUCER_MEM_MB = 256

# ---------------------------------------------------------------------------------------------- budgets
SAFETY_RESERVE_MB = 512  # memory always left free, on top of the user's budget
MIN_FREE_FOR_KAFKA_MB = 1536  # below this the UI warns that Kafka may not fit

# ------------------------------------------------------------------------------------ latency histogram
# Latency = how long a message waited between being sent and being processed.
HIST_EDGES = [0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000]

# ------------------------------------------------------------------------------------------- run limits
SAMPLE_INTERVAL_S = 0.5  # how often a running run is sampled (rates, lag, latency percentiles)
# Requests that arrive all the time (every container reports once per second, the page polls the state). They are left out
# of the access log so the lines that matter stay readable.
QUIET_PATHS = ("/internal/report", "/api/state")
RUN_TIMEOUT_S = 1800  # a run is stopped after 30 minutes
STALL_TIMEOUT_S = 8  # after the producer is done, no progress for this long means the rest was lost
JOIN_TIMEOUT_S = 120  # how long to wait for all workers to receive their partitions


# -------------------------------------------------------------------------------------- Kafka client tuning
# The SAME values go to the Python and to the Node.js workers (librdkafka property names), and the workers contain no
PRODUCER_CONF: dict[str, str | int | bool] = {
    "acks": "1",  # the partition leader confirms; there is a single broker anyway
    "linger.ms": 5,  # wait up to 5 ms to batch messages together: far higher throughput
    "batch.num.messages": 10000,  # at most this many messages in one batch
    "queue.buffering.max.messages": 300000,  # how many messages the client holds before the producer has to wait
    "compression.type": "none",  # the padding compresses unrealistically well, so compression would hide real costs
}
CONSUMER_CONF: dict[str, str | int | bool] = {
    "auto.offset.reset": "latest",  # a new group starts at the end: the producer starts after every worker joined
    "enable.auto.commit": True,
    "session.timeout.ms": 10000,
    "fetch.wait.max.ms": 50,  # return quickly instead of waiting to fill a big batch: lower latency
    # Spelled out although they equal the defaults, so every client reads under the same conditions:
    "fetch.min.bytes": 1,  # the broker answers as soon as there is any data
    "fetch.max.bytes": 52428800,  # at most 50 MB in one fetch answer
    "fetch.message.max.bytes": 1048576,  # at most 1 MB per partition in one fetch answer (Java: max.partition.fetch.bytes)
}
