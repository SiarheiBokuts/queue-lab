"""The worker contract: the environment variables every worker container gets, and the JSON report it sends back.

    every role   ROLE, KAFKA_BOOTSTRAP, TOPIC, RUN_ID, WORKER_ID, REPORT_URL, HIST_EDGES
    consumer     GROUP_ID, SCENARIO, SCENARIO_PARAM, CONSUMER_CONF, CONCURRENCY, ORDERING
    producer     MESSAGES, MSG_SIZE, PARTITIONS, PRODUCER_INDEX, RATE, PAYLOAD, USERS, PRODUCER_CONF

*_CONF hold the Kafka client settings of config.py as JSON, identical for every language.
"""

import json

from . import config
from .models import SetupRequest

# Latency histogram edges as the workers receive them. Defined once in config.HIST_EDGES.
HIST_EDGES_ENV = ",".join(str(e) for e in config.HIST_EDGES)

COMMON = ("ROLE", "KAFKA_BOOTSTRAP", "TOPIC", "RUN_ID", "WORKER_ID", "REPORT_URL", "HIST_EDGES")
CONSUMER_ONLY = ("GROUP_ID", "SCENARIO", "SCENARIO_PARAM", "CONSUMER_CONF", "CONCURRENCY", "ORDERING")
PRODUCER_ONLY = ("MESSAGES", "MSG_SIZE", "PARTITIONS", "PRODUCER_INDEX", "RATE", "PAYLOAD", "USERS", "PRODUCER_CONF")


# What workers send back: one JSON report per second, the same fields from every language. The backend reads exactly these.
CONSUMER_REPORT = ("runId", "worker", "role", "lang", "ts", "ready", "assigned", "received", "duplicates", "gaps", "errors",
                   "offsets", "hist", "final", "firstAt", "lastAt", "maxLatencyMs")  # fmt: skip
PRODUCER_REPORT = ("runId", "worker", "role", "lang", "ts", "sent", "delivered", "errors", "done", "started", "finished")


def _common(role: str, queue: str, run_id: str, worker_id: str) -> dict[str, str]:
    return {
        "ROLE": role,
        "KAFKA_BOOTSTRAP": config.BROKER_BOOTSTRAP,
        "TOPIC": queue,
        "RUN_ID": run_id,
        "WORKER_ID": worker_id,
        "REPORT_URL": config.REPORT_URL,
        "HIST_EDGES": HIST_EDGES_ENV,
    }


def consumer_env(req: SetupRequest, run_id: str, worker_id: str) -> dict[str, str]:
    """Environment of one consumer. All consumers of a run share one consumer group."""
    return _common(config.ROLE_CONSUMER, req.queue, run_id, worker_id) | {
        "GROUP_ID": f"g-{run_id}",
        "SCENARIO": req.scenario.type,
        "SCENARIO_PARAM": str(req.scenario.param),
        "CONSUMER_CONF": json.dumps(config.CONSUMER_CONF),
        "CONCURRENCY": str(req.workers.concurrency),
        "ORDERING": req.workers.ordering,
    }


def producer_env(req: SetupRequest, run_id: str, worker_id: str, index: int = 0) -> dict[str, str]:
    """Environment of one producer. The messages and the target rate are split evenly between the producers."""
    base, extra = divmod(req.messages, req.producer.count)
    share = base + (1 if index < extra else 0)
    # The compression chosen for this run replaces the default; everything else is the shared client setting.
    conf = config.PRODUCER_CONF | {"compression.type": req.compression}
    return _common(config.ROLE_PRODUCER, req.queue, run_id, worker_id) | {
        "MESSAGES": str(share),
        "MSG_SIZE": str(req.message_size),
        "PARTITIONS": str(req.partitions),
        "PRODUCER_INDEX": str(index),
        "RATE": str(req.rate // req.producer.count),
        "PAYLOAD": req.payload,
        "USERS": str(req.users),
        "PRODUCER_CONF": json.dumps(conf),
    }
