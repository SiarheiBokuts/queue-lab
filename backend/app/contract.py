"""The worker contract: the environment variables every worker container gets, and the JSON report it sends back.

    every role   ROLE, KAFKA_BOOTSTRAP, TOPIC, RUN_ID, WORKER_ID, REPORT_URL, HIST_EDGES
    consumer     GROUP_ID, SCENARIO, SCENARIO_PARAM, CONSUMER_CONF, CONCURRENCY (batch size), BATCH_WAIT_MS, CPU_THREADS, JSON_DOC
    producer     MESSAGES, MSG_SIZE, PARTITIONS, PRODUCER_INDEX, RATE, PAYLOAD, USERS, MAX_UNCONFIRMED, PRODUCER_CONF

*_CONF hold the Kafka client settings of config.py as JSON, identical for every language.
"""

import json

from . import config
from .models import SetupRequest

# Latency histogram edges as the workers receive them. Defined once in config.HIST_EDGES.
HIST_EDGES_ENV = ",".join(str(e) for e in config.HIST_EDGES)

COMMON = ("ROLE", "KAFKA_BOOTSTRAP", "TOPIC", "RUN_ID", "WORKER_ID", "REPORT_URL", "HIST_EDGES")
CONSUMER_ONLY = ("GROUP_ID", "SCENARIO", "SCENARIO_PARAM", "CONSUMER_CONF", "CONCURRENCY", "BATCH_WAIT_MS", "CPU_THREADS", "JSON_DOC")
PRODUCER_ONLY = ("MESSAGES", "MSG_SIZE", "PARTITIONS", "PRODUCER_INDEX", "RATE", "PAYLOAD", "USERS", "MAX_UNCONFIRMED", "PRODUCER_CONF")

# Variables that only some languages read. The others do not need them: the Kafka client of Python, Go, Rust and Java
# enforces the unconfirmed-messages limit itself, while Node.js uses the number to size its batches.
READ_ONLY_BY = {"MAX_UNCONFIRMED": ("node",)}


# What workers send back: one JSON report per second, the same fields from every language. The backend reads exactly these.
CONSUMER_REPORT = ("runId", "worker", "role", "lang", "ts", "ready", "assigned", "received", "duplicates", "ranges", "overflow", "errors",
                   "offsets", "hist", "final", "firstAt", "lastAt", "maxLatencyMs", "orderViolations", "batchBusyMs",
                   "batchWaitMs")  # fmt: skip
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


def consumer_env(req: SetupRequest, run_id: str, worker_id: str, cpu_threads: int = 1) -> dict[str, str]:
    """Environment of one consumer. All consumers of a run share one consumer group."""
    return _common(config.ROLE_CONSUMER, req.queue, run_id, worker_id) | {
        "GROUP_ID": f"g-{run_id}",
        "SCENARIO": req.scenario.type,
        "SCENARIO_PARAM": str(req.scenario.param),
        "CONSUMER_CONF": json.dumps(config.CONSUMER_CONF),
        "CONCURRENCY": str(req.workers.concurrency),
        "BATCH_WAIT_MS": str(config.BATCH_WAIT_MS),
        "CPU_THREADS": str(cpu_threads),
        "JSON_DOC": config.JSON_DOC,
    }


def max_unconfirmed(message_size: int) -> int:
    """How many messages one sender may hold that the broker has not confirmed yet: the smaller of the message limit
    and the byte limit of config.PRODUCER_CONF. The Kafka clients enforce these limits themselves; Node.js needs the
    number to size its batches, so it gets it here and not from its own arithmetic."""
    by_bytes = config.PRODUCER_CONF["queue.buffering.max.kbytes"] * 1024 // message_size
    return max(1, min(config.PRODUCER_CONF["queue.buffering.max.messages"], by_bytes))


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
        "MAX_UNCONFIRMED": str(max_unconfirmed(req.message_size)),
        "PRODUCER_CONF": json.dumps(conf),
    }
