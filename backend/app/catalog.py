"""Human descriptions of the processing scenarios and the real worker source code shown in the UI."""

import re
import textwrap

from . import config

SCENARIOS = [
    {
        "id": "echo",
        "title": "Echo (count only)",
        "summary": "The worker receives each message, checks it and does nothing else. It measures the raw speed of Kafka plus the client library.",
        "simulates": "A consumer whose real work is free, for example it only forwards the message somewhere very fast.",
        "does": [
            "Reads the message and checks its sequence number to detect loss and duplicates.",
            "Records how long the message waited since it was sent (latency).",
            "Then immediately takes the next one.",
        ],
        "watch": [
            "Highest throughput you can get from this setup: the ceiling set by Kafka, the network and the client.",
            "Very low CPU per worker and a lag that drains almost as fast as it grows.",
            "A good baseline: run it first, then compare with the other scenarios.",
        ],
        "param": None,
    },
    {
        "id": "delay",
        "title": "Delay (sleep per message)",
        "summary": "The worker waits N milliseconds for every message, like a service that calls a database or an HTTP API.",
        "simulates": "I/O-bound work: a SQL query, a payment API, a call to another microservice. The CPU is idle while the worker waits.",
        "does": [
            "Does the same checks as echo.",
            "Then sleeps for the chosen number of milliseconds before taking the next message.",
        ],
        "watch": [
            "Throughput is capped by the delay: one worker handles at most 1000 / N messages per second.",
            "Total speed is about workers x 1000 / N, but only as many workers as there are partitions can work.",
            "Lag grows when the producer is faster. Adding workers (up to the partition count) scales speed almost linearly.",
            "CPU stays low; latency is mostly time spent waiting in the queue.",
        ],
        "param": {
            "label": "Delay per message (ms)",
            "default": 5,
            "min": 0,
            "hint": "1 ms is a fast service, 20 ms is a slow one.",
        },
    },
    {
        "id": "cpu",
        "title": "CPU load (hashing)",
        "summary": "For every message the worker computes SHA-256 N times in a row, which keeps one CPU core busy.",
        "simulates": "CPU-bound work: parsing, encryption, compression, image or JSON processing.",
        "does": [
            "Does the same checks as echo.",
            "Then runs a hash loop of the chosen length, so the work per message is predictable.",
        ],
        "watch": [
            "Worker CPU rises toward 100% of one core, so the CPU, not the network, becomes the bottleneck.",
            "Throughput scales with the number of workers only while there are free cores and partitions.",
            "Python and Node.js behave differently here: run the same setup with both and compare in Runs.",
            "Memory usually stays small, so you can see how little memory the work really needs.",
        ],
        "param": {
            "label": "Hash iterations per message",
            "default": 500,
            "min": 0,
            "hint": "200 is light, 5000 is heavy.",
        },
    },
]


def region(text: str, name: str) -> str:
    """Cut the code between `@name-start` and `@name-end` markers out of a source file and dedent it."""
    m = re.search(rf"^[^\n]*@{name}-start[^\n]*\n(.*?)^[^\n]*@{name}-end", text, re.S | re.M)
    return textwrap.dedent(m.group(1)).rstrip() if m else ""


# Where the marked parts of the worker code live. Only these two are shown: how messages are sent, and the step that
# processes each message. The bookkeeping around them (counting, checks, reporting) stays out of the way.
REGION_FILES = {
    "python": {"scenario": "consumer.py", "producer": "producer.py"},
    "node": {"scenario": "src/consumer.ts", "producer": "src/producer.ts"},
    "java": {"scenario": "src/main/java/lab/Consumer.java", "producer": "src/main/java/lab/Producer.java"},
    "go": {"scenario": "consumer.go", "producer": "producer.go"},
    "rust": {"scenario": "src/consumer.rs", "producer": "src/producer.rs"},
}


# A plain-words note for every shared Kafka client setting (shown on the Code page). The values themselves live in config.py.
CONF_NOTES = {
    "acks": "How many brokers must confirm a message before the sender goes on. 1 = only the leader (there is one broker here anyway).",
    "linger.ms": "The sender waits up to this long to put messages into one batch: much higher throughput for a few ms of delay.",
    "batch.num.messages": "At most this many messages go into one batch.",
    "queue.buffering.max.messages": "How many messages the client holds before the sender has to wait.",
    "compression.type": "Replaced by the compression chosen for the run in the form.",
    "auto.offset.reset": "Where a new group starts. 'latest' = at the end, so only messages sent during the run count.",
    "enable.auto.commit": "True: the client remembers its progress by itself. In production you commit after processing; the lab does not measure that.",
    "session.timeout.ms": "How long Kafka waits for a sign of life before it gives the worker's partitions to another worker.",
    "fetch.min.bytes": "The broker answers as soon as it has this much data (1 byte = at once). The Kafka default, written out so every language reads the same way.",
    "fetch.max.bytes": "The most data one fetch answer may hold (50 MB, the Kafka default, written out for all languages).",
    "fetch.message.max.bytes": "The most data per partition in one fetch answer (1 MB, the Kafka default). The Java client calls it max.partition.fetch.bytes.",
    "fetch.wait.max.ms": "How long the broker may wait to fill a batch. Lower = lower latency. Typical defaults: 100 ms (librdkafka) and 500 ms (Java).",
}  # fmt: skip

# The work every worker does for every message, in the same order, in every language. A comparison is fair only when
# the work is the same: each language implements these steps in the way that is fastest and most natural for it.
COMMON_WORK = [
    "Read the header of the message (run, sender, partition, sequence number, send time) straight from its bytes.",
    "Count the message as received and remember when the first one arrived.",
    "Check the sequence number of its stream: a jump means lost messages, a repeat means a duplicate.",
    "Put the latency (now minus send time) into a histogram bucket and remember the slowest one.",
    "Remember how far this partition has been read, which gives the messages waiting.",
    "Hand the message to the scenario step (wait, compute or nothing): up to the chosen number at once, messages of one user in order.",
    "Once per second send the totals to the lab. Nothing else is done per message.",
]

# How each worker reads messages and where that is set, for the Code page. Kafka's own defaults: max.poll.records = 500,
# max.poll.interval.ms = 5 minutes, fetch.min.bytes = 1, max.partition.fetch.bytes = 1 MB.
READING = {
    "python": {"read": "consume(500): up to 500 messages per call", "handle": "one thread per lane; a user's messages always use the same lane", "file": "workers/python/consumer.py", "cpu": "a pool of processes, one per core (at most 4): one process computes on one core only"},
    "node": {"read": "the client hands over a batch per partition (eachBatch); its size is decided by the client", "handle": "one async chain per lane; a user's messages always use the same lane", "file": "workers/node/src/consumer.ts", "cpu": "a pool of worker threads, one per core (at most 4): the main thread keeps reading"},
    "java": {"read": "poll(): up to max.poll.records = 500, the Kafka default (not changed here)", "handle": "one virtual thread per lane; a user's messages always use the same lane", "file": "workers/java/src/main/java/lab/Consumer.java", "cpu": "the lanes already run in parallel on all cores"},
    "go": {"read": "Poll(): one message per call from the client's internal queue (it prefetches up to 100,000 per partition)", "handle": "one goroutine per lane; a user's messages always use the same lane", "file": "workers/go/consumer.go", "cpu": "the lanes already run in parallel on all cores"},
    "rust": {"read": "poll(): one message per call from the client's internal queue (it prefetches up to 100,000 per partition)", "handle": "one thread per lane; a user's messages always use the same lane", "file": "workers/rust/src/consumer.rs", "cpu": "the lanes already run in parallel on all cores"},
}  # fmt: skip

def code() -> dict:
    """The main parts of the worker code per language, a template for the user's own step, and the shared client settings."""
    out: dict = {}
    for lang, files in REGION_FILES.items():
        base = config.WORKERS_DIR / lang
        regions = {name: region((base / file).read_text(encoding="utf-8"), name) for name, file in files.items()}
        out[lang] = regions
    out["client_conf"] = {"producer": config.PRODUCER_CONF, "consumer": config.CONSUMER_CONF}
    out["client_notes"] = CONF_NOTES
    out["reading"] = READING
    out["common_work"] = COMMON_WORK
    return out
