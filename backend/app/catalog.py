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
            "Reads the message and remembers its number, so the lab can count lost and repeated messages.",
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
        "id": "json",
        "title": "JSON (parse and write back)",
        "summary": "For every message the worker parses a JSON document and writes it back N times, with the standard JSON library of its language. Closest to what a real service does with a message.",
        "simulates": "CPU-bound work on structured data: reading a request, changing it, answering. Most services do this all day.",
        "does": [
            "Does the same checks as echo.",
            "Then parses the same JSON document (an order with a few items, about 600 bytes) and writes it back to text, N times. The document comes from the lab, so every language works on identical bytes.",
        ],
        "watch": [
            "Worker CPU rises toward 100% of one core, so the CPU, not the network, becomes the bottleneck.",
            "Throughput scales with the number of workers only while there are free cores and partitions.",
            "Each language uses its own standard library, so this compares the libraries too: Python's json is written in C, Go's and Java's are written in the languages themselves. That is what a real service in that language gets.",
            "Memory usually stays small, so you can see how little memory the work really needs.",
        ],
        "param": {
            "label": "Parse and write rounds per message",
            "default": 5,
            "min": 0,
            "hint": "1 is a light service, 20 is a heavy one.",
        },
    },]


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
    "queue.buffering.max.messages": "How many unconfirmed messages the sender may hold before it has to wait. The smaller of this and the size limit below counts.",
    "queue.buffering.max.kbytes": "How much unconfirmed data (32 MB) the sender may hold before it has to wait. The same memory limit for every language, so big messages cannot fill the sender's container.",
    "compression.type": "Replaced by the compression chosen for the run in the form.",
    "auto.offset.reset": "Where a new group starts. 'latest' = at the end, so only messages sent during the run count.",
    "enable.auto.commit": "False: the worker commits itself after every batch it has handled, like production. A crash before the commit repeats the batch.",
    "session.timeout.ms": "How long Kafka waits for a sign of life before it gives the worker's partitions to another worker.",
    "queued.min.messages": "How many messages per partition the client reads ahead while the workers are busy. Small, so a backlog waits in Kafka and not in the memory of the worker. Java has no such setting: it reads one fetch at a time.",
    "queued.max.messages.kbytes": "The most data per partition the client keeps ready, 32 MB.",
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
    "Remember the number of the message in its stream (sender and partition) as part of a range, so the lab can count lost and repeated messages exactly.",
    "Put the latency (now minus send time) into a histogram bucket and remember the slowest one.",
    "Remember how far this partition has been read, which gives the messages waiting.",
    "Hand the message to the scenario step (wait, work with JSON or nothing): up to the chosen number at once, messages of one user in order.",
    "Once per second send the totals to the lab. Nothing else is done per message.",
]

# How each worker reads messages and where that is set, for the Code page. Every language runs the same loop:
# read a batch, group it by user, handle the groups at the same time, commit.
READING = {
    "python": {"read": "consume(N): up to N messages per call, N = the batch size", "handle": "one thread per user in the batch; a user's messages one after another", "file": "workers/python/consumer.py", "cpu": "a pool of processes (one computes on one core only), as many compute at once as the worker has whole cores"},
    "node": {"read": "eachBatch: up to N messages of one partition (js.consumer.max.batch.size = N)", "handle": "one async task per user in the batch; a user's messages one after another", "file": "workers/node/src/consumer.ts", "cpu": "a pool of worker threads, as many compute at once as the worker has whole cores; the main thread keeps reading"},
    "java": {"read": "poll(): up to N messages (max.poll.records = N)", "handle": "one virtual thread per user in the batch; a user's messages one after another", "file": "workers/java/src/main/java/lab/Consumer.java", "cpu": "the groups compute in parallel, a counter of free places lets as many compute at once as the worker has whole cores"},
    "go": {"read": "Poll() until N messages or the wait is over", "handle": "one goroutine per user in the batch; a user's messages one after another", "file": "workers/go/consumer.go", "cpu": "the groups compute in parallel, a counter of free places lets as many compute at once as the worker has whole cores"},
    "rust": {"read": "poll() until N messages or the wait is over", "handle": "one thread per user in the batch; a user's messages one after another", "file": "workers/rust/src/consumer.rs", "cpu": "the groups compute in parallel, a counter of free places lets as many compute at once as the worker has whole cores"},
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
