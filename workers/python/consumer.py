"""The consumer (the "handler"): reads messages in batches, verifies them and runs the chosen scenario.

The same loop is written in every language of the lab, with the same steps and names:

    read_batch -> group_by_user -> run_batch (handle_group for every user at the same time) -> commit

A batch is at most CONCURRENCY messages. The messages of one user are always handled one after another, like in
production; the messages of different users are handled at the same time. The next batch is read when the whole
batch is done: the client has already fetched the next messages in the background, and starting early could let a
message of a busy user overtake an older one.
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor

from confluent_kafka import Consumer

import cpu
import reporting
import settings
from lib import Item, OrderCheck, batch_timing, group_by_user, parse_header
from stats import ConsumerStats

SCENARIO = os.environ.get("SCENARIO", "echo")
PARAM = int(os.environ.get("SCENARIO_PARAM", "0") or 0)


def read_batch(consumer: Consumer) -> list:
    """Wait at most BATCH_WAIT_MS for a message, then take up to CONCURRENCY messages that have already arrived."""
    batch = consumer.consume(num_messages=1, timeout=settings.BATCH_WAIT_MS / 1000)
    if batch and settings.CONCURRENCY > 1:
        batch += consumer.consume(num_messages=settings.CONCURRENCY - 1, timeout=0)  # no waiting to fill the batch
    return batch


def make_handle(cpu_pool):
    """The scenario for one message, as a function."""

    # @scenario-start
    def handle() -> None:
        """What the worker does with one message. The same three scenarios in every language:
        echo = nothing, the message is only counted; delay = wait, like a call to a database;
        json = parse a JSON document and write it back, like a service reading a request."""
        if SCENARIO == "delay" and PARAM > 0:
            time.sleep(PARAM / 1000)  # wait PARAM milliseconds and do nothing else
        elif SCENARIO == "json" and PARAM > 0:
            # Parse the document and write it back PARAM times. It runs in another process,
            # so this one stays free to read.
            cpu_pool.submit(cpu.work, settings.JSON_DOC, PARAM).result()
        # echo: nothing to do

    # @scenario-end
    return handle


def handle_group(group: list[Item], handle, order: OrderCheck) -> float:
    """Handle the messages of one user one after another. Returns the time the group was done."""
    for item in group:
        order.check(item)
        handle()
    return time.perf_counter()


def run_batch(groups: list[list[Item]], handle, order: OrderCheck, executor: ThreadPoolExecutor) -> tuple[float, float]:
    """Handle all groups at the same time and wait until every one of them is done.

    Returns how long the batch took and how long of that some groups were done while others still worked (milliseconds).
    """
    started = time.perf_counter()
    done = [f.result() for f in [executor.submit(handle_group, g, handle, order) for g in groups]]
    return batch_timing(started, done)


def commit(consumer: Consumer) -> None:
    """Tell Kafka that everything read so far is handled. A crash before this repeats the batch."""
    consumer.commit(asynchronous=False)


def run() -> None:
    stats = ConsumerStats()
    consumer = Consumer({
        "bootstrap.servers": settings.BOOT,
        "group.id": os.environ["GROUP_ID"],
        **settings.kafka_conf("CONSUMER_CONF"),
    })  # fmt: skip
    consumer.subscribe(
        [settings.TOPIC],
        on_assign=lambda _c, parts: stats.assign([p.partition for p in parts]),
        on_revoke=lambda _c, parts: stats.revoke([p.partition for p in parts]),
    )
    reporting.start_reporter(stats.send_report)
    cpu_pool = cpu.make_pool(settings.CPU_THREADS) if SCENARIO == "json" and PARAM > 0 else None
    handle = make_handle(cpu_pool)
    executor = ThreadPoolExecutor(max_workers=settings.CONCURRENCY)
    try:
        while not reporting.stop.is_set():
            batch = read_batch(consumer)
            if not batch:
                continue
            mine = stats.record_batch(batch, time.time() * 1000, parse_header)
            busy_ms, wait_ms = run_batch(group_by_user(mine), handle, stats.order, executor)
            stats.add_batch(busy_ms, wait_ms)
            commit(consumer)
    finally:
        executor.shutdown(wait=True)  # let the messages already taken finish before the final report
        if cpu_pool is not None:
            cpu_pool.shutdown(wait=True)
        stats.send_report(final=True)
        consumer.close()  # leave the group cleanly so the partitions are reassigned at once
