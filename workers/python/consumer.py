"""The consumer (the "handler"): reads messages in a consumer group, verifies them, runs the chosen scenario."""

import itertools
import os
import queue
import threading
import time

from confluent_kafka import Consumer

import cpu
import reporting
import settings
from lib import SeqTracker, bucket, lane_for, parse_header

LANE_QUEUE = 8  # messages that may wait in front of one lane before the reading loop pauses


def run() -> None:
    scenario = os.environ.get("SCENARIO", "echo")
    param = int(os.environ.get("SCENARIO_PARAM", "0") or 0)
    tracker = SeqTracker()  # detects gaps and duplicates per partition
    state = {
        "received": 0, "errors": 0, "hist": [0] * (len(settings.EDGES) + 1), "offsets": {}, "assigned": set(),
        "first_at": None, "last_at": None, "max_latency_ms": 0.0,
    }  # fmt: skip

    def on_assign(_consumer, partitions) -> None:
        """Kafka gave us partitions (also called again after every rebalance)."""
        with reporting.lock:
            for p in partitions:
                state["assigned"].add(p.partition)

    def on_revoke(_consumer, partitions) -> None:
        """Kafka took partitions away, usually because another worker joined or left."""
        with reporting.lock:
            for p in partitions:
                state["assigned"].discard(p.partition)

    # Offset reset, commit and timeout settings come from the backend (the same values for every language).
    consumer = Consumer({
        "bootstrap.servers": settings.BOOT,
        "group.id": os.environ["GROUP_ID"],
        **settings.kafka_conf("CONSUMER_CONF"),
    })  # fmt: skip
    consumer.subscribe([settings.TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    def send_report(final: bool = False) -> None:
        """Send cumulative counters. The histogram is sent as a delta and reset after each report."""
        with reporting.lock:
            payload = {
                "runId": settings.RUN, "worker": settings.WID, "role": "consumer", "lang": "python",
                "ts": time.time(), "ready": bool(state["assigned"]), "assigned": sorted(state["assigned"]),
                "received": state["received"], "duplicates": tracker.duplicates, "gaps": tracker.gaps,
                "errors": state["errors"], "offsets": {str(k): v for k, v in state["offsets"].items()},
                "hist": state["hist"], "final": final, "firstAt": state["first_at"], "lastAt": state["last_at"],
                "maxLatencyMs": state["max_latency_ms"],
            }  # fmt: skip
            state["hist"] = [0] * (len(settings.EDGES) + 1)
        reporting.post(payload)

    reporting.start_reporter(send_report)
    cpu_pool = cpu.make_pool() if scenario == "cpu" and param > 0 else None  # processes only when there is something to compute

    # @scenario-start
    def handle() -> None:
        """What the worker does with one message. Several lanes run it at the same time."""
        if scenario == "delay" and param > 0:
            time.sleep(param / 1000)  # pretend to wait for a database or an API call
        elif scenario == "cpu" and param > 0:
            cpu_pool.submit(cpu.burn, param).result()  # computing happens in a pool of processes, this thread waits for it
        # echo: do nothing, just count the message
    # @scenario-end

    # Production style: ONE loop reads from Kafka and hands every message to a "lane". There are CONCURRENCY lanes, each
    # handles its messages one after another, so up to CONCURRENCY messages are handled at the same time (while one lane
    lane_count = settings.CONCURRENCY
    lanes = [queue.Queue(maxsize=LANE_QUEUE) for _ in range(lane_count)] if lane_count > 1 and scenario != "echo" else []
    next_lane = itertools.count()

    def lane_worker(lane: queue.Queue) -> None:
        while lane.get() is not None:
            handle()

    threads = [threading.Thread(target=lane_worker, args=(lane,), daemon=True) for lane in lanes]
    for thread in threads:
        thread.start()

    def dispatch(key: bytes | None) -> None:
        if not lanes:
            handle()  # concurrency 1 (or nothing to wait for): one message at a time
            return
        index = lane_for(key, lane_count) if settings.ORDERING == "key" else next(next_lane) % lane_count
        lanes[index].put(True)  # blocks while that lane is full

    try:
        while not reporting.stop.is_set():
            batch = consumer.consume(num_messages=500, timeout=0.5)
            now_ms = time.time() * 1000
            keys = []
            with reporting.lock:  # one lock for the whole batch, not one per message
                for m in batch:
                    if m.error():
                        state["errors"] += 1
                        continue
                    header = parse_header(m.value(), settings.RUN)
                    if header is None:
                        continue  # a leftover from another run: ignore it
                    producer, partition, seq, sent_ms = header
                    state["received"] += 1
                    if state["first_at"] is None:
                        state["first_at"] = now_ms / 1000
                    tracker.observe((producer, partition), seq)  # counts gaps (lost) and duplicates
                    state["offsets"][partition] = m.offset() + 1  # the next offset to read, used for lag
                    latency = now_ms - sent_ms  # time from "sent" to "received", in milliseconds
                    state["hist"][bucket(latency, settings.EDGES)] += 1
                    if latency > state["max_latency_ms"]:
                        state["max_latency_ms"] = latency
                    keys.append(m.key())
                state["last_at"] = time.time()
            for key in keys:
                dispatch(key)  # may wait for a free lane, so it happens outside the lock
    finally:
        for lane in lanes:
            lane.put(None)  # tell the lanes to stop once their messages are handled
        for thread in threads:
            thread.join()  # let the messages already taken finish before the final report
        if cpu_pool is not None:
            cpu_pool.shutdown(wait=True)
        send_report(final=True)
        consumer.close()  # leave the group cleanly so the partitions are reassigned at once

