"""The producer: sends its share of the messages over all partitions, at the requested rate, then reports "done"."""

import os
import time

from confluent_kafka import Producer

import reporting
import settings
from lib import build_header, pacing_chunk, pacing_delay, user_key

POOL_BYTES = 1 << 20  # random padding is cut from one pre-generated megabyte, so generating it costs almost nothing


def run() -> None:
    total = int(os.environ["MESSAGES"])  # this producer's share of the messages
    size = int(os.environ["MSG_SIZE"])
    nparts = int(os.environ["PARTITIONS"])
    index = int(os.environ["PRODUCER_INDEX"])  # which producer this is (0, 1, ...) when several share the load
    rate = float(os.environ["RATE"])  # this producer's target messages per second; 0 = as fast as possible
    random_payload = os.environ["PAYLOAD"] == "random"  # incompressible data instead of a repeated filler
    users = int(os.environ["USERS"])  # how many different users (message keys) the messages are spread over
    state = {"sent": 0, "delivered": 0, "errors": 0, "done": False, "started": None, "finished": None}

    def on_delivery(err, _msg) -> None:
        """Called by the client for every message once the broker confirmed or rejected it."""
        with reporting.lock:
            if err is not None:
                state["errors"] += 1
            else:
                state["delivered"] += 1

    # Batching, acks, queue size and compression come from the backend (the same values for every language).
    producer = Producer({"bootstrap.servers": settings.BOOT, **settings.kafka_conf("PRODUCER_CONF")})
    pool = os.urandom(POOL_BYTES + size) if random_payload else b""

    def send_report() -> None:
        with reporting.lock:
            payload = {
                "runId": settings.RUN, "worker": settings.WID, "role": "producer", "lang": "python", "ts": time.time(),
                "sent": state["sent"], "delivered": state["delivered"], "errors": state["errors"],
                "done": state["done"], "started": state["started"], "finished": state["finished"],
            }  # fmt: skip
        reporting.post(payload)

    reporting.start_reporter(send_report, keep_going=lambda: not state["done"])

    # @producer-start
    seqs = [0] * nparts  # each partition has its own sequence counter for this producer, starting at 0
    started = time.time()
    state["started"] = started
    check_every = pacing_chunk(rate, 500)  # how often to look at the clock and serve delivery callbacks
    for i in range(total):
        if reporting.stop.is_set():
            break
        part = (i + index) % nparts  # round robin; producers start on different partitions so the load stays even
        header = build_header(settings.RUN, index, part, seqs[part], time.time() * 1000)
        seqs[part] += 1
        fill = max(0, size - len(header))  # pad up to the requested message size
        padding = pool[(i * 997) % POOL_BYTES :][:fill] if random_payload else b"x" * fill
        payload = header + padding
        while True:
            try:
                producer.produce(settings.TOPIC, payload, key=user_key(part, i, nparts, users), partition=part, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.05)  # the local queue is full: let the client drain it, then retry
        with reporting.lock:
            state["sent"] = i + 1
        if (i + 1) % check_every == 0:
            producer.poll(0)  # serve delivery callbacks regularly
            delay = pacing_delay(i + 1, rate, time.time() - started)  # ahead of the target rate? then wait
            if delay > 0:
                time.sleep(delay)
    # @producer-end
    producer.flush(60)  # wait until every message is confirmed by the broker
    with reporting.lock:
        state["finished"] = time.time()
        state["done"] = True
    send_report()