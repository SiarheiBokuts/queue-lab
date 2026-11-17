"""The producer: sends its share of the messages over all partitions, at the requested rate, then reports "done".

The file reads top to bottom in the same groups as the producer of every other language:
 1. settings      what this producer was asked to do
 2. counters      what the report shows, shared by the loop, the delivery callbacks and the reporter
 3. client        the Kafka producer and its delivery reports
 4. message       how one message is built and handed to the client
 5. run           the steps in order (the sending loop is the part shown on the Code page)
"""

import os
import time
from dataclasses import dataclass

from confluent_kafka import Producer

import reporting
import settings
from lib import build_header, pacing_chunk, pacing_delay, user_key

# POOL_BYTES: random padding is cut from one pre-generated megabyte, so generating it costs almost nothing.
POOL_BYTES = 1 << 20


# ---- 1. settings -------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class ProducerSettings:
    total: int  # this producer's share of the messages
    size: int  # bytes per message
    partitions: int  # partitions of the queue
    index: int  # which producer this is (0, 1, ...) when several share the load
    rate: float  # this producer's target messages per second; 0 = as fast as possible
    random_payload: bool  # incompressible data instead of a repeated filler
    users: int  # how many different users (message keys) the messages are spread over


def read_settings() -> ProducerSettings:
    return ProducerSettings(
        total=int(os.environ["MESSAGES"]),
        size=int(os.environ["MSG_SIZE"]),
        partitions=int(os.environ["PARTITIONS"]),
        index=int(os.environ["PRODUCER_INDEX"]),
        rate=float(os.environ["RATE"]),
        random_payload=os.environ["PAYLOAD"] == "random",
        users=int(os.environ["USERS"]),
    )


# ---- 2. counters -------------------------------------------------------------------------------------------------


class Counters:
    """Every change and every read goes through reporting.lock: the loop, the client and the reporter share them."""

    def __init__(self) -> None:
        self.sent = 0
        self.delivered = 0
        self.errors = 0
        self.done = False
        self.started: float | None = None  # seconds since 1970; None = not yet
        self.finished: float | None = None

    def set_sent(self, n: int) -> None:
        with reporting.lock:
            self.sent = n

    def confirmed(self, failed: bool) -> None:
        """Called for every message once the broker confirmed or rejected it."""
        with reporting.lock:
            if failed:
                self.errors += 1
            else:
                self.delivered += 1

    def start(self) -> None:
        with reporting.lock:
            self.started = time.time()

    def finish(self) -> None:
        with reporting.lock:
            self.finished = time.time()
            self.done = True

    def running(self) -> bool:
        with reporting.lock:
            return not self.done

    def report(self) -> None:
        with reporting.lock:
            payload = {
                "runId": settings.RUN, "worker": settings.WID, "role": "producer", "lang": "python", "ts": time.time(),
                "sent": self.sent, "delivered": self.delivered, "errors": self.errors, "done": self.done,
                "started": self.started, "finished": self.finished,
            }  # fmt: skip
        reporting.post(payload)


# ---- 3. client ---------------------------------------------------------------------------------------------------


def new_kafka_producer(counters: Counters) -> Producer:
    def on_delivery(err, _message) -> None:
        """Delivery report: the broker confirmed or rejected a message."""
        counters.confirmed(err is not None)

    # Batching, acks, queue size and compression come from the backend (the same values for every language).
    conf = settings.kafka_conf("PRODUCER_CONF")
    conf["bootstrap.servers"] = settings.BOOT
    conf["on_delivery"] = on_delivery
    return Producer(conf)


# ---- 4. message --------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Message:
    partition: int  # the partition this message goes to
    key: bytes  # the user the message belongs to
    value: bytes  # the bytes to send: header padded up to the message size


class MessageMaker:
    """Builds message number i of this producer: which partition, which user, and the bytes to send.

    It owns what has to be remembered between messages: the sequence counter of every partition.
    """

    def __init__(self, cfg: ProducerSettings) -> None:
        self.cfg = cfg
        self.seqs = [0] * cfg.partitions  # each partition has its own sequence counter for this producer, starting at 0
        # The data random padding is cut from (empty when the padding is a repeated filler).
        self.pool = os.urandom(POOL_BYTES + cfg.size) if cfg.random_payload else b""

    def make(self, i: int) -> Message:
        cfg = self.cfg
        part = (i + cfg.index) % cfg.partitions  # round robin; producers start on different partitions, load stays even
        header = build_header(settings.RUN, cfg.index, part, self.seqs[part], time.time() * 1000)
        self.seqs[part] += 1
        return Message(part, user_key(part, i, cfg.partitions, cfg.users), self._pad(header, i))

    def _pad(self, header: bytes, i: int) -> bytes:
        """The header (who sent it, where, which number, when) padded up to the requested message size."""
        fill = max(0, self.cfg.size - len(header))
        if self.cfg.random_payload:
            offset = (i * 997) % POOL_BYTES
            return header + self.pool[offset : offset + fill]
        return header + b"x" * fill


def produce(producer: Producer, message: Message) -> None:
    """Hand a message to the client; when the local queue is full, wait for the client to drain it."""
    while True:
        try:
            producer.produce(settings.TOPIC, message.value, key=message.key, partition=message.partition)
            return
        except BufferError:
            producer.poll(0.05)


# ---- 5. run ------------------------------------------------------------------------------------------------------


def run() -> None:
    cfg = read_settings()
    counters = Counters()
    producer = new_kafka_producer(counters)
    maker = MessageMaker(cfg)
    reporting.start_reporter(counters.report, keep_going=counters.running)

    # @producer-start
    begin = time.time()
    counters.start()
    check_every = pacing_chunk(cfg.rate, 500)  # how often to look at the clock and serve delivery callbacks
    for i in range(cfg.total):
        if reporting.stop.is_set():
            break
        message = maker.make(i)  # message number i: its partition, its user and its bytes
        produce(producer, message)
        counters.set_sent(i + 1)
        if (i + 1) % check_every == 0:
            producer.poll(0)  # serve delivery callbacks regularly
            wait = pacing_delay(i + 1, cfg.rate, time.time() - begin)  # ahead of the target rate? then wait
            if wait > 0:
                time.sleep(wait)
    # @producer-end
    producer.flush(60)  # wait until every message is confirmed by the broker
    counters.finish()
    counters.report()
