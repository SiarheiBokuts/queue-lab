"""What a consumer counts about the messages it reads, and the report it sends once per second."""

import time

import reporting
import settings
from lib import Item, OrderCheck, SeqRanges, bucket


class ConsumerStats:
    """All counters of one consumer. Every method takes `reporting.lock`, because the reporter thread reads them."""

    def __init__(self) -> None:
        self.received = 0
        self.errors = 0
        self.first_at: float | None = None  # seconds since the epoch
        self.last_at: float | None = None
        self.max_latency_ms = 0.0
        self.offsets: dict[int, int] = {}  # the next offset to read per partition, used to work out the lag
        self.assigned: set[int] = set()
        self.seen = SeqRanges()  # which message numbers arrived: the lab counts lost and repeated ones from it
        self.order = OrderCheck()  # the messages of a user must be handled in the order they were sent
        self.batch_busy_ms = 0.0  # time spent handling batches
        self.batch_wait_ms = 0.0  # of which some groups were done and the others still worked
        self.hist = self._empty_histogram()

    @staticmethod
    def _empty_histogram() -> list[int]:
        return [0] * (len(settings.EDGES) + 1)

    def assign(self, partitions: list[int]) -> None:
        """Kafka gave us partitions (also called again after every rebalance)."""
        with reporting.lock:
            self.assigned.update(partitions)

    def revoke(self, partitions: list[int]) -> None:
        """Kafka took partitions away, usually because another worker joined or left."""
        with reporting.lock:
            self.assigned.difference_update(partitions)

    def add_batch(self, busy_ms: float, wait_ms: float) -> None:
        """Remember how long a batch took and how much of it was spent waiting for the slowest group."""
        with reporting.lock:
            self.batch_busy_ms += busy_ms
            self.batch_wait_ms += wait_ms

    def record_batch(self, messages: list, now_ms: float, parse) -> list[Item]:
        """Count a batch of Kafka messages under one lock. Returns the items of the ones that belong to this run."""
        mine: list[Item] = []
        with reporting.lock:
            for m in messages:
                if m.error():
                    self.errors += 1
                    continue
                header = parse(m.value(), settings.RUN)
                if header is None:
                    continue  # a leftover from another run: ignore it
                producer, partition, seq, sent_ms = header
                self.received += 1
                if self.first_at is None:
                    self.first_at = now_ms / 1000
                self.seen.observe(producer, partition, seq)
                self.offsets[partition] = m.offset() + 1
                latency = now_ms - sent_ms  # from "sent" to "received", in milliseconds
                self.hist[bucket(latency, settings.EDGES)] += 1
                self.max_latency_ms = max(self.max_latency_ms, latency)
                mine.append(Item(m.key(), producer, seq))
            self.last_at = time.time()
        return mine

    def send_report(self, final: bool = False) -> None:
        """Send the cumulative counters. The histogram goes as the change since the last report, then it is reset."""
        with reporting.lock:
            payload = {
                "runId": settings.RUN, "worker": settings.WID, "role": "consumer", "lang": "python",
                "ts": time.time(), "ready": bool(self.assigned), "assigned": sorted(self.assigned),
                "received": self.received, "duplicates": self.seen.repeats, "ranges": self.seen.report(),
                "overflow": self.seen.overflow, "orderViolations": self.order.violations,
                "batchBusyMs": round(self.batch_busy_ms, 1), "batchWaitMs": round(self.batch_wait_ms, 1),
                "errors": self.errors, "offsets": {str(k): v for k, v in self.offsets.items()},
                "hist": self.hist, "final": final, "firstAt": self.first_at, "lastAt": self.last_at,
                "maxLatencyMs": self.max_latency_ms,
            }  # fmt: skip
            self.hist = self._empty_histogram()
        reporting.post(payload)
