"""What a consumer counts about the messages it reads, and the report it sends once per second."""

import time

import reporting
import settings
from lib import SeqRanges, bucket


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

    def record_batch(self, messages: list, now_ms: float, parse) -> list:
        """Count a batch of Kafka messages under one lock. Returns the messages that belong to this run."""
        mine = []
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
                mine.append(m)
            self.last_at = time.time()
        return mine

    def send_report(self, final: bool = False) -> None:
        """Send the cumulative counters. The histogram goes as the change since the last report, then it is reset."""
        with reporting.lock:
            payload = {
                "runId": settings.RUN, "worker": settings.WID, "role": "consumer", "lang": "python",
                "ts": time.time(), "ready": bool(self.assigned), "assigned": sorted(self.assigned),
                "received": self.received, "duplicates": self.seen.repeats, "ranges": self.seen.report(), "overflow": self.seen.overflow,
                "errors": self.errors, "offsets": {str(k): v for k, v in self.offsets.items()},
                "hist": self.hist, "final": final, "firstAt": self.first_at, "lastAt": self.last_at,
                "maxLatencyMs": self.max_latency_ms,
            }  # fmt: skip
            self.hist = self._empty_histogram()
        reporting.post(payload)
