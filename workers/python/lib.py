"""Pure helpers of the Python worker: no Kafka, no network, no environment. Easy to unit test.

Message format, the same in every language:

    <run>|<producer>|<partition>|<sequence>|<sentAtMs>|xxxxxxxx...

run: id of the run, so leftovers of earlier runs are ignored. producer: which sender. partition: where it was sent.
sequence: counts from 0 per sender and partition; a gap means loss, a repeat a duplicate. sentAtMs: the sender's clock,
subtracted from "now" to get the latency. The rest is padding up to the message size chosen in the form.
"""

import zlib
from collections.abc import Hashable

HEADER_SEPARATOR = b"|"
HEADER_MAX_BYTES = 96  # the header is far shorter; we never copy the (possibly 64 KB) padding just to read it


def parse_edges(raw: str) -> list[float]:
    """Parse the HIST_EDGES environment value, e.g. "0.5,1,2,5", into the bucket edges in milliseconds."""
    return [float(part) for part in raw.split(",") if part.strip()]


def bucket(latency_ms: float, edges: list[float]) -> int:
    """Index of the histogram bucket a latency falls into.

    Bucket i holds latencies <= edges[i]; the last bucket (index len(edges)) holds everything above the last edge.
    Counting messages per bucket keeps reports small even at hundreds of thousands of messages per second.
    """
    for index, edge in enumerate(edges):
        if latency_ms <= edge:
            return index
    return len(edges)


def build_header(run: str, producer: int, partition: int, seq: int, sent_ms: float) -> bytes:
    """Header of a message; the producer appends padding after it."""
    return f"{run}|{producer}|{partition}|{seq}|{sent_ms:.3f}|".encode()


def parse_header(value: bytes, expected_run: str) -> tuple[int, int, int, float] | None:
    """Return (producer, partition, sequence, sent_at_ms), or None if the message is not part of `expected_run`."""
    parts = value[:HEADER_MAX_BYTES].split(HEADER_SEPARATOR, 5)
    if len(parts) < 6 or parts[0].decode(errors="replace") != expected_run:
        return None
    try:
        return int(parts[1]), int(parts[2]), int(parts[3]), float(parts[4])
    except ValueError:
        return None


class SeqTracker:
    """Detects lost and duplicated messages from per-partition sequence numbers.

    Within one partition Kafka preserves order, so after sequence N of a stream (one producer, one partition) the next
    message of that stream must be N + 1.
    A higher number means messages were skipped (gap); a number we already saw (or lower) means a duplicate.
    The first message seen on a stream only sets the baseline, because a worker may join mid-run after a rebalance.
    """

    def __init__(self) -> None:
        self.last: dict[Hashable, int] = {}
        self.duplicates = 0
        self.gaps = 0  # total number of skipped sequence numbers

    def observe(self, stream: Hashable, seq: int) -> str:
        """Record one message of a stream, e.g. (producer, partition), and return "ok", "gap" or "duplicate"."""
        last = self.last.get(stream)
        if last is None or seq == last + 1:
            self.last[stream] = seq
            return "ok"
        if seq <= last:
            self.duplicates += 1
            return "duplicate"
        self.gaps += seq - last - 1
        self.last[stream] = seq
        return "gap"


def user_key(part: int, i: int, nparts: int, users: int) -> bytes:
    """Message key of the user a message belongs to, like a payment of one user.

    A user always lands in the same partition (user number % partitions == partition), so Kafka keeps that user's messages
    in order. Within a partition the users take turns.
    """
    per_partition = max(1, users // nparts)
    return f"user-{part + nparts * ((i // nparts) % per_partition)}".encode()


def lane_for(key: bytes | None, lanes: int) -> int:
    """The lane (0 .. lanes-1) that handles messages with this key. The same key always gets the same lane."""
    return zlib.crc32(key or b"") % lanes


def pacing_delay(sent: int, rate: float, elapsed: float) -> float:
    """Seconds a producer must wait so that `sent` messages take at least sent / rate seconds. 0 means "go on".

    This is how a producer holds a steady target rate: it compares the time it should have taken with the time it did
    take. A rate of 0 means "as fast as possible", and a producer that is behind schedule never waits.
    """
    if rate <= 0:
        return 0.0
    return max(0.0, sent / rate - elapsed)


def pacing_chunk(rate: float, max_chunk: int) -> int:
    """How many messages to send between two pacing checks: about 10 ms worth, so the rate stays smooth."""
    if rate <= 0:
        return max_chunk
    return max(1, min(max_chunk, int(rate / 100)))
