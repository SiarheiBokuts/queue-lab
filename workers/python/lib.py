"""Pure helpers of the Python worker: no Kafka, no network, no environment. Easy to unit test.

Message format, the same in every language:

    <run>|<producer>|<partition>|<sequence>|<sentAtMs>|xxxxxxxx...

run: id of the run, so leftovers of earlier runs are ignored. producer: which sender. partition: where it was sent.
sequence: counts from 0 per sender and partition; a gap means loss, a repeat a duplicate. sentAtMs: the sender's clock,
subtracted from "now" to get the latency. The rest is padding up to the message size chosen in the form.
"""

import threading
from typing import NamedTuple

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


# Ranges kept per stream; beyond this the worker only counts (the report says how many it did not list).
MAX_RANGES = 1000


class SeqRanges:
    """Remembers which message numbers arrived, as ranges, so the lab can count lost and repeated messages exactly.

    Every message has a number per stream (one sender, one partition). Numbers that arrive one after another make
    one range (`0-4999`), so a run without losses is one range per stream. A number that is already inside a range
    is a repeat. The lab merges the ranges of all workers: what was sent and is in nobody's ranges is lost, what is
    counted more than once is repeated (for example after a worker restarts and its batch is read again).
    """

    def __init__(self) -> None:
        self.streams: dict[tuple[int, int], list[list[int]]] = {}
        self.repeats = 0  # numbers this worker saw twice
        self.overflow = 0  # numbers not listed because a stream hit MAX_RANGES

    def observe(self, producer: int, partition: int, seq: int) -> bool:
        """Remember one message number. Returns False when this worker has seen it before."""
        ranges = self.streams.setdefault((producer, partition), [])
        if ranges and seq == ranges[-1][1] + 1:
            ranges[-1][1] = seq  # the usual case: the next number
            return True
        if any(start <= seq <= end for start, end in ranges):
            self.repeats += 1
            return False
        if len(ranges) >= MAX_RANGES:
            self.overflow += 1
            return True
        ranges.append([seq, seq])
        return True

    def report(self) -> dict[str, list[list[int]]]:
        """The ranges per stream, sorted and merged, keyed "producer:partition"."""
        out: dict[str, list[list[int]]] = {}
        for (producer, partition), ranges in self.streams.items():
            merged: list[list[int]] = []
            for start, end in sorted(ranges):
                if merged and start <= merged[-1][1] + 1:
                    merged[-1][1] = max(merged[-1][1], end)
                else:
                    merged.append([start, end])
            out[f"{producer}:{partition}"] = merged
        return out

def user_key(part: int, i: int, nparts: int, users: int) -> bytes:
    """Message key of the user a message belongs to, like a payment of one user.

    A user always lands in the same partition (user number % partitions == partition), so Kafka keeps that user's
    messages
    in order. Within a partition the users take turns.
    """
    per_partition = max(1, users // nparts)
    return f"user-{part + nparts * ((i // nparts) % per_partition)}".encode()


class Item(NamedTuple):
    """What the handling step needs to know about a message: whose it is and which one it is."""

    key: bytes | None  # the user
    producer: int  # which sender
    seq: int  # the number of the message in its stream


def group_by_user(items: list[Item]) -> list[list[Item]]:
    """Group the items by their key (the user). The order inside a group is the order they were sent in."""
    groups: dict[bytes | None, list[Item]] = {}
    for item in items:
        groups.setdefault(item.key, []).append(item)
    return list(groups.values())


def batch_timing(started: float, done: list[float]) -> tuple[float, float]:
    """How long a batch took, and how long of that some groups were done while others still worked (milliseconds).

    `started` and `done` are clock readings in seconds. A batch without groups (all its messages belonged to another
    run) took no time and waited for nobody.
    """
    if not done:
        return 0.0, 0.0
    return (max(done) - started) * 1000, (max(done) - min(done)) * 1000


class OrderCheck:
    """Checks that the messages of one user are handled in the order they were sent.

    For every (user, sender) it remembers the number of the message handled last. A message with a lower number than
    that was handled too late: it is counted as an order violation. A well built consumer never has one.
    """

    def __init__(self) -> None:
        self.last: dict[tuple[bytes | None, int], int] = {}
        self.violations = 0
        self._lock = threading.Lock()

    def check(self, item: Item) -> bool:
        """Remember that `item` is handled now. Returns False when it came after a later message of the same user."""
        with self._lock:
            key = (item.key, item.producer)
            last = self.last.get(key)
            if last is not None and item.seq < last:
                self.violations += 1
                return False
            self.last[key] = item.seq
            return True

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
